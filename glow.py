import cPickle as pickle
import os
import json
import logging
import shutil
import time
from collections import defaultdict
from datetime import datetime, timedelta
from operator import itemgetter

import hb
import settings_local as settings

log = logging.getLogger('glow')


# The default version number we look for.
FX = settings.FIREFOX_VERSION
JSON_DIR = os.path.join(settings.BASE_DIR, 'json')
PICKLE = settings.path('glow.pickle')
BACKUP = PICKLE + '.bak'

hbase = hb.Client(settings.HBASE_HOST, settings.HBASE_PORT,
                  settings.HBASE_TABLES['realtime'])

# Maps {country: continent}.
continents = json.load(open(settings.path('continents.json')))

# Maps {country code: name}.
countries = json.load(open(settings.path('countries.json')))

# Maps {country code: {region code: name}}.
regions = json.load(open(settings.path('regions.json')))
geo = (continents, countries, regions)

# We're not supposed to show downloads for these countries (607127#c10):
# Cuba, Iran, Syria, N. Korea, Myanmar, Sudan. Go figure.
REDACTED = ('CU', 'IR', 'SY', 'KP', 'MM', 'SD')

##
## 1. The part that talks to Hbase and collects data.
##

# These contain global download totals that get updated every time we process a
# new chunk of data.
G = {
    'total': 0,
    'counts': [],
    'daisy': {},
}

# The global locale count aggregator.
# {continent: {country: {region: {city: total}}}}
G['daisy'] = dict((k, {}) for k in continents.values())
for country, continent in continents.items():
    G['daisy'][continent][country] = {}
for country, regions in regions.items():
    continent = continents[country]
    for region in regions:
        G['daisy'][continent][country][region] = defaultdict(int)


def row_name(dt):
    """Convert a datetime into the Hbase timestamp format."""
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%dT%H:%M:00.000')
    else:
        return dt


def time_sequence(dt, num=100):
    for i in xrange(num):
        yield dt + timedelta(minutes=i + 1)


def product(name=None):
    if name:
        # TODO: :mobile:
        return 'product:firefox::' + name
    else:
        return 'product:'


def row_sum(row, prefix):
    if row:
        return sum(v for k, v in row.columns.iteritems()
                   if k.startswith(prefix))
    else:
        return 0


def get_counts(dt, num=1, name=FX):
    """Get `num` minutes of download counts starting at `dt`."""
    prefix = product(name)
    if num == 1:
        rows = hbase.row(row_name(dt), [product()])
    else:
        rows = hbase.scanner(row_name(dt), [product()]).list(num)
    return [(t.utctimetuple()[:5], row_sum(row, prefix))
            for t, row in zip(time_sequence(dt, num), rows)]


def extend_counts(counts):
    for t, count in counts:
        G['total'] += count
        G['counts'].append((t, G['total']))


def process_locations(rows):
    """
    Break up the hbase rows into a list of
    [(continent, country, region, city, lat, lon, num_downloads)].

    The cumulative count in `daisy` is updated inline.
    """
    # Get local names for fast lookups in the loop.
    daisy = G['daisy']
    continents, countries, regions = geo
    rv = []
    total = 0
    for row in rows:
        new = []
        # We localize country names on the client.
        for key, val in row.columns.iteritems():
            total += val
            _, country, region, city, lat, lon = key.split(':')
            if country in REDACTED:
                continue
            try:
                continent = continents[country]
                daisy[continent][country][region][city] += val
                new.append((continent, country, region, city,
                            lat, lon, val))
            except (KeyError, ValueError):
                pass
        rv.append((total, new))
    return rv


def _get_locations(dt, num=1, name=FX):
    """Get `num` minutes of download locations starting at `dt`."""
    if num == 1:
        rows = hbase.row(row_name(dt), ['location:'])
    else:
        rows = hbase.scanner(row_name(dt), ['location:']).list(num)
    locs = process_locations(rows)
    return [(t.utctimetuple()[:5], r)
            for t, r in zip(time_sequence(dt, num), locs)]


def get_map(dt, num=1, name=FX):
    """Get a list of [`dt`, num_rows, [(lat, long, num_downloads)]]."""
    # Get (time, num_rows, [(lat, long, hits)]) for each datetime.
    times = [(t, (num, [r[-3:] for r in rows]))
             for t, (num, rows) in _get_locations(dt, num, name)]
    hits = [row for t in times for row in t[1][1]]
    return (times[0][0], len(hits), hits)


def get_daisy():
    """
    Aggregate the location data into an easy json structure:

        (None, total,
         [continent, total, [country, total, [region, total, [city, total]]]])

    The expected format of `data` is:

        {continent: {country: {region: {city: total}}}}

    Each outer total is the sum of its childrens' inner totals.
    """
    def unpack(dict_):
        """Unpack a (key, (v1, v2)) structure into (key, v1, v2)."""
        return revsort((a, b, c) for a, (b, c) in dict_.iteritems())

    revsort = lambda xs: sorted(xs, key=itemgetter(1), reverse=True)
    continents, world_sum = {}, 0
    for continent, country_dict in G['daisy'].iteritems():
        countries, continent_sum = {}, 0
        for country, region_dict in country_dict.iteritems():
            regions, country_sum = {}, 0
            for region, cities in region_dict.iteritems():
                total = sum(cities.itervalues())
                if total:
                    regions[region] = [total, revsort(cities.items())]
                    country_sum += total
            if country_sum:
                countries[country] = [country_sum, unpack(regions)]
                continent_sum += country_sum
        if continent_sum:
            continents[continent] = [continent_sum, unpack(countries)]
            world_sum += continent_sum
    return (None, world_sum, unpack(continents))


##
## 2. The main loop.
##

def makedirs(d):
    if not os.path.exists(d):
        log.info('Making dir %s.' % d)
        os.makedirs(d)


def write_files(dt, count_data=None, map_data=None, daisy_data=None,
                interval=60):
    """Write all the data dicts we were given to their files."""
    log.info('Writing data for %s.' % dt)
    xs = {'count': count_data, 'map': map_data, 'daisy': daisy_data}
    for name, data in xs.items():
        if not data:
            continue
        fmt = '%Y/%m/%d/%H/%M/{name}.json'.format(name=name)
        now = os.path.join(JSON_DIR, dt.strftime(fmt))
        next = (dt + timedelta(seconds=interval)).strftime(fmt)
        makedirs(os.path.dirname(now))
        d = {'next': next, 'interval': interval, 'data': data}
        json.dump(d, open(now, 'w'), separators=(',', ':'))


def collect(now):
    """Grab Hbase data, write json files, save internal state."""
    log.info('Fetching data for %s.' % now)
    extend_counts(get_counts(now))
    write_files(now, G['counts'], get_map(now), get_daisy())
    dump_state(now)


def do_the_stuff_to_the_thing():
    now = datetime.now()
    next = now + timedelta(minutes=1)
    # Wait until :30 to give Hbase some processing time.
    if now.second < 30:
        log.info('Waiting until :30 past.')
        time.sleep(30 - now.second)

    collect(now)

    # Sleep until the next minute comes around.
    wait = next.replace(second=30) - datetime.now().replace(microsecond=0)
    # The delta will be around -1 days, 86400 seconds if we're into the next
    # minute already.
    if wait.seconds <= 60:
        log.info('Sleeping for %s seconds.' % wait.seconds)
        time.sleep(wait.seconds)
    else:
        log.info('Skipping sleep.')


def main():
    load_state()
    log.info('Looping, infinitely.')
    while 1:
        try:
            do_the_stuff_to_the_thing()
        except hb.exceptions:
            log.error('Recycling Hbase connection.', exc_info=True)
            hbase.recycle()


#
# 3. Saving and loading application state.
#

def dump_state(dt):
    """Dump all the global aggregators so we can pick at the same spot."""
    log.info('Saving state for %s.' % dt)
    if os.path.exists(PICKLE):
        shutil.copyfile(PICKLE, BACKUP)
    d = {'G': G, 'last_update': dt}
    pickle.dump(d, open(PICKLE, 'w'))


def load_state():
    """Figure out where we left off, catch up on old data if needed."""
    if not (os.path.exists(PICKLE) or os.path.exists(BACKUP)):
        return
    log.info('Found a pickle, picking it up.')
    try:
        d = pickle.load(open(PICKLE))
    except Exception:
        log.error('Trouble opening pickle.', exc_info=True)
        if os.path.exists(BACKUP):
            log.info('Loading backup pickle.')
            d = pickle.load(open(BACKUP))

    for k, v in d['G'].items():
        G[k] = v

    now = datetime.now()
    delta = now - d['last_update'].replace(second=0)
    if delta.seconds > 60:
        log.info('Missing %s minutes. Catching up.' % (delta.seconds / 60))
        for i in xrange(1, delta.seconds / 60):
            collect(d['last_update'] + timedelta(minutes=i))

    # Collect once more if the clock rolled over during catchup.
    if datetime.now().minute != now.minute:
        log.info('Rollover!')
        collect(now)

    # Wait until the next minute if the last update was at 1:15:00 and the
    # current time is less than 1:16:00 so we don't count twice.
    if datetime.now().minute == d['last_update'].minute:
        log.info('Waiting for the minute to roll over.')
        time.sleep(60 - datetime.now().second)

#
# 4. Cleanup.
#


def cleanup():
    # Delete all the data from two days ago. This expects to run in cron daily
    # so there won't be any data older than two days.
    d = (datetime.now() - timedelta(days=2)).strftime('%Y/%m/%d')
    path = os.path.join(JSON_DIR, d)
    if os.path.exists(path):
        log.info('Dropping %s.' % path)
        shutil.rmtree(path)
        # Try to delete the month and year directories. rmdir only works if the
        # directory is empty, so this will clean up empty directories.
        try:
            os.rmdir(os.path.dirname(path))
            os.rmdir(os.path.dirname(os.path.dirname(path)))
        except OSError:
            pass
