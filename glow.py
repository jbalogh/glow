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
    'arc': {},
    'version': 8,
}

# This should be a lambda but pickle can't pickle a lambda.
def defaultdict_int():
    return defaultdict(int)

# The global locale count aggregator.
# {continent: {country: {region: {city: total}}}}
G['arc'] = dict((k, {}) for k in continents.values())
for country, continent in continents.items():
    G['arc'][continent][country] = defaultdict(defaultdict_int)
for country, regions in regions.items():
    continent = continents[country]
    for region in regions:
        G['arc'][continent][country][region] = defaultdict(int)


def row_name(dt):
    """Convert a datetime into the Hbase timestamp format."""
    # TODO: mobile.
    return 'firefox::%s:%s' % (FX, dt.strftime('%Y-%m-%dT%H:%M:00.000'))


def time_sequence(dt, num=100):
    for i in xrange(num):
        yield dt + timedelta(minutes=i)


def row_sum(row):
    return sum(row.columns.itervalues()) if row else 0


def get_counts(dt, num=1):
    """Get `num` minutes of download counts starting at `dt`."""
    if num == 1:
        rows = hbase.row(row_name(dt), ['product'])
    else:
        rows = hbase.scanner(row_name(dt), ['product']).list(num)
    return [(t.utctimetuple()[:5], row_sum(row))
            for t, row in zip(time_sequence(dt, num), rows)]


def extend_counts(counts):
    for t, count in counts:
        G['total'] += count
        G['counts'].append((t, G['total']))
    G['counts'] = G['counts'][-60:]
    if len(G['counts']) == 1:
        t = datetime(*G['counts'][0][0])
        G['counts'].insert(0, (t.utctimetuple()[:5], 0))


def process_locations(rows):
    """
    Break up the hbase rows into a list of
    [(continent, country, region, city, lat, lon, num_downloads)].

    The cumulative count in `arc` is updated inline.
    """
    # Get local names for fast lookups in the loop.
    arc = G['arc']
    continents, countries, regions = geo
    rv = []
    total = 0
    for row in rows:
        new = []
        # We localize country names on the client.
        for key, val in row.columns.iteritems():
            total += val
            country, region, city, lat, lon = key.split(':')[-5:]
            if country in REDACTED:
                continue
            try:
                # Sometimes maxmind gives us regions named '  ' or '00'. Those
                # are invalid. The frontend expects invalid regions named ''.
                if region.strip() in ('', '00'):
                    region = ''
                    log.debug('Renaming region: %s.' % key)
                # (0, 0) means the download is from a satellite/proxy.
                if float(lat) == float(lon) == 0:
                    continue
                continent = continents[country]
                arc[continent][country][region][city] += val
                new.append((continent, country, region, city,
                            lat, lon, val))
            except (KeyError, ValueError):
                log.error('skipping key: %s' % key, exc_info=True)
                pass
        rv.append((total, new))
    return rv


def _get_locations(dt, num=1):
    """Get `num` minutes of download locations starting at `dt`."""
    if num == 1:
        rows = hbase.row(row_name(dt), ['location:'])
    else:
        rows = hbase.scanner(row_name(dt), ['location:']).list(num)
    locs = process_locations(rows)
    return [(t.utctimetuple()[:5], r)
            for t, r in zip(time_sequence(dt, num), locs)]


def get_map(dt, num=1):
    """Get a list of [`dt`, num_rows, [(lat, long, num_downloads)]]."""
    # Get (time, num_rows, [(lat, long, hits)]) for each datetime.
    times = [(t, (num, [r[-3:] for r in rows]))
             for t, (num, rows) in _get_locations(dt, num)]
    hits = [row for t in times for row in t[1][1]]
    return (times[0][0], len(hits), hits)


def get_arc():
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
        return revsort((a.strip(), b, c) for a, (b, c) in dict_.iteritems())

    revsort = lambda xs: sorted(xs, key=itemgetter(1), reverse=True)
    continents, world_sum = {}, 0
    for continent, country_dict in G['arc'].iteritems():
        countries, continent_sum = {}, 0
        for country, region_dict in country_dict.iteritems():
            regions, country_sum = {}, 0
            for region, cities in region_dict.iteritems():
                total = sum(cities.itervalues())
                if total:
                    cs = [(k.strip(), v) for k, v in cities.iteritems()]
                    regions[region] = [total, revsort(cs)]
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


def write_files(dt, count_data=None, map_data=None, arc_data=None,
                interval=60):
    """Write all the data dicts we were given to their files."""
    log.info('Writing data for %s.' % dt)
    xs = {'count': count_data, 'map': map_data, 'arc': arc_data}
    for name, data in xs.items():
        if not data:
            continue
        fmt = '%Y/%m/%d/%H/%M/{name}.json'.format(name=name)
        path = os.path.join(JSON_DIR, dt.strftime(fmt))
        next = (dt + timedelta(seconds=interval)).strftime(fmt)
        makedirs(os.path.dirname(path))
        d = {'next': next, 'interval': interval, 'data': data}
        json.dump(d, open(path, 'w'), separators=(',', ':'))


def collect(dt):
    """Grab Hbase data, write json files, save internal state."""
    log.info('Fetching data for %s.' % dt)
    extend_counts(get_counts(dt))
    write_files(dt, G['counts'], get_map(dt), get_arc())
    dump_state(dt)


def now():
    # Live one minute in the past so Hbase has time to collect a full minute of
    # data before we start talking to it.
    return datetime.utcnow() - timedelta(minutes=1)


def do_the_stuff_to_the_thing():
    dt = now()
    next = dt + timedelta(minutes=1)
    # Wait until :15 to give Hbase some processing time.
    if dt.second < 15:
        log.info('Waiting until :15 past.')
        time.sleep(15 - dt.second)

    collect(dt)

    # Sleep until the next minute comes around.
    wait = next.replace(second=15) - now().replace(microsecond=0)
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

    if d['G'].get('version') == 7:
        upgrade_7to8(d['G'])

    if d['G'].get('version') == G['version']:
        for k, v in d['G'].items():
            G[k] = v
    else:
        log.info('Skipping out of date pickle (want v%s).' % G['version'])

    dt = now()
    delta = dt - d['last_update'].replace(second=0)
    if delta.seconds > 60:
        log.info('Missing %s minutes. Catching up.' % (delta.seconds / 60))
        for i in xrange(1, delta.seconds / 60):
            collect(d['last_update'] + timedelta(minutes=i))

    # Collect once more if the clock rolled over during catchup.
    if now().minute != dt.minute:
        log.info('Rollover!')
        load_state()

    # Wait until the next minute if the last update was at 1:15:00 and the
    # current time is less than 1:16:00 so we don't count twice.
    if now().minute == d['last_update'].minute:
        log.info('Waiting for the minute to roll over.')
        time.sleep(60 - now().second)


def upgrade_7to8(d):
    d['version'] = 8
    alfred = d['arc']['NA']['US']['NY']['Alfred']
    log.info('Removing %s downloads from Alfred.' % alfred)
    d['counts'] = [(a, b - alfred) for a, b in d['counts']]
    log.info('Adjusting count: %s => %s' % (d['total'], d['total'] - alfred))
    d['total'] -= alfred
    d['arc']['NA']['US']['NY']['Alfred'] = 0

#
# 4. Cleanup.
#


def cleanup():
    # Delete all the data from two days ago. This expects to run in cron daily
    # so there won't be any data older than two days.
    d = (now() - timedelta(days=2)).strftime('%Y/%m/%d')
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
