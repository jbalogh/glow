"""
Take a messages.po file and turn it into a javascript file.

All the strings end up in a variable called catalog. That locale's short time
format is in _timefmt and the numeric separator is in _group.

Call the function with the source directory of messages.po files and the
destination directory for the l10n.js files.
"""
import codecs
import json

import path
from babel.core import Locale, UnknownLocaleError
from babel.messages import pofile


ROOT = path.path('locale')
DOMAIN = 'messages.po'
DEFAULT = 'en_US'


def steal():
    for f in path.path('locale').walkfiles('messages.po'):
        lang = f.split('/')[1]
        django = path.path('django') / lang / 'LC_MESSAGES' / 'django.po'
        if django.exists():
            d = po_to_dict(django)
            with codecs.open(f, 'a', 'utf-8') as fd:
                for k in 'AM', 'PM':
                    fd.write('\nmsgid: "%s"\nmsgstr: "%s"\n' % (k, d.get(k, k)))


def po_to_dict(path):
    return dict((p.id, p.string) for p in pofile.read_po(open(path))
                if p.id and p.string)


def main(src, dst):
    locales = []
    for f in path.path(src).walkfiles('messages.po'):
        print f
        lang = f.split('/')[1]
        locales.append(lang.replace('_', '-'))
        print lang
        try:
            locale = Locale(lang)
        except UnknownLocaleError:
            print 'Unknown locale:', lang
            locale = Locale(DEFAULT)
        out = path.path(dst) / lang
        if not out.exists():
            out.makedirs()
        d = {'po': json.dumps(po_to_dict(f), separators=(',', ':')),
             'timefmt': locale.time_formats['short'].pattern,
             'numfmt': locale.decimal_formats[None].pattern,
             'group': locale.number_symbols['group']}
        print '% 5s %8s %s %s' % (lang, d['timefmt'], d['group'], d['numfmt'])
        with codecs.open(out / 'l10n.js', 'w', 'utf-8') as fd:
            fd.write(template % d)

        default = path.path('locale/countries/en-US.json')
        countries = path.path('locale/countries/%s.json' %
                              lang.replace('_', '-'))
        regions = path.path('locale/%s/regions.json' % lang)
        cities = path.path('locale/%s/cities.json' % lang)
        if not countries.exists():
            print '*' * 30, 'missing', lang
            countries = default
        with codecs.open(out / 'countries.js', 'w', 'utf-8') as fd:
            d = dict((k.upper(), v)
                     for k, v in json.load(countries.open()).items())
            if regions.exists():
                print 'Adding regions for', lang
                d.update(json.load(regions.open()))
            if cities.exists():
                print 'Adding cities for', lang
                d.update(json.load(cities.open()))
            fd.write('var _countries = %s;' % json.dumps(d, separators=(',', ':')))
    lo = ["'%s'" % x.lower() for x in locales]
    print '$locales = array(%s);' % ', '.join(lo)


template = """\
var catalog = %(po)s,
    _timefmt = "%(timefmt)s",
    _group = "%(group)s",
    _numfmt = "%(numfmt)s";
"""
