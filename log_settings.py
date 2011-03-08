import logging
import logging.handlers

import dictconfig

import settings_local as settings


fmt = '[%(asctime)s] %(name)s:%(levelname)s %(message)s'


cfg = {
    'version': 1,
    'filters': {},
    'formatters': {
        'file': {'format': fmt},
        'syslog': {'format': '%s: %s' % (settings.SYSLOG_TAG, fmt)},
    },
    'handlers': {
        'file': {
            '()': logging.FileHandler,
            'filename': settings.path('glow.log'),
            'mode': 'a',
            'formatter': 'file',
        },
        'syslog': {
            '()': logging.handlers.SysLogHandler,
            'facility': logging.handlers.SysLogHandler.LOG_USER,
            'formatter': 'syslog',
        },
    },
    'root': {
        'level': logging.DEBUG,
        'handlers': ['file'],
    },
}


if settings.USE_SYSLOG:
    cfg['root']['handlers'].append('syslog')


dictconfig.dictConfig(cfg)
