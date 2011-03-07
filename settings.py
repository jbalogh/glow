import os

HBASE_HOST = 'node1.research.hadoop.sjc1.mozilla.com'
HBASE_HOST = '10.2.72.102'
HBASE_PORT = 9090

HBASE_TABLES = {
    'realtime': 'dmo_metrics_realtime',
    'hourly': 'dmo_metrics_hourly',
    'new': 'dmo_metrics_realtime_newschema',
}

ROOT = os.path.dirname(os.path.abspath(__file__))
path = lambda *a: os.path.join(ROOT, *a)

BASE_DIR = path('data')

USE_SYSLOG = False
SYSLOG_TAG = 'http_app_glow'
