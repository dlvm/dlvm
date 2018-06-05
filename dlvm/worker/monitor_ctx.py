from collections import namedtuple


MonitorContext = namedtuple(
    'MonitorContext', ['req_ctx', 'name', 'res'])
