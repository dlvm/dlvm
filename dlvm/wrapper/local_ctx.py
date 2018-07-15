from collections import namedtuple
from threading import local
import enum
from datetime import datetime


backend_local = local()
frontend_local = local()


class Direction(enum.Enum):
    forward = 'forward'
    backward = 'backward'


WorkerContext = namedtuple(
    'WorkerContext', [
        'worklog', 'direction', 'enforce', 'lock_dt'])


def get_empty_worker_ctx():
    return WorkerContext(
        worklog=set(),
        direction=Direction.forward,
        enforce=False,
        lock_dt=datetime(3000, 1, 1, 0, 0, 0))
