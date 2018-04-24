from typing import NamedTuple, Set
from threading import local
import enum
from datetime import datetime
import uuid


backend_local = local()
frontend_local = local()


class Direction(enum.Enum):
    forward = 'forward'
    backward = 'backward'


class WorkerContext(NamedTuple):
    worklog: Set
    direction: Direction
    enforce: bool
    lock_owner: str
    lock_dt: datetime


def get_empty_worker_ctx():
    return WorkerContext(
        worklog=set(),
        direction=Direction.forward,
        enforce=False,
        lock_owner=uuid.uuid4().hex,
        lock_dt=datetime(3000, 1, 1, 0, 0, 0))
