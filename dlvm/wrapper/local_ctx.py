from typing import NamedTuple, Set, Optional
from threading import local
import enum

backend_local = local()
frontend_local = local()


class Direction(enum.Enum):
    forward = 'forward'
    backward = 'backward'


class WorkerContext(NamedTuple):
    worklog: Set = set()
    direction: Direction = Direction.forward
    enforce: bool = False
    lock_owner: Optional[str] = None


class RpcError(Exception):
    pass
