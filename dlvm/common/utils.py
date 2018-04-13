from typing import NamedTuple, Type
from types import TracebackType
from threading import Lock
import enum
import uuid
from logging import LoggerAdapter


class RequestContext(NamedTuple):
    req_id: uuid.UUID
    logger: LoggerAdapter


class ExcInfo(NamedTuple):
    etype: Type[BaseException]
    value: BaseException
    tb: TracebackType


def run_once(func):
    has_run = False
    lock = Lock()

    def wrapper(*args, **kwargs):
        nonlocal has_run
        with lock:
            if has_run is False:
                has_run = True
                return func(*args, **kwargs)

    return wrapper


class HttpStatus(enum.IntEnum):
    OK = 200
    Created = 201
    BadRequest = 400
    NotFound = 404
    InternalServerError = 500
