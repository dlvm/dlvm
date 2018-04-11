from threading import Lock
import enum
from collections import namedtuple

RequestContext = namedtuple('RequestContext', [
    'req_id', 'logger'])

WorkContext = namedtuple('WorkContext', [
    'session', 'done_set'])


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


def namedtuple_with_default(name, arg_list, default_tuple=()):
    cls = namedtuple(name, arg_list)
    cls.__new__.__defaults__ = default_tuple
    return cls


class HttpStatus(enum.IntEnum):
    OK = 200
    Created = 201
    BadRequest = 400
    NotFound = 404
    InternalServerError = 500
