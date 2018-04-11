from threading import Lock
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
