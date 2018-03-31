from typing import NewType, MutableSet, Sequence, Mapping, Generator, \
    Any, Callable, Union
import os
from threading import Lock
from logging.handlers import WatchedFileHandler
from logging import Logger, LoggerAdapter

from sqlalchemy.orm.session import Session


ReqId = NewType('ReqId', str)


class RequestContext():

    def __init__(
            self, req_id: ReqId, logger: Union[Logger, LoggerAdapter])-> None:
        self.req_id = req_id
        self.logger = logger

    def __repr__(self)-> str:
        return 'RequestContext(req_id={0},logger={1})'.format(
            self.req_id, self.logger)


class WorkContext():

    def __init__(
            self, session: Session, done_set: MutableSet[str])-> None:
        self.session = session
        self.done_set = done_set

    def __repr__(self)-> str:
        return 'WorkContext(session={0},done_set={1}'.format(
            self.session, self.done_set)


def chunks(array: Sequence, n: int)-> Generator[Sequence, None, None]:
    """Yield successive n-sized chunks from array."""
    for i in range(0, len(array), n):
        yield array[i:i+n]


def singleton(cls: type)-> Callable:
    instance = None
    lock = Lock()

    def _singleton(*args: Sequence, **kwargs: Mapping)-> Any:
        nonlocal instance
        with lock:
            if instance is None:
                instance = cls(*args, **kwargs)
        return instance

    return _singleton


class PidWatchedFileHandler(WatchedFileHandler):

    def __init__(
            self, filename: str, *args: Any, **kwargs: Any)-> None:
        filename_pid = '{filename}-{pid}'.format(
            filename=filename, pid=os.getpid())
        super(PidWatchedFileHandler, self).__init__(
            filename_pid, *args, **kwargs)
