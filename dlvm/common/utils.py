from typing import NewType, MutableSet, Sequence, Generator, NamedTuple

import os
from logging.handlers import WatchedFileHandler
from logging import LoggerAdapter
from enum import IntEnum

from sqlalchemy.orm.session import Session


ReqId = NewType('ReqId', str)


class RequestContext(NamedTuple):
    req_id: ReqId
    logger: LoggerAdapter


class WorkContext(NamedTuple):
    session: Session
    done_set: MutableSet[str]


def chunks(array: Sequence, n: int)-> Generator[Sequence, None, None]:
    """Yield successive n-sized chunks from array."""
    for i in range(0, len(array), n):
        yield array[i:i+n]


class PidWatchedFileHandler(WatchedFileHandler):

    def __init__(
            self, filename: str,
            mode: str = 'a',
            encoding: str = None,
            delay: bool = False)-> None:
        filename_pid = '{filename}-{pid}'.format(
            filename=filename, pid=os.getpid())
        super(PidWatchedFileHandler, self).__init__(
            filename_pid, mode, encoding, delay)


class HttpStatus(IntEnum):
    OK = 200
    Created = 201
    BadRequest = 400
    NotFound = 404
    InternalServerError = 500
