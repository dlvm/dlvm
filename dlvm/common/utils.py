from typing import NamedTuple, Type
from types import TracebackType
from threading import Lock
import enum
import uuid
import os
from logging.handlers import WatchedFileHandler
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


def get_empty_thin_mapping(thin_block_bytes, thin_data_blocks):
    thin_block_sectors = thin_block_bytes // 512
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(curr_dir, 'thin_mapping_template.xml')
    with open(template_path) as f:
        template = f.read()
    return template.format(
        thin_block_sectors=thin_block_sectors,
        thin_data_blocks=thin_data_blocks)


class PidWatchedFileHandler(WatchedFileHandler):

    def __init__(
            self, filename, mode='a', encoding=None, delay=False):
        filename_pid = filename.format(pid=os.getpid())
        super(PidWatchedFileHandler, self).__init__(
            filename_pid, mode, encoding, delay)


def chunks(array, n):
    """Yield successive n-sized chunks from array."""
    for i in range(0, len(array), n):
        yield array[i:i+n]
