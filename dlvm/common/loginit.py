import os
import logging
import logging.config
from threading import Lock
import yaml

from dlvm.common.constant import lc_path

logger_conf_path = os.path.join(lc_path, 'logger.yml')

inited = False
lock = Lock()


def __loginit()-> None:
    global inited
    if inited is True:
        return
    if os.path.isfile(logger_conf_path):
        with open(logger_conf_path) as f:
            logger_conf = yaml.safe_load(f)
            logging.config.dictConfig(logger_conf)
            inited = True


def loginit()-> None:
    with lock:
        __loginit()
