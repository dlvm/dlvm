import os
import logging
import logging.config
from threading import Lock

import yaml

from dlvm.common.constant import lc_path

logger_cfg_path = os.path.join(lc_path, 'logger.yml')

inited = False
lock = Lock()


def __loginit()-> None:
    global inited
    if inited is True:
        return
    if os.path.isfile(logger_cfg_path):
        with open(logger_cfg_path) as f:
            logger_cfg = yaml.safe_load(f)
            logging.config.dictConfig(logger_cfg)
            inited = True


def loginit()-> None:
    with lock:
        __loginit()
