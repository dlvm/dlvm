import os
import logging
import logging.config
import json

from dlvm.common.constant import lc_path
from dlvm.common.utils import run_once

logger_cfg_path = os.path.join(lc_path, 'logger.yml')

inited = False


@run_once
def loginit():
    if os.path.isfile(logger_cfg_path):
        with open(logger_cfg_path) as f:
            logger_cfg = json.load(f)
            logging.config.dictConfig(logger_cfg)
