import os
import logging
import logging.config
import json

from dlvm.common.constant import LC_PATH, LOGGER_CFG_FILE
from dlvm.common.utils import run_once

logger_cfg_path = os.path.join(LC_PATH, LOGGER_CFG_FILE)


@run_once
def loginit():
    if os.path.isfile(logger_cfg_path):
        with open(logger_cfg_path) as f:
            logger_cfg = json.load(f)
            logging.config.dictConfig(logger_cfg)
