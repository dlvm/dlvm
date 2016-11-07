#!/usr/bin/env python

import os
import logging.config
import yaml
from constant import lc_path

logger_conf_path = os.path.join(lc_path, 'logger.yml')

inited = False


def loginit():
    global inited
    if inited is True:
        return
    with open(logger_conf_path) as f:
        logger_conf = yaml.safe_load(f)
        logging.config.dictConfig(logger_conf)
    inited = True
