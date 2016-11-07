#!/usr/bin/env python

import os
import yaml
from constant import lc_path


class Conf(object):

    def __init__(self, conf_path):
        self.conf_path = conf_path
        self.conf = None

    def __getattr__(self, attr):
        if self.conf is None:
            with open(self.conf_path) as f:
                self.conf = yaml.safe_load(f)
        return self.conf[attr]


conf_path = os.path.join(lc_path, 'dlvm.conf')
conf = Conf(conf_path)
