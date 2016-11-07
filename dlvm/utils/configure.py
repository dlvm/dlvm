#!/usr/bin/env python

import os
from ConfigParser import ConfigParser
from constant import lc_path

conf_path = os.path.join(lc_path, 'dlvm.conf')
conf = ConfigParser()
conf.read(conf_path)
