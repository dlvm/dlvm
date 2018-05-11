import os
from configparser import ConfigParser

from dlvm.common.constant import LC_PATH, DEFAULT_CFG_FILE, DLVM_CFG_FILE
from dlvm.common.utils import run_once


class DlvmConfigParser(ConfigParser):

    def getlist(self, section, option):
        return self.get(section, option).split()

    def getsize(self, section, option):
        val = self.get(section, option)
        count = int(val[:-1])
        unit = val[-1:]
        if unit == 'T':
            return count*1024*1024*1024*1024
        elif unit == 'G':
            return count*1024*1024*1024
        elif unit == 'M':
            return count*1024*1024
        elif unit == 'K':
            return count*1024
        else:
            raise TypeError('invalid unit: %s %s' % (val, unit))


@run_once
def load_cfg():
    cfg = DlvmConfigParser()
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    default_path = os.path.join(curr_dir, DEFAULT_CFG_FILE)
    cfg.read(default_path)
    cfg_path = os.path.join(LC_PATH, DLVM_CFG_FILE)
    if os.path.isfile(cfg_path):
        cfg.read(cfg_path)
    return cfg


cfg = load_cfg()
