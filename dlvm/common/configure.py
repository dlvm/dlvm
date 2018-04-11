import os
from configparser import ConfigParser

from dlvm.common.constant import lc_path
from dlvm.common.utils import run_once


class DlvmConfigParser(ConfigParser):

    def getlist(self, section, option):
        return self.get(section, option).split()


@run_once
def load_cfg():
    cfg = DlvmConfigParser()
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    default_path = os.path.join(curr_dir, 'default.cfg')
    cfg.read(default_path)
    cfg_path = os.path.join(lc_path, 'dlvm.cfg')
    if os.path.isfile(cfg_path):
        cfg.read(cfg_path)
    return cfg


cfg = load_cfg()
