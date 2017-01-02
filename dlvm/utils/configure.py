#!/usr/bin/env python

import os
import yaml
from constant import lc_path


DEFAULT_CONF = {
    'thost_db': 'sqlite:////data/dlvm/thost.db',
    'dpv_db': 'sqlite:////data/dlvm/dpv.db',
    'api_port': 9521,
    'api_listener': '127.0.0.1',
    'dpv_port': 9522,
    'dpv_listener': '127.0.0.1',
    'thost_port': 9523,
    'thost_listener': '127.0.0.1',
    'local_vg': 'dlvm_vg',
    'dpv_list_limit': 100,
    'dvg_list_limit': 100,
    'dlv_list_limit': 100,
    'mj_list_limit': 100,
    'transaction_list_limit': 100,
    'dpv_timeout': 10,
    'thost_timeout': 10,
    'cross_dpv': False,
    'init_factor': 4,
    'init_max': 1024*1024*1024*200,
    'init_min': 1024*1024*1024*1,
    'thin_meta_factor': 48,
    'mirror_meta_size': 1024*1024*2,
    'thin_block_size': 1024*1024*2,
    'mirror_meta_blocks': 1,
    'mirror_region_size': 1024*1024*2,
    'stripe_chunk_blocks': 1,
    'low_water_mark': 100,
    'sudo': True,
    'tmp_dir': '/tmp',
    'dlvm_prefix': 'dlvm',
    'mj_mirror_interval': 10,
    'dmsetup_path': '/sbin/dmsetup',
    'lvm_path': '/sbin/lvm',
    'iscsiadm_path': '/sbin/iscsiadm_path',
    'targetcli_path': '/sbin/targetcli',
    'dd_path': '/bin/dd',
    'iscsi_port': 3250,
    'iscsi_userid': 'dlvm_user',
    'iscsi_password': 'dlvm_password',
    'monitor_single_leg_failed': '/bin/dlvm_monitor_single_leg_failed',
    'monitor_multi_legs_failed': '/bin/dlvm_monitor_multi_legs_failed',
    'monitor_pool_full': '/bin/dlvm_monitor_pool_full',
    'monitor_mj_mirror_failed': '/bin/dlvm_monitor_mj_mirror_failed',
    'monitor_mj_mirror_complete': '/bin/dlvm_monitor_mj_mirror_complete',
    'mj_meta_size': 4194304,
    'bm_throttle': 0,
    'target_prefix': 'iqn.2016-12.dlvm.target',
    'initiator_prefix': 'iqn.2016-12.dlvm.initiator',
    'iscsi_path_fmt': '/dev/disk',
    'dpv_transaction_db': '/run/dlvm/dpv_transaction_db',
    'dpv_major_file': '/run/dlvm/dpv_major_file',
    'thost_transaction_db': '/run/dlvm/thost_transaction_db',
    'thost_major_file': '/run/dlvm/thost_major_file',
    'broker_url':
    'amqp://dlvm_monitor:dlvm_password@localhost:5672/dlvm_vhost',
    'db_uri':  'sqlite:////run/dlvm/dlvm.db',
}


class Conf(object):

    def __init__(self, conf_path):
        self.conf_path = conf_path
        self.conf = None

    def __getattr__(self, attr):
        if self.conf is None:
            if os.path.isfile(self.conf_path):
                with open(self.conf_path) as f:
                    self.conf = yaml.safe_load(f)
            else:
                self.conf = {}
        if attr in self.conf:
            return self.conf[attr]
        else:
            return DEFAULT_CONF[attr]


conf_path = os.path.join(lc_path, 'conf.yml')
conf = Conf(conf_path)
