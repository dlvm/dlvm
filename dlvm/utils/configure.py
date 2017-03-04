#!/usr/bin/env python

import os
import yaml
from constant import lc_path


DEFAULT_CONF = {
    'dpv_port': 9522,
    'dpv_listener': '127.0.0.1',
    'thost_port': 9523,
    'thost_listener': '127.0.0.1',
    'local_vg': 'dlvm_vg',
    'dpv_list_limit': 100,
    'dvg_list_limit': 100,
    'dlv_list_limit': 100,
    'thost_list_limit': 100,
    'fj_list_limit': 100,
    'obt_list_limit': 100,
    'snapshot_list_limit': 100,
    'dpv_timeout': 10,
    'thost_timeout': 10,
    'cross_dpv': False,
    'init_factor': 4,
    'init_max': 1024*1024*1024*200,
    'init_min': 1024*1024*1024*1,
    'lvm_unit': 4*1024*1024,
    'thin_meta_factor': 48,
    'thin_meta_min': 2*1024*1024,
    'mirror_meta_size': 1024*1024*2,
    'thin_block_size': 1024*1024*2,
    'mirror_meta_blocks': 1,
    'mirror_region_size': 1024*1024*2,
    'stripe_chunk_blocks': 1,
    'low_water_mark': 100,
    'sudo': True,
    'tmp_dir': '/tmp',
    'dlvm_prefix': 'dlvm',
    'fj_mirror_interval': 10,
    'cmd_paths': [],
    'iscsi_port': 3260,
    'iscsi_userid': 'dlvm_user',
    'iscsi_password': 'dlvm_password',
    'monitor_single_leg_failed': '/bin/dlvm_monitor_single_leg_failed',
    'monitor_multi_legs_failed': '/bin/dlvm_monitor_multi_legs_failed',
    'monitor_pool_full': '/bin/dlvm_monitor_pool_full',
    'monitor_fj_mirror_failed': '/bin/dlvm_monitor_fj_mirror_failed',
    'monitor_fj_mirror_complete': '/bin/dlvm_monitor_fj_mirror_complete',
    'fj_meta_size': 4194304,
    'bm_throttle': 0,
    'target_prefix': 'iqn.2016-12.dlvm.target',
    'initiator_prefix': 'iqn.2016-12.dlvm.initiator',
    'iscsi_path_fmt':
    '/dev/disk/by-path/ip-{address}:{port}-iscsi-{target_name}-lun-0',
    'work_dir': '/opt/dlvm_work_dir',
    'broker_url':
    'amqp://dlvm_monitor:dlvm_password@localhost:5672/dlvm_vhost',
    'db_uri':  'sqlite:////tmp/dlvm.db',
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
