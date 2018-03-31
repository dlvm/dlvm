from typing import Sequence
import os

import yaml

from dlvm.common.constant import lc_path
from dlvm.common.utils import singleton


@singleton
class DlvmConf():

    def __init__(self)-> None:
        self.dpv_port = 9522
        self.dpv_listener = '127.0.0.1'
        self.ihost_port = 9523
        self.ihost_listener = '127.0.0.1'
        self.local_vg = 'dlvm_vg'
        self.list_limit = 100
        self.dpv_timeout = 300
        self.ihost_timeout = 300
        self.test_mode = False
        self.init_factor = 4
        self.init_max = 200*1024*1024*1024
        self.init_min = 1024*1024*1024*1024
        self.lvm_unit = 4*1024*1024
        self.thin_meta_factor = 48
        self.thin_meta_min = 2*1024*1024
        self.mirror_meta_size = 2*1024*1024
        self.thin_block_size = 2*1024*1024
        self.mirror_meta_blocks = 1
        self.mirror_region_size = 2*1024*1024
        self.stripe_chunk_blocks = 1
        self.low_water_mark = 100
        self.sudo = True
        self.tmp_dir = '/tmp'
        self.dpv_prefix = 'dlvmback'
        self.ihost_prefix = 'dlvmfront'
        self.fj_mirror_interval = 10
        self.cj_mirror_interval = 10
        self.cj_data_factor = 2
        self.cj_meta_size = 2*1024*1024
        self.cj_block_sectors = 4096
        self.cj_low_water_mark = 10
        self.cmd_paths: Sequence[str] = []
        self.iscsi_port = 3260
        self.iscsi_userid = 'dlvm_user'
        self.iscsi_password = 'dlvm_password'
        self.fj_meta_size = 4194304
        self.bm_throttle = 0
        self.target_prefix = 'iqn.2016-12.dlvm.target'
        self.initiator_prefix = 'iqn.2016-12.dlvm.initiator'
        self.initiator_iface = 'default'
        self.iscsi_path_fmt \
            = '/dev/disk/by-path/ip-{address}:{port}-iscsi-{target_name}-lun-0'
        self.work_dir = '/opt/dlvm_work_dir'
        self.broker_uri \
            = 'amqp://dlvm_monitor:dlvm_password@localhost:5672/dlvm_vhost'
        self.db_uri = 'sqlite://'
        self.rpc_expiry = 5
        self.api_hook: Sequence[str] = [
            'dlvm.hook.log_hook.LogApiHook',
        ]
        self.rpc_server_hook: Sequence[str] = [
            'dlvm.hook.log_hook.LogRpcServerHook',
        ]
        self.rpc_client_hook: Sequence[str] = [
            'dlvm.hook.log_hook.LogRpcClientHook',
        ]
        self.mq_hook: Sequence[str] = []

    def load_conf(self, conf_path: str)-> None:
        if os.path.isfile(conf_path):
            with open(conf_path) as f:
                conf_dict = yaml.safe_load(f)
            for key in conf_dict:
                setattr(self, key, conf_dict[key])


conf_path = os.path.join(lc_path, 'dlvm.yml')
cfg = DlvmConf()
cfg.load_conf(conf_path)
