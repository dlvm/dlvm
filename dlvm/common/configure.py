from typing import List
import os
from configparser import SafeConfigParser

from dlvm.common.constant import lc_path


DEFAULT_CONF = {
    'dpv_port': '9522',
    'dpv_listener': '127.0.0.1',
    'ihost_port': '9523',
    'ihost_listener': '127.0.0.1',
    'local_vg': 'dlvm_vg',
    'list_limit': '100',
    'dpv_timeout': '300',
    'ihost_timeout': '300',
    'test_mode': 'no',
    'init_factor': '4',
    'init_max': '200G',
    'init_min': '1G',
    'lvm_unit': '4M',
    'thin_meta_factor': '48',
    'thin_meta_min': '2M',
    'mirror_meta_size': '2M',
    'thin_block_size': '2M',
    'mirror_meta_blocks': '1',
    'mirror_region_size': '2M',
    'stripe_chunk_blocks': '1',
    'low_water_mark': '100',
    'sudo': 'yes',
    'tmp_dir': '/tmp',
    'dpv_prefix': 'dlvmback',
    'ihost_prefix': 'dlvmfront',
    'fj_mirror_interval': '10',
    'cj_mirror_interval': '10',
    'cj_data_factor': '2',
    'cj_meta_size': '2M',
    'cj_block_sectors': '4096',
    'cj_low_water_mark': '10',
    'cmd_paths': '',
    'iscsi_port': '3260',
    'iscsi_userid': 'dlvm_user',
    'iscsi_password': 'dlvm_password',
    'monitor_program': '/opt/dlvm_env/bin/dlvm_monitor_action.py',
    'fj_meta_size': '4M',
    'bm_throttle': '0',
    'target_prefix': 'iqn.2016-12.dlvm.target',
    'initiator_prefix': 'iqn.2016-12.dlvm.initiator',
    'initiator_iface': 'default',
    'iscsi_path_fmt':
    '/dev/disk/by-path/ip-{address}:{port}-iscsi-{target_name}-lun-0',
    'work_dir': '/opt/dlvm_work_dir',
    'broker_url':
    'amqp://dlvm_monitor:dlvm_password@localhost:5672/dlvm_vhost',
    'db_uri':  'sqlite:////tmp/dlvm.db',
    'rpc_expiry': '5',
    'rpc_server_hook': '',
    'rpc_client_hook': '',
    'api_hook': '',
    'mq_hook': '',
}


unit_mapping = {
    'G': 1024*1024*1024,
    'M': 1024*1024,
    'K': 1024,
    'S': 512,
}


class DlvmConfigParser(SafeConfigParser):

    def getliststr(self, section: str, name: str)-> List[str]:
        return self.get(section, name).split()

    def getsize(self, section: str, name: str)-> int:
        raw_value = self.get(section, name)
        value = int(raw_value[:-1])
        unit = raw_value[-1]
        return value * unit_mapping[unit]


cfg = DlvmConfigParser(DEFAULT_CONF)
cfg_path = os.path.join(lc_path, 'dlvm.cfg')
if os.path.isfile(cfg_path):
    cfg.read(cfg_path)
