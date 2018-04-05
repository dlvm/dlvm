from typing import NamedTuple, Sequence
import os

import yaml

from dlvm.common.constant import lc_path


class HookCfg(NamedTuple):
    api_hook: Sequence[str] = [
        'dlvm.hook.log_hook.LogApiHook',
    ]
    rpc_server_hook: Sequence[str] = [
        'dlvm.hook.log_hook.LogRpcServerHook',
    ]
    rpc_client_hook: Sequence[str] = [
        'dlvm.hook.log_hook.LogRpcClientHook',
    ]


class RpcCfg(NamedTuple):
    dpv_port: int = 9522
    dpv_listener: str = 'localhost'
    dpv_timeout: int = 300
    ihost_port: int = 9523
    ihost_listener: str = 'localhost'
    ihost_timeout: int = 300


class Cfg(NamedTuple):
    hook: HookCfg
    rpc: RpcCfg


def load_cfg()-> Cfg:
    cfg_path = os.path.join(lc_path, 'dlvm.yml')
    cfg_args = {}
    if os.path.isfile(cfg_path):
        with open(cfg_path) as f:
            cfg_dict = yaml.safe_load(f)
    else:
        cfg_dict = {}
    for key in Cfg._fields:
        sub_cfg_dict = cfg_dict.get(key, {})
        sub_cfg_type = Cfg.__annotations__[key]
        sub_cfg = sub_cfg_type(**sub_cfg_dict)
        cfg_args[key] = sub_cfg
    cfg = Cfg(**cfg_args)
    return cfg


cfg = load_cfg()
