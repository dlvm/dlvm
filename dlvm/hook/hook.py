from typing import Type, NewType, List, Sequence, Mapping, Any, Union, Optional
from abc import ABC, abstractmethod
from importlib import import_module

from dlvm.common.configure import cfg
from dlvm.common.utils import RequestContext, WorkContext


HookRet = NewType('HookRet', tuple)


class ApiParam():

    def __init__(
            self, func_name: str,
            req_ctx: RequestContext,
            work_ctx: WorkContext,
            params: Optional[Mapping],
            kwargs: Optional[Mapping])-> None:
        self.func_name = func_name
        self.req_ctx = req_ctx
        self.work_ctx = work_ctx
        self.params = params
        self.kwargs = kwargs


class ApiHook(ABC):

    hook_name = 'api_hook'

    @abstractmethod
    def pre_hook(self, param: ApiParam)-> Optional[HookRet]:
        raise NotImplementedError

    @abstractmethod
    def post_hook(
            self, param: ApiParam, hook_ret: Optional[HookRet],
            body: Mapping)-> None:
        raise NotImplementedError

    @abstractmethod
    def error_hook(self, param: ApiParam, hook_ret: Optional[HookRet],
                   e: Exception, calltrace: str)-> None:
        raise NotImplementedError


class RpcServerParam():

    def __init__(
            self, func_name: str, req_ctx: RequestContext, expire_time: int,
            args: Sequence)-> None:
        self.func_name = func_name
        self.req_ctx = req_ctx
        self.expire_time = expire_time
        self.args = args,

    def __repr__(self):
        return (
            'RpcServerParam('
            'func_name={0},req_ctx={1},expire_time={2},args={3})'
        ).format(
            self.func_name, repr(self.req_ctx), self.expire_time,
            self.args)


class RpcServerHook(ABC):

    hook_name = 'rpc_server_hook'

    @abstractmethod
    def pre_hook(self, param: RpcServerParam)-> Optional[HookRet]:
        raise NotImplementedError

    @abstractmethod
    def post_hook(self, param: RpcServerParam,
                  hook_ret: Optional[HookRet], ret: Any)-> None:
        raise NotImplementedError

    @abstractmethod
    def error_hook(
            self, param: RpcServerParam,
            hook_ret: Optional[HookRet], e: Exception,
            calltrace: str)-> None:
        raise NotImplementedError


class RpcClientParam():

    def __init__(
            self, func_name: str, req_ctx: RequestContext, expire_time: int,
            args: Sequence)-> None:
        self.func_name = func_name
        self.req_ctx = req_ctx
        self.expire_time = expire_time
        self.args = args,

    def __repr__(self):
        return (
            'RpcParam('
            'func_name={0},req_ctx={1},expire_time={2},args={3})'
        ).format(
            self.func_name, repr(self.req_ctx), self.expire_time,
            self.args)


class RpcClientHook(ABC):

    hook_name = 'rpc_client_hook'

    @abstractmethod
    def pre_hook(self, param: RpcClientParam)-> Optional[HookRet]:
        raise NotImplementedError

    @abstractmethod
    def post_hook(self, param: RpcClientParam,
                  hook_ret: Optional[HookRet], ret: Any)-> None:
        raise NotImplementedError

    @abstractmethod
    def error_hook(self, param: RpcClientParam,
                   hook_ret: Optional[HookRet], e: Exception,
                   calltrace: str)-> None:
        raise NotImplementedError


HookType = Union[
    Type[ApiHook],
    Type[RpcServerHook],
    Type[RpcClientHook],
]


def build_hook(hook_cls: HookType)-> List:
    hook_list = []
    cfg_hook_list = getattr(cfg, hook_cls.hook_name)
    for cfg_hook_path in cfg_hook_list:
        spliter = cfg_hook_path.rindex('.')
        mod_name = cfg_hook_path[:spliter]
        cls_name = cfg_hook_path[spliter+1:]
        mod = import_module(mod_name)
        custom_cls = getattr(mod, cls_name)
        instance = custom_cls()
        hook_list.append(instance)
    return hook_list
