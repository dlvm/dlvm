from typing import Type, List, Sequence, Mapping, Any, Union, Optional
from abc import ABC, abstractmethod
from importlib import import_module

from dlvm.common.configure import cfg
from dlvm.common.utils import RequestContext


class HookRet(ABC):
    pass


class ApiHook(ABC):

    __name__ = 'api_hook'

    @abstractmethod
    def pre_hook(self)->None:
        pass

    @abstractmethod
    def post_hook(self)->None:
        pass

    @abstractmethod
    def error_hook(self)->None:
        pass


class RpcServerParam():

    def __init__(
            self, func_name, req_ctx: RequestContext, expire_time: int,
            args: Sequence, kwargs: Mapping)-> None:
        self.func_name = func_name
        self.req_ctx = req_ctx
        self.expire_time = expire_time
        self.args = args,
        self.kwargs = kwargs

    def __repr__(self):
        return (
            'RpcServerParam('
            'func_name={0},req_ctx={1},expire_time={2},args={3},kwargs={4})'
        ).format(
            self.func_name, repr(self.req_ctx), self.expire_time,
            self.args, self.kwargs)


class RpcServerHook(ABC):

    __name__ = 'rpc_server_hook'

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
            self, func_name, req_ctx: RequestContext, expire_time: int,
            args: Sequence, kwargs: Mapping)-> None:
        self.func_name = func_name
        self.req_ctx = req_ctx
        self.expire_time = expire_time
        self.args = args,
        self.kwargs = kwargs

    def __repr__(self):
        return (
            'RpcParam('
            'func_name={0},req_ctx={1},expire_time={2},args={3},kwargs={4})'
        ).format(
            self.func_name, repr(self.req_ctx), self.expire_time,
            self.args, self.kwargs)


class RpcClientHook(ABC):

    __name__ = 'rpc_client_hook'

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


def build_hook(
        hook_cls: Union[
            Type[ApiHook],
            Type[RpcServerHook],
            Type[RpcClientHook]])-> List:
    hook_list = []
    cfg_hook_list = cfg.getliststr('hook', hook_cls.__name__)
    for cfg_hook_path in cfg_hook_list:
        spliter = cfg_hook_path.rindex('.')
        mod_name = cfg_hook_path[:spliter]
        cls_name = cfg_hook_path[spliter+1:]
        mod = import_module(mod_name)
        custom_cls = getattr(mod, cls_name)
        instance = custom_cls()
        hook_list.append(instance)
    return hook_list
