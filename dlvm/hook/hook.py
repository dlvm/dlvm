from typing import TypeVar, Generic, List, NamedTuple, NewType, Mapping

from abc import ABC, abstractmethod
from importlib import import_module

from dlvm.common.configure import cfg
from dlvm.common.utils import RequestContext, WorkContext


HookRet = NewType('HookRet', object)

RpcArg = NewType('RpcArg', Mapping)
RpcRet = NewType('RpcRet', Mapping)


class ApiContext(NamedTuple):
    req_ctx: RequestContext
    work_ctx: WorkContext
    func_name: str


class ApiHook(ABC):

    hook_name = 'api_hook'

    @abstractmethod
    def pre_hook(self, x: int)-> None:
        raise NotImplementedError

    @abstractmethod
    def post_hook(self)-> None:
        raise NotImplementedError

    @abstractmethod
    def error_hook(self)-> None:
        raise NotImplementedError


class RpcServerContext(NamedTuple):
    req_ctx: RequestContext
    func_name: str
    expire_time: int
    rpc_arg: RpcArg


class RpcServerHook(ABC):

    hook_name = 'rpc_server_hook'

    @abstractmethod
    def pre_hook(self, rpc_server_ctx: RpcServerContext)-> HookRet:
        raise NotImplementedError

    @abstractmethod
    def post_hook(
            self, rpc_server_ctx: RpcServerContext,
            hook_ret: HookRet, rpc_ret: RpcRet)-> None:
        raise NotImplementedError

    @abstractmethod
    def error_hook(
            self, rpc_server_ctx: RpcServerContext, hook_ret: HookRet,
            e: Exception, calltrace: str)-> None:
        raise NotImplementedError


class RpcClientContext(NamedTuple):
    req_ctx: RequestContext
    server: str
    port: int
    timeout: int
    rpc_name: str
    expire_time: int
    rpc_arg: RpcArg


class RpcClientHook(ABC):

    hook_name = 'rpc_client_hook'

    @abstractmethod
    def pre_hook(self, rpc_client_ctx: RpcClientContext)-> HookRet:
        raise NotImplementedError

    @abstractmethod
    def post_hook(
            self, rpc_client_ctx: RpcClientContext,
            hook_ret: HookRet, rpc_ret: RpcRet)-> None:
        raise NotImplementedError

    @abstractmethod
    def error_hook(
            self, rpc_client_ctx: RpcClientContext, hook_ret: HookRet,
            e: Exception, calltrace: str)-> None:
        raise NotImplementedError


ApiHookConcrete = NewType('ApiHookConcrete', ApiHook)
RpcServerHookConcrete = NewType('RpcServerHookConcrete', RpcServerHook)
RpcClientHookConcrete = NewType('RpcClientHookConcrete', RpcClientHook)

T = TypeVar(
    'T',
    ApiHookConcrete,
    RpcServerHookConcrete,
    RpcClientHookConcrete,
)


class HookBuilder(Generic[T]):

    def __call__(self, hook_name: str)-> List[T]:
        hook_list = []
        cfg_hook_list = getattr(cfg.hook, hook_name)
        for cfg_hook_path in cfg_hook_list:
            spliter = cfg_hook_path.rindex('.')
            mod_name = cfg_hook_path[:spliter]
            cls_name = cfg_hook_path[spliter+1:]
            mod = import_module(mod_name)
            custom_cls = getattr(mod, cls_name)
            instance = custom_cls()
            hook_list.append(instance)
        return hook_list
