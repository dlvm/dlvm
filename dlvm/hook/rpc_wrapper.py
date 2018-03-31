from typing import List, Callable, Any, \
    Sequence, Mapping, MutableMapping, Optional
from types import MethodType
import time
from logging import Logger
import traceback

import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc import AsyncResult

from dlvm.common.utils import ReqId, RequestContext
from dlvm.hook.hook import build_hook, HookRet, RpcRet, \
    RpcServerHook, RpcServerParam, RpcClientHook, RpcClientParam


rpc_server_hook_list: List[RpcServerHook] = build_hook(RpcServerHook)
rpc_client_hook_list: List[RpcClientHook] = build_hook(RpcClientHook)


class RpcExpireError(Exception):

    def __init__(self, curr_time: int, expire_time: int)-> None:
        msg = 'curr_time={0}, expire_time={1}'.format(
            curr_time, expire_time)
        super(RpcExpireError, self).__init__(msg)


class RpcServer():

    def __init__(
            self, name: str, logger: Logger, hostname: str, port: int)-> None:
        self.logger = logger
        self.hostname = hostname
        self.port = port
        self.service = type(name, (rpyc.Service,), {})

    def rpc(self, func: Callable[..., RpcRet])-> Callable[..., RpcRet]:
        name = 'exposed_' + func.__name__
        logger = self.logger

        def wrapper(
                self: Any, req_id: ReqId, expire_time: int,
                args: Sequence)-> RpcRet:
            hook_ret_dict: MutableMapping[
                RpcServerHook, Optional[HookRet]] = {}
            req_ctx = RequestContext(req_id, logger)
            param = RpcServerParam(
                func.__name__, req_ctx, expire_time, args)
            for hook in rpc_server_hook_list:
                try:
                    hook_ret = hook.pre_hook(param)
                except Exception:
                    logger.error(
                        'rpc server pre_hook failed: %s %s',
                        repr(param), repr(hook),
                        exc_info=True)
                else:
                    hook_ret_dict[hook] = hook_ret
            try:
                curr_time = int(time.time())
                if expire_time != 0 and curr_time > expire_time:
                    raise RpcExpireError(curr_time, expire_time)
                ret = func(req_ctx, *args)
            except Exception as e:
                calltrace = traceback.format_exc()
                for hook in rpc_server_hook_list:
                    hook_ret = hook_ret_dict.get(hook)
                    try:
                        hook.error_hook(
                            param, hook_ret, e, calltrace)
                    except Exception:
                        logger.error(
                            'rpc server error_hook failed: %s %s %s %s %s',
                            repr(hook), repr(param),
                            hook_ret, e, calltrace,
                            exc_info=True)
                raise
            else:
                for hook in rpc_server_hook_list:
                    hook_ret = hook_ret_dict.get(hook)
                    try:
                        hook.post_hook(
                            param, hook_ret, ret)
                    except Exception:
                        logger.error(
                            'rpc server post_hook failed: %s %s %s %s',
                            repr(hook), repr(param),
                            hook_ret, ret, exc_info=True)
                return ret
        setattr(self.service, name, wrapper)
        return func

    def start(self)-> None:
        t = ThreadedServer(
            self.service, hostname=self.hostname, port=self.port)
        t.start()


class RpcResponse():

    def __init__(
            self, req_ctx: RequestContext, param: RpcClientParam,
            hook_ret_dict: Mapping[RpcClientHook, Optional[HookRet]],
            conn: Any, async_result: AsyncResult)-> None:
        self.req_ctx = req_ctx
        self.param = param
        self.hook_ret_dict = hook_ret_dict
        self.conn = conn
        self.async_result = async_result

    def get_value(self)-> RpcRet:
        try:
            self.async_result.wait()
            ret: RpcRet = self.async_result.value
        except Exception as e:
            calltrace = traceback.format_exc()
            for hook in rpc_client_hook_list:
                hook_ret = self.hook_ret_dict.get(hook)
                try:
                    hook.error_hook(
                        self.param, hook_ret, e, calltrace)
                except Exception:
                    self.req_ctx.logger.error(
                        'rpc client error_hook failed: %s %s %s %s %s',
                        repr(hook), repr(self.param),
                        hook_ret, e, calltrace,
                        exc_info=True)
            raise
        else:
            for hook in rpc_client_hook_list:
                hook_ret = self.hook_ret_dict.get(hook)
                try:
                    hook.post_hook(
                        self.param, hook_ret, ret)
                except Exception:
                    self.req_ctx.logger.error(
                        'rpc client post_hook failed: %s %s %s %s',
                        repr(hook), repr(self.param), ret, hook_ret,
                        exc_info=True)
            return ret


class RpcClient():

    def __init__(
            self, req_ctx: RequestContext, addr: str, port: int,
            expire_time: int, timeout: int)-> None:
        self.req_ctx = req_ctx
        self.addr = addr
        self.port = port
        self.expire_time = expire_time
        self.timeout = timeout

    def __getattr__(self, key: str)-> Callable[..., Any]:

        def func(self: Any, *args: Any)-> RpcResponse:
            param = RpcClientParam(
                key, self.req_ctx, self.expire_time, args)
            hook_ret_dict: MutableMapping[
                RpcClientHook, Optional[HookRet]] = {}
            for hook in rpc_client_hook_list:
                try:
                    hook_ret = hook.pre_hook(param)
                except Exception:
                    self.req_ctx.logger.error(
                        'rpc client pre_hook failed: %s %s',
                        repr(param), repr(hook), exc_info=True)
                else:
                    hook_ret_dict[hook] = hook_ret
            try:
                conn = rpyc.connect(self.addr, self.port)
                remote_func = rpyc.async(getattr(conn.root, key))
                async_result = remote_func(
                    self.req_ctx.req_id, self.expire_time, args)
                async_result.set_expiry(self.timeout)
            except Exception as e:
                calltrace = traceback.format_exc()
                for hook in rpc_client_hook_list:
                    hook_ret = hook_ret_dict.get(hook)
                    try:
                        hook.error_hook(
                            param, hook_ret, e, calltrace)
                    except Exception:
                        self.req_ctx.logger.error(
                            'rpc server error_hook failed: %s %s %s %s %s',
                            repr(hook), repr(param),
                            hook_ret, e, calltrace,
                            exc_info=True)
                raise
            return RpcResponse(
                self.req_ctx, param, hook_ret_dict, conn, async_result)

        return MethodType(func, self)
