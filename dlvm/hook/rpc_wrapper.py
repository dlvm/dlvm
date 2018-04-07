from typing import MutableMapping, Callable, Optional, NamedTuple, List

from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
from xmlrpc.client import Transport, ServerProxy
from threading import Thread
import traceback
import time
from logging import Logger, LoggerAdapter

from dlvm.common.utils import ReqId, RequestContext
from dlvm.hook.hook import hook_builder, HookRet, RpcArg, RpcRet, \
    RpcServerHook, RpcServerHookConcrete, RpcServerContext, \
    RpcClientHook, RpcClientHookConcrete, RpcClientContext


rpc_server_hook_list: List[RpcServerHookConcrete] = hook_builder(
    RpcServerHook.hook_name)
rpc_client_hook_list: List[RpcClientHookConcrete] = hook_builder(
    RpcClientHook.hook_name)


class RpcExpireError(Exception):

    def __init__(self, curr_time: int, expire_time: int)-> None:
        msg = 'curr_time={0}, expire_time={1}'.format(
            curr_time, expire_time)
        super(RpcExpireError, self).__init__(msg)


class DlvmRpcServer(ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, listener: str, port: int, logger: Logger)-> None:
        self.logger = logger
        return super(DlvmRpcServer, self).__init__(
            (listener, port), allow_none=True)

    def register_function(
            self, func: Callable[[RequestContext, RpcArg], RpcRet])-> None:

        def wrapper_func(
                req_id: ReqId, expire_time: int, rpc_arg: RpcArg)-> RpcRet:
            hook_ret_dict: MutableMapping[RpcServerHookConcrete, HookRet] = {}
            logger = LoggerAdapter(self.logger, {'req_id': req_id})
            req_ctx = RequestContext(req_id, logger)
            rpc_server_ctx = RpcServerContext(
                req_ctx, func.__name__, expire_time, rpc_arg)
            for hook in rpc_server_hook_list:
                try:
                    hook_ret = hook.pre_hook(rpc_server_ctx)
                except Exception:
                    req_ctx.logger.error(
                        'rpc server pre_hook failed: %s %s',
                        hook, rpc_server_ctx, exc_info=True)
                    hook_ret_dict[hook] = HookRet(None)
                else:
                    hook_ret_dict[hook] = hook_ret
            try:
                curr_time = int(time.time())
                if expire_time != 0 and curr_time > expire_time:
                    raise RpcExpireError(curr_time, expire_time)
                rpc_ret = func(req_ctx, rpc_arg)
            except Exception as e:
                calltrace = traceback.format_exc()
                for hook in rpc_server_hook_list:
                    hook_ret = hook_ret_dict[hook]
                    try:
                        hook.error_hook(
                            rpc_server_ctx, hook_ret, e, calltrace)
                    except Exception:
                        req_ctx.logger.error(
                            'rpc server error_hook failed: %s %s %s %s %s',
                            hook, rpc_server_ctx, hook_ret, e, calltrace,
                            exc_info=True)
                raise
            else:
                for hook in rpc_server_hook_list:
                    hook_ret = hook_ret_dict[hook]
                    try:
                        hook.post_hook(
                            rpc_server_ctx, hook_ret, rpc_ret)
                    except Exception:
                        req_ctx.logger.error(
                            'rpc server post_hook failed: %s %s %s %s',
                            hook, rpc_server_ctx, hook_ret, rpc_ret,
                            exc_info=True)
                return rpc_ret

        return super(DlvmRpcServer, self).register_function(
            wrapper_func, func.__name__)


class TimeoutTransport(Transport):

    def __init__(self, timeout: int)-> None:
        super(TimeoutTransport, self).__init__()
        self.__timeout = timeout

    def make_connection(self, host: str)-> int:
        conn = super(TimeoutTransport, self).make_connection(host)
        conn.timeout = self.__timeout
        return conn


class RpcErrInfo(NamedTuple):
    e: Exception
    calltrace: str


class RpcError(Exception):

    def __init__(self, err_info: RpcErrInfo)-> None:
        self.err_info = err_info
        super(RpcError, self).__init__('rpc error')


class RpcClientThread(Thread):

    def __init__(
            self, rpc_client_ctx: RpcClientContext,
            hook_ret_dict: MutableMapping[RpcClientHookConcrete, HookRet],
    )-> None:
        self.rpc_client_ctx = rpc_client_ctx
        self.hook_ret_dict = hook_ret_dict
        self.rpc_ret: Optional[RpcRet] = None
        self.err_info: Optional[RpcErrInfo] = None
        super(RpcClientThread, self).__init__()

    def do_remote_call(self)-> None:
        transport = TimeoutTransport(timeout=self.rpc_client_ctx.timeout)
        address = 'http://{0}:{1}'.format(
            self.rpc_client_ctx.server, self.rpc_client_ctx.port)
        with ServerProxy(
                address, transport=transport, allow_none=True) as proxy:
            rpc_func = getattr(proxy, self.rpc_client_ctx.rpc_name)
            rpc_ret = rpc_func(
                self.rpc_client_ctx.req_ctx.req_id,
                self.rpc_client_ctx.expire_time,
                self.rpc_client_ctx.rpc_arg)
            self.rpc_ret = rpc_ret

    def run(self)-> None:
        try:
            self.do_remote_call()
        except Exception as e:
            calltrace = traceback.format_exc()
            self.err_info = RpcErrInfo(e, calltrace)

    def get_value(self)-> RpcRet:
        self.join()
        if self.err_info is not None:
            for hook in rpc_client_hook_list:
                hook_ret = self.hook_ret_dict[hook]
                try:
                    hook.error_hook(
                        self.rpc_client_ctx, hook_ret,
                        self.err_info.e, self.err_info.calltrace)
                except Exception:
                    self.rpc_client_ctx.req_ctx.logger.error(
                        'rpc client error_hook failed: %s %s %s %s %s',
                        hook, self.rpc_client_ctx, hook_ret,
                        self.err_info.e, self.err_info.calltrace,
                        exc_info=True)
            raise RpcError(self.err_info)
        else:
            assert(self.rpc_ret is not None)
            for hook in rpc_client_hook_list:
                hook_ret = self.hook_ret_dict[hook]
                try:
                    hook.post_hook(
                        self.rpc_client_ctx, hook_ret, self.rpc_ret)
                except Exception:
                    self.rpc_client_ctx.req_ctx.logger.error(
                        'rpc client post_hook failed: %s %s %s %s',
                        hook, self.rpc_client_ctx, hook_ret, self.rpc_ret,
                        exc_info=True)
            return self.rpc_ret


def rpc_async_call(
        req_ctx: RequestContext, server: str, port: int, timeout: int,
        rpc_name: str, expire_time: int, rpc_arg: RpcArg)-> RpcClientThread:
    rpc_client_ctx = RpcClientContext(
        req_ctx, server, port, timeout, rpc_name, expire_time, rpc_arg)
    hook_ret_dict: MutableMapping[RpcClientHookConcrete, HookRet] = {}
    for hook in rpc_client_hook_list:
        try:
            hook_ret = hook.pre_hook(rpc_client_ctx)
        except Exception:
            req_ctx.logger.error(
                'rpc client pre_hook failed: %s %s',
                hook, rpc_client_ctx, exc_info=True)
            hook_ret_dict[hook] = HookRet(None)
        else:
            hook_ret_dict[hook] = hook_ret
    try:
        t = RpcClientThread(rpc_client_ctx, hook_ret_dict)
        t.start()
    except Exception as e:
        calltrace = traceback.format_exc()
        for hook in rpc_client_hook_list:
            hook_ret = hook_ret_dict[hook]
            try:
                hook.error_hook(
                    rpc_client_ctx, hook_ret, e, calltrace)
            except Exception:
                req_ctx.logger.error(
                    'rpc client error_hook failed: %s %s %s %s %s',
                    hook, rpc_client_ctx, hook_ret, e, calltrace,
                    exc_info=True)
        raise
    else:
        return t
