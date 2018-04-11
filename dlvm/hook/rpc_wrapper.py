import sys
import time
import uuid
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
from xmlrpc.client import Transport, ServerProxy
from threading import Thread
from logging import LoggerAdapter
from collections import namedtuple

from dlvm.common.utils import RequestContext
from dlvm.hook.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook, ExcInfo


rpc_server_hook_list = build_hook_list('rpc_server_hook')
rpc_client_hook_list = build_hook_list('rpc_client_hook')


RpcServerContext = namedtuple('RpcServerContext', [
    'req_ctx', 'func_name', 'expire_time', 'rpc_arg'])
RpcClientContext = namedtuple('RpcClientContext', [
    'req_ctx', 'server', 'port', 'timeout',
    'func_name', 'expire_time', 'rpc_arg'])
RpcSchema = namedtuple('RpcSchema', [
    'arg_schema_cls', 'ret_schema_cls'])


class RpcExpireError(Exception):

    def __init__(self, curr_time, expire_time):
        msg = 'curr_time={0}, expire_time={1}'.format(
            curr_time, expire_time)
        super(RpcExpireError, self).__init__(msg)


class RpcError(Exception):

    def __init__(self, exc_info):
        self.exc_info = exc_info
        super(RpcError, self).__init__('rpc error')


class DlvmRpcServer(ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, listener, port):
        super(DlvmRpcServer, self).__init__(
            (listener, port), allow_none=True)


class TimeoutTransport(Transport):

    def __init__(self, timeout: int)-> None:
        super(TimeoutTransport, self).__init__()
        self.__timeout = timeout

    def make_connection(self, host: str)-> int:
        conn = super(TimeoutTransport, self).make_connection(host)
        conn.timeout = self.__timeout
        return conn


class RpcClientThread(Thread):

    def __init__(self, rpc_client_ctx, hook_ret_dict, ret_schema_cls):
        self.rpc_client_ctx = rpc_client_ctx
        self.hook_ret_dict = hook_ret_dict
        self.ret_schema_cls = ret_schema_cls
        self.real_ret = None
        self.exc_info = None
        super(RpcClientThread, self).__init__()

    def do_remote_call(self):
        transport = TimeoutTransport(timeout=self.rpc_client_ctx.timeout)
        address = 'http://{0}:{1}'.format(
            self.rpc_client_ctx.server, self.rpc_client_ctx.port)
        with ServerProxy(
                address, transport=transport, allow_none=True) as proxy:
            rpc_func = getattr(proxy, self.rpc_client_ctx.func_name)
            rpc_ret_s = rpc_func(
                self.rpc_client_ctx.req_ctx.req_id.hex,
                self.rpc_client_ctx.expire_time,
                self.rpc_client_ctx.rpc_arg)
            rpc_ret = self.ret_schema_cls().load(rpc_ret_s)
            self.rpc_ret = rpc_ret

    def run(self):
        try:
            self.do_remote_call()
        except Exception:
            etype, value, tb = sys.exc_info()
            exc_info = ExcInfo(etype, value, tb)
            self.exc_info = exc_info

    def get_value(self):
        self.join()
        if self.exc_info is not None:
            run_error_hook(
                'rpc_client', rpc_client_hook_list,
                self.rpc_client_ctx, self.hook_ret_dict, self.exc_info)
            raise RpcError(self.exc_info)
        else:
            run_post_hook(
                'rpc_client', rpc_client_hook_list,
                self.rpc_client_ctx, self.hook_ret_dict, self.rpc_ret)
            return self.rpc_ret


class Rpc():

    def __init__(self, listener, port, logger):
        self.server = DlvmRpcServer(listener, port)
        self.logger = logger
        self.func_dict = {}

    def rpc(self, arg_schema_cls, ret_schema_cls):

        def wrapper(func):

            func_name = func.__name__

            def rpc_wrapper(req_id_hex, expire_time, rpc_arg_s):
                req_id = uuid.UUID(hex=req_id_hex)
                logger = LoggerAdapter(self.logger, {'req_id': req_id})
                req_ctx = RequestContext(req_id, logger)
                hook_ctx = RpcServerContext(
                    req_ctx, func_name, expire_time, rpc_arg_s)
                hook_ret_dict = run_pre_hook(
                    'rpc_server', rpc_server_hook_list, hook_ctx)
                try:
                    curr_time = int(time.time())
                    if expire_time != 0 and curr_time > expire_time:
                        raise RpcExpireError(curr_time, expire_time)
                    rpc_arg = arg_schema_cls().load(rpc_arg_s)
                    rpc_ret = func(req_ctx, rpc_arg)
                    rpc_ret_s = ret_schema_cls().dump(rpc_ret)
                except Exception:
                    etype, value, tb = sys.exc_info()
                    exc_info = ExcInfo(etype, value, tb)
                    run_error_hook(
                        'rpc_server', rpc_server_hook_list,
                        hook_ctx, hook_ret_dict, exc_info)
                    raise
                else:
                    run_post_hook(
                        'rpc_server', rpc_server_hook_list,
                        hook_ctx, hook_ret_dict, rpc_ret_s)
                    return rpc_ret_s

            self.server.register_function(rpc_wrapper, func_name)
            self.func_dict[func_name] = RpcSchema(
                arg_schema_cls, ret_schema_cls)
            return func

        return wrapper

    def start_server(self):
        self.server.serve_forever()

    def async_call(
            self, req_ctx, server, port, timeout,
            func_name, expire_time, rpc_arg):
        rpc_schema = self.func_dict[func_name]
        rpc_arg_s = rpc_schema.arg_schema_cls().dump(rpc_arg)
        hook_ctx = RpcClientContext(
            req_ctx, server, port, timeout,
            func_name, expire_time, rpc_arg_s)
        hook_ret_dict = run_pre_hook(
            'rpc_client', rpc_client_hook_list, hook_ctx)
        t = RpcClientThread(
            hook_ctx, hook_ret_dict, rpc_schema.ret_schema_cls)
        t.start()
        return t
