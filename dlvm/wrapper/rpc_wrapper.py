from typing import NamedTuple, Type, Optional
import sys
import uuid
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
import xmlrpc.client
from threading import Thread
from logging import LoggerAdapter, getLogger
from datetime import datetime, timedelta
from types import MethodType

from dlvm.common.utils import RequestContext, ExcInfo
from dlvm.common.marshmallow_ext import NtSchema
from dlvm.common.loginit import loginit
from dlvm.common.configure import cfg
from dlvm.common.error import RpcError
from dlvm.common.modules import DistributePhysicalVolume, \
    InitiatorHost, ServiceStatus
from dlvm.wrapper.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook
from dlvm.wrapper.local_ctx import backend_local, frontend_local, \
    Direction


xmlrpc.client.MAXINT = 2**63-1
xmlrpc.client.MININT = -2**63

rpc_server_hook_list = build_hook_list('rpc_server_hook')
rpc_client_hook_list = build_hook_list('rpc_client_hook')


class RpcServerContext(NamedTuple):
    req_ctx: RequestContext
    func_name: str
    lock_dt: datetime
    arg_d: object


class RpcClientContext(NamedTuple):
    req_ctx: RequestContext
    address: str
    timeout: int
    lock_dt: datetime
    func_name: str
    arg_d: object


class RpcExpireError(Exception):

    def __init__(self, curr_dt, lock_dt):
        msg = 'curr_dt={0}, lock_dt={1}'.format(
            curr_dt, lock_dt)
        super(RpcExpireError, self).__init__(msg)


class RpcSchema(NamedTuple):
    arg_schema: Optional[Type[NtSchema]]
    ret_schema: Optional[Type[NtSchema]]


expire_delta = timedelta(seconds=cfg.getint('lock', 'expire_seconds'))


class DlvmRpcServer(ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, listener, port):
        super(DlvmRpcServer, self).__init__(
            (listener, port), allow_none=True, use_builtin_types=True)


class TimeoutTransport(xmlrpc.client.Transport):

    def __init__(self, timeout):
        super(TimeoutTransport, self).__init__(use_builtin_types=True)
        self.__timeout = timeout

    def make_connection(self, host):
        conn = super(TimeoutTransport, self).make_connection(host)
        conn.timeout = self.__timeout
        return conn


def remote_call(
        req_ctx, address, timeout, lock_dt,
        func_name, arg_d):
    transport = TimeoutTransport(timeout)
    hook_ctx = RpcClientContext(
        req_ctx, address, timeout, lock_dt, func_name, arg_d)
    hook_ret_dict = run_pre_hook(
        'rpc_client', rpc_client_hook_list, hook_ctx)
    try:
        with xmlrpc.client.ServerProxy(
                address, transport, allow_none=True) as proxy:
            rpc_func = getattr(proxy, func_name)
            ret_d = rpc_func(str(req_ctx.req_id), lock_dt, arg_d)
    except Exception:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        run_error_hook(
            'rpc_client', rpc_client_hook_list,
            hook_ctx, hook_ret_dict, exc_info)
        raise RpcError
    else:
        run_post_hook(
            'rpc_client', rpc_client_hook_list,
            hook_ctx, hook_ret_dict, ret_d)
        return ret_d


class SyncClient():

    def __init__(
            self, req_ctx, server, port, timeout, lock_dt, rpc_dict):
        self.req_ctx = req_ctx
        self.lock_dt = lock_dt
        self.address = 'http://{0}:{1}'.format(server, port)
        self.timeout = timeout
        self.rpc_dict = rpc_dict

    def __getattr__(self, func_name):

        def func(self, arg=None):
            rpc_schema = self.rpc_dict[func_name]
            if rpc_schema.arg_schema is None:
                arg_d = None
            else:
                arg_d = rpc_schema.arg_schema().dump(arg)
            ret_d = remote_call(
                self.req_ctx, self.address, self.timeout, self.lock_dt,
                func_name, arg_d)
            if rpc_schema.ret_schema is None:
                ret = None
            else:
                ret = rpc_schema.ret_schema().load(ret_d)
            return ret

        return MethodType(func, self)


class AsyncThread(Thread):

    def __init__(
            self, req_ctx, address, timeout,
            lock_dt, rpc_schema, func_name, arg, enforce_func):
        self.req_ctx = req_ctx
        self.address = address
        self.timeout = timeout
        self.lock_dt = lock_dt
        self.rpc_schema = rpc_schema
        self.func_name = func_name
        if rpc_schema.arg_schema is None:
            self.arg_d = None
        else:
            self.arg_d = rpc_schema.arg_schema().dump(arg)
        self.enforce_func = enforce_func

        worklog = frontend_local.worker_ctx.worklog
        direction = frontend_local.worker_ctx.direction
        key = '%s-%s-%s' % (
            self.address, self.func_name, self.arg_d)
        self.key = key
        if direction == Direction.forward and key in worklog:
            bypass = True
        elif direction == Direction.backward and key not in worklog:
            bypass = True
        else:
            bypass = False
        self.bypass = bypass
        self.err = None

        super(AsyncThread, self).__init__()

    def start(self):
        if self.bypass is False:
            super(AsyncThread, self).start()

    def run(self):
        try:
            remote_call(
                self.req_ctx, self.address, self.timeout, self.lock_dt,
                self.func_name, self.arg_d)
        except Exception as e:
            self.err = e

    def wait(self):
        if self.bypass is True:
            return None
        self.join()
        worklog = frontend_local.worker_ctx.worklog
        direction = frontend_local.worker_ctx.direction
        enforce = frontend_local.worker_ctx.enforce
        if self.err is None:
            if direction == Direction.forward:
                worklog.add(self.key)
            else:
                worklog.remove(self.key)
        else:
            if enforce is True:
                self.enforce_func()
                if direction == Direction.forward:
                    worklog.add(self.key)
                else:
                    worklog.remove(self.key)
        return self.err


class AsyncClient():

    def __init__(
            self, req_ctx, server, port, timeout,
            lock_dt, rpc_dict, enforce_func):
        self.req_ctx = req_ctx
        self.address = 'http://{0}:{1}'.format(server, port)
        self.timeout = timeout
        self.lock_dt = lock_dt
        self.rpc_dict = rpc_dict
        self.enforce_func = enforce_func

    def __getattr__(self, func_name):

        def func(arg=None):
            rpc_schema = self.rpc_dict[func_name]
            assert(rpc_schema.ret_schema is None)
            t = AsyncThread(
                self.req_ctx, self.address, self.timeout,
                self.lock_dt, rpc_schema,
                func_name, arg, self.enforce_func)
            t.start()
            return t

        return func


class DlvmRpc():

    def __init__(self, listener, port, logger):
        self.logger = logger
        self.server = DlvmRpcServer(listener, port)
        self.rpc_dict = {}

    def register(self, arg_schema=None, ret_schema=None):

        def create_wrapper(func):

            func_name = func.__name__

            def wrapper(req_id_str, lock_dt, arg_d):
                req_id = uuid.UUID(req_id_str)
                logger = LoggerAdapter(self.logger, {'req_id': req_id})
                req_ctx = RequestContext(req_id, logger)
                hook_ctx = RpcServerContext(
                    req_ctx, func_name, lock_dt, arg_d)
                hook_ret_dict = run_pre_hook(
                    'rpc_server', rpc_server_hook_list, hook_ctx)
                try:
                    curr_dt = datetime.utcnow()
                    if curr_dt - lock_dt > expire_delta:
                        raise RpcExpireError(curr_dt, lock_dt)
                    backend_local.req_ctx = req_ctx
                    if arg_schema is None:
                        ret = func()
                    else:
                        arg = arg_schema().load(arg_d)
                        ret = func(arg)
                    if ret_schema is None:
                        ret_d = None
                    else:
                        ret_d = ret_schema().dump(ret)
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
                        hook_ctx, hook_ret_dict, ret_d)
                    return ret_d

            self.server.register_function(wrapper, func_name)
            self.rpc_dict[func_name] = RpcSchema(
                arg_schema, ret_schema)
            return func

        return create_wrapper

    def start_server(self):
        self.server.serve_forever()

    def sync_client(self, req_ctx, server, port, timeout, lock_dt):
        return SyncClient(
            req_ctx, server, port, timeout, lock_dt, self.rpc_dict)

    def async_client(
            self, req_ctx, server, port, timeout, lock_dt, enforce_func):
        return AsyncClient(
            req_ctx, server, port, timeout, lock_dt,
            self.rpc_dict, enforce_func)


class DpvRpc(DlvmRpc):

    def __init__(self):
        loginit()
        logger = getLogger('dpv_agent')
        listener = cfg.get('rpc', 'dpv_listener')
        port = cfg.getint('rpc', 'dpv_port')
        super(DpvRpc, self).__init__(listener, port, logger)

    def sync_client(self, dpv_name):
        req_ctx = frontend_local.req_ctx
        server = dpv_name
        port = cfg.getinit('rpc', 'dpv_port')
        timeout = cfg.getint('rpc', 'dpv_timeout')
        lock_dt = frontend_local.worker_ctx.lock_dt
        return super(DpvRpc, self).sync_client(
            req_ctx, server, port, timeout, lock_dt)

    def async_client(self, dpv_name):
        req_ctx = frontend_local.req_ctx
        server = dpv_name
        port = cfg.getinit('rpc', 'dpv_port')
        timeout = cfg.getint('rpc', 'dpv_timeout')
        lock_dt = frontend_local.worker_ctx.lock_dt

        def enforce_func():
            session = frontend_local.session
            lock_owner = frontend_local.worker_ctx.lock_owner
            dpv = session.query(DistributePhysicalVolume) \
                .filter_by(dpv_name=dpv_name) \
                .with_lockmode('update') \
                .one()
            assert(dpv.lock.lock_owner == lock_owner)
            if dpv.service_status != ServiceStatus.unavailable:
                dpv.service_status = ServiceStatus.unavailable
                session.add(dpv)
                session.commit()

        super(DpvRpc, self).async_client(
            req_ctx, server, port, timeout, lock_dt, enforce_func)


class IhostRpc(DlvmRpc):

    def __init__(self):
        loginit()
        logger = getLogger('ihost_agent')
        listener = cfg.get('rpc', 'ihost_listener')
        port = cfg.getint('rpc', 'ihost_port')
        super(IhostRpc, self).__init__(listener, port, logger)

    def sync_client(self, ihost_name):
        req_ctx = frontend_local.req_ctx
        server = ihost_name
        port = cfg.getinit('rpc', 'ihost_port')
        timeout = cfg.getint('rpc', 'ihost_timeout')
        lock_dt = frontend_local.worker_ctx.lock_dt
        return super(IhostRpc, self).sync_client(
            req_ctx, server, port, timeout, lock_dt)

    def async_client(self, ihost_name):
        req_ctx = frontend_local.req_ctx
        server = ihost_name
        port = cfg.getinit('rpc', 'ihost_port')
        timeout = cfg.getint('rpc', 'ihost_timeout')
        lock_dt = frontend_local.worker_ctx.lock_dt

        def enforce_func():
            session = frontend_local.session
            lock_owner = frontend_local.worker_ctx.lock_owner
            ihost = session.query(InitiatorHost) \
                .filter_by(ihost_name=ihost_name) \
                .with_lockmode('update') \
                .one()
            assert(ihost.lock.lock_owner == lock_owner)
            if ihost.service_status != ServiceStatus.unavailable:
                ihost.service_status = ServiceStatus.unavailable
                session.add(ihost)
                session.commit()

        super(IhostRpc, self).async_client(
            req_ctx, server, port, timeout, lock_dt, enforce_func)
