from typing import NamedTuple, Sequence, Mapping
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
    args: Sequence
    kwargs: Mapping


class RpcClientContext(NamedTuple):
    req_ctx: RequestContext
    address: str
    timeout: int
    lock_dt: datetime
    func_name: str
    rpc_args: Sequence
    rpc_kwargs: Mapping


class RpcExpireError(Exception):

    def __init__(self, curr_dt, lock_dt):
        msg = 'curr_dt={0}, lock_dt={1}'.format(
            curr_dt, lock_dt)
        super(RpcExpireError, self).__init__(msg)


expire_delta = timedelta(seconds=cfg.getint('lock', 'expire_seconds'))


class DlvmRpcServer(ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, listener, port, logger):
        self.logger = logger
        super(DlvmRpcServer, self).__init__(
            (listener, port), allow_none=True, use_builtin_types=True)

    def register(self, func):

        func_name = func.__name__

        def wrapper(req_id_str, lock_dt, *args, **kwargs):
            req_id = uuid.UUID(req_id_str)
            logger = LoggerAdapter(self.logger, {'req_id': req_id})
            req_ctx = RequestContext(req_id, logger)
            hook_ctx = RpcServerContext(
                req_ctx, func_name, lock_dt, args, kwargs)
            hook_ret_dict = run_pre_hook(
                'rpc_server', rpc_server_hook_list, hook_ctx)
            try:
                curr_dt = datetime.utcnow()
                if curr_dt - lock_dt > expire_delta:
                    raise RpcExpireError(curr_dt, lock_dt)
                backend_local.req_ctx = req_ctx
                rpc_ret = func(*args, **kwargs)
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
                    hook_ctx, hook_ret_dict, rpc_ret)
                return rpc_ret

        self.register_function(wrapper, func_name)
        return func


class TimeoutTransport(xmlrpc.client.Transport):

    def __init__(self, timeout):
        super(TimeoutTransport, self).__init__(use_builtin_types=True)
        self.__timeout = timeout

    def make_connection(self, host):
        conn = super(TimeoutTransport, self).make_connection(host)
        conn.timeout = self.__timeout
        return conn


class RpcClientThread(Thread):

    def __init__(self, remote_func, args, kwargs, key, bypass, enforce_func):
        self.remote_func = remote_func
        self.args = args
        self.kwargs = kwargs
        self.err = None
        self.key = key
        self.bypass = bypass
        self.enforce_func = enforce_func
        super(RpcClientThread, self).__init__()

    def start(self):
        if self.bypass is False:
            super(RpcClientThread, self).start()

    def run(self):
        # all async call should ignore ret
        try:
            self.remote_func(*self.args, **self.kwargs)
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


class DlvmRpcClient():

    def __init__(
            self, req_ctx, server, port, timeout, lock_dt, enforce_func):
        self.req_ctx = req_ctx
        self.lock_dt = lock_dt
        self.timeout = timeout
        self.address = 'http://{0}:{1}'.format(server, port)
        self.transport = TimeoutTransport(timeout)
        self.enforce_func = enforce_func

    def async(self, func_name):
        remote_func = getattr(self, func_name)

        def async_func(self, *args, **kwargs):
            worklog = frontend_local.worker_ctx.worklog
            direction = frontend_local.worker_ctx.direction
            key = '%s-%s-%s-%s' % (
                self.address, func_name, args, kwargs)
            bypass = False
            if direction == Direction.forward and key in worklog:
                bypass = True
            if direction == Direction.backward and key not in worklog:
                bypass = True
            t = RpcClientThread(
                remote_func=remote_func, args=args, kwargs=kwargs,
                key=key, bypass=bypass, enforce_func=self.enforce_func)
            t.start()
            return t

        return MethodType(async_func, self)

    def __getattr__(self, func_name):

        def remote_func(self, *args, **kwargs):
            hook_ctx = RpcClientContext(
                self.req_ctx, self.address, self.timeout, self.lock_dt,
                func_name, args, kwargs)
            hook_ret_dict = run_pre_hook(
                'rpc_client', rpc_client_hook_list, hook_ctx)
            try:
                with xmlrpc.client.ServerProxy(
                        self.address,
                        transport=self.transport,
                        allow_none=True) as proxy:
                    rpc_func = getattr(proxy, func_name)
                    rpc_ret = rpc_func(
                        str(self.req_ctx.req_id), self.lock_dt,
                        *args, **kwargs)
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
                    hook_ctx, hook_ret_dict, rpc_ret)
                return rpc_ret
        return MethodType(remote_func, self)


class DpvServer(DlvmRpcServer):

    def __init__(self):
        loginit()
        logger = getLogger('dpv_agent')
        listener = cfg.get('rpc', 'dpv_listener')
        port = cfg.getint('rpc', 'dpv_port')
        super(DpvServer, self).__init__(listener, port, logger)


class IhostServer(DlvmRpcServer):

    def __init__(self):
        loginit()
        logger = getLogger('ihost_agent')
        listener = cfg.get('rpc', 'ihost_listener')
        port = cfg.getint('rpc', 'ihost_port')
        super(DpvServer, self).__init__(listener, port, logger)


class DpvClient(DlvmRpcClient):

    def __init__(self, dpv_name):
        req_ctx = frontend_local.req_ctx
        server = dpv_name
        port = cfg.getint('rpc', 'dpv_port')
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

        super(DpvClient, self).__init__(
            req_ctx, server, port, timeout, lock_dt, enforce_func)


class IhostClient(DlvmRpcClient):

    def __init__(self, ihost_name):
        req_ctx = frontend_local.req_ctx
        server = ihost_name
        port = cfg.getint('rpc', 'ihost_port')
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

        super(IhostClient, self).__init__(
            req_ctx, server, port, timeout, lock_dt, enforce_func)
