#!/usr/bin/env python

from types import MethodType
import time
import rpyc
from rpyc.utils.server import ThreadedServer


class RpcExpireError(Exception):

    def __init__(self, curr_time, expire_time):
        msg = 'curr_time=%d, expire_time=%d' % (
            curr_time, expire_time)
        super(RpcExpireError, self).__init__(msg)


class MyService(rpyc.Service):
    pass


def rpc(func):
    name = 'exposed_' + func.__name__

    def wrap(self, request_id, expire_time, *args, **kwargs):
        self.logger.debug(
            'request_id=%s rpc call %s %d %s %s',
            request_id,
            func.__name__,
            expire_time,
            args,
            kwargs,
        )
        try:
            curr_time = int(time.time())
            if expire_time != 0 and curr_time > expire_time:
                raise RpcExpireError(curr_time, expire_time)
            ret = func(request_id, *args, **kwargs)
        except Exception:
            self.logger.error(
                'request_id=%s rpc failed: %s', func.__name__,
                exc_info=True,
            )
            raise
        else:
            self.logger.debug(
                'request_id=%s rpc reply: %s %s',
                func.__name__,
                ret,
            )
        return ret
    setattr(MyService, name, wrap)


def start_server(hostname, port, logger):
    setattr(MyService, 'logger', logger)
    t = ThreadedServer(MyService, hostname=hostname, port=port)
    t.start()


class RpcClient(object):

    def __init__(self, addr, port, expire_time, timeout, logger):
        self.conn = rpyc.connect(addr, port)
        self.addr = addr
        self.port = port
        self.expire_time = expire_time
        self.timeout = timeout
        self.logger = logger

    def __getattr__(self, key):
        def func(self, *args, **kwargs):
            remote_func = rpyc.async(getattr(self.conn.root, key))
            ret = remote_func(self.expire_time, *args, **kwargs)
            ret.set_expiry(self.timeout)
            self.logger.debug(
                'rpc call: %s %s %s %s %s',
                key, self.expire_time, args, kwargs, ret,
            )
            return ret
        return MethodType(func, self)
