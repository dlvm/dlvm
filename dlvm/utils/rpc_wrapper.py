#!/usr/bin/env python

from types import MethodType
import time
import rpyc
from rpyc.utils.server import ThreadedServer
from configure import conf
from error import RpcTimeout


class MyService(rpyc.Service):
    pass


def rpc(func):
    name = 'exposed_' + func.__name__

    def wrap(self, timestamp, *args, **kwargs):
        self.logger.debug(
            'rpc call: %s %d %s %s',
            func.__name__,
            timestamp,
            args,
            kwargs,
        )
        if timestamp != 0 and int(time.time()) > timestamp:
            raise RpcTimeout
        try:
            ret = func(*args, **kwargs)
        except:
            self.logger.error(
                'rpc failed: %s', func.__name__,
                exc_info=True,
            )
            raise
        else:
            self.logger.debug(
                'rpc reply: %s %s',
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

    def __init__(self, addr, port, timestamp, logger):
        self.conn = rpyc.connect(addr, port)
        self.addr = addr
        self.port = port
        self.timestamp = timestamp
        self.logger = logger

    def __getattr__(self, key):
        def func(self, *args, **kwargs):
            remote_func = rpyc.async(getattr(self.conn.root, key))
            ret = remote_func(self.timestamp, *args, **kwargs)
            ret.set_expiry(conf.rpc_expiry)
            self.logger.debug(
                'rpc call: %s %s %s %s %s',
                key, self.timestamp, args, kwargs, ret,
            )
            return ret
        return MethodType(func, self)
