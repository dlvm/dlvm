#!/usr/bin/env python

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
from xmlrpclib import Transport, ServerProxy


class WrapperRpcServer(ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, listener, port, logger):
        self.logger = logger
        return SimpleXMLRPCServer.__init__(
            self, (listener, port), allow_none=True)

    def register_function(self, func):
        def wrapper_func(*args, **kwargs):
            try:
                self.logger.debug(
                    'rpc call: %s %s %s',
                    func.__name__,
                    args,
                    kwargs,
                )
                ret = func(*args, **kwargs)
                self.logger.debug(
                    'rpc reply: %s %s',
                    func.__name__,
                    ret,
                )
            except:
                self.logger.error(
                    'rpc failed: %s', func.__name__,
                    exc_info=True,
                )
                raise
            return ret
        wrapper_func.__name__ = func.__name__
        return SimpleXMLRPCServer.register_function(
            self, wrapper_func)


class TimeoutTransport(Transport):

    def __init__(self, timeout):
        Transport.__init__(self)
        self._timeout = timeout

    def make_connection(self, host):
        conn = Transport.make_connection(self, host)
        conn.timeout = self._timeout
        return conn


class WrapperRpcClient(ServerProxy):

    def __init__(self, server, port, timeout):
        transport = TimeoutTransport(timeout=timeout)
        address = 'http://{server}:{port}'.format(
            server=server, port=port)
        return ServerProxy.__init__(
            self, address, transport=transport, allow_none=True)
