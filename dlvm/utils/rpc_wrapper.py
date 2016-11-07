#!/usr/bin/env python

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
from xmlrpclib import Transport, ServerProxy


class WrapperRpcServer(ThreadingMixIn, SimpleXMLRPCServer):

    def __init__(self, listener, port):
        return SimpleXMLRPCServer.__init__(
            self, (listener, port), allow_none=True)


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
