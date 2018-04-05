import unittest
import logging
import time
from multiprocessing import Process
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

from dlvm.common.utils import RequestContext
from dlvm.hook.rpc_wrapper import DlvmRpcServer, rpc_async_call


class DlvmRpcServerTest(unittest.TestCase):

    def setUp(self):

        def add(req_ctx, add_arg):
            a = add_arg['a']
            b = add_arg['b']
            return {'result': a+b}

        def start_rpc_server():
            logger = logging.getLogger('dummy_logger')
            server = DlvmRpcServer('localhost', 9522, logger)
            server.register_function(add)
            server.serve_forever()

        self.p = Process(target=start_rpc_server)
        self.p.start()
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_add(self):
        with ServerProxy('http://localhost:9522') as proxy:
            ret = proxy.add('0', 0, {'a': 1, 'b': 2})
        self.assertEqual(ret['result'], 3)


class DlvmRpcClienTest(unittest.TestCase):

    def setUp(self):

        def add(req_id, expire_time, add_arg):
            a = add_arg['a']
            b = add_arg['b']
            return {'result': a+b}

        def start_rpc_server():
            server = SimpleXMLRPCServer(('localhost', 9522))
            server.register_function(add)
            server.serve_forever()

        self.p = Process(target=start_rpc_server)
        self.p.start()
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_add(self):
        logger = logging.getLogger('dummy_logger')
        req_ctx = RequestContext('0', logger)
        t = rpc_async_call(
            req_ctx, 'localhost', 9522, 300, 'add', 0, {'a': 1, 'b': 2})
        ret = t.get_value()
        self.assertEqual(ret['result'], 3)
