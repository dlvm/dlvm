import unittest
import logging
import time
from multiprocessing import Process
import uuid
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client

from dlvm.common.utils import RequestContext
from dlvm.hook.rpc_wrapper import DlvmRpcServer, DlvmRpcClient


class DlvmRpcClientTest(unittest.TestCase):

    def setUp(self):

        def add(req_id_hex, expire_dt, x, y):
            return x+y

        def start_server():
            with SimpleXMLRPCServer(('localhost', 9522)) as server:
                server.register_function(add)
                server.serve_forever()

        self.p = Process(target=start_server)
        self.p.start()
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_sync_client(self):
        req_id = uuid.uuid4()
        logger = logging.getLogger('rpc_client_logger')
        req_ctx = RequestContext(req_id, logger)
        client = DlvmRpcClient(req_ctx, 'localhost', 9522, 300, 0)
        arg1 = 2
        arg2 = 3
        ret = client.add(arg1, arg2)
        self.assertEqual(ret, arg1+arg2)

    def test_async_client(self):
        req_id = uuid.uuid4()
        logger = logging.getLogger('rpc_client_logger')
        req_ctx = RequestContext(req_id, logger)
        client = DlvmRpcClient(req_ctx, 'localhost', 9522, 300, 0)
        async_add = client.async('add')
        arg1 = 2
        arg2 = 3
        t = async_add(arg1, arg2)
        ret = t.get_value()
        self.assertEqual(ret, arg1+arg2)


class DlvmRpcServerTest(unittest.TestCase):

    def setUp(self):
        logger = logging.getLogger('rpc_server_logger')
        rpc_server = DlvmRpcServer('localhost', 9522, logger)

        @rpc_server.register
        def add(x, y):
            return x+y

        def start_server():
            rpc_server.serve_forever()

        self.p = Process(target=start_server)
        self.p.start()
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_rpc_server(self):
        arg1 = 1
        arg2 = 2
        with xmlrpc.client.ServerProxy(
                "http://localhost:9522/", allow_none=True) as proxy:
            ret = proxy.add(uuid.uuid4().hex, None, arg1, arg2)
        self.assertEqual(ret, arg1+arg2)
