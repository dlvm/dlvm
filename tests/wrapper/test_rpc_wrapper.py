import unittest
import logging
import time
from multiprocessing import Process
import uuid
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client

from dlvm.common.utils import RequestContext
from dlvm.wrapper.rpc_wrapper import DlvmRpcServer, DlvmRpcClient
from dlvm.wrapper.local_ctx import Direction, frontend_local


class DlvmRpcClientTest(unittest.TestCase):

    def setUp(self):

        def add(req_id_hex, expire_dt, x, y):
            return x+y

        def start_server():
            with SimpleXMLRPCServer(('localhost', 8888)) as server:
                server.register_function(add)
                server.serve_forever()

        self.p = Process(target=start_server)
        self.p.start()
        frontend_local.worklog = set()
        frontend_local.direction = Direction.forward
        frontend_local.force = False
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_sync_client(self):
        req_id = uuid.uuid4()
        logger = logging.getLogger('rpc_client_logger')
        req_ctx = RequestContext(req_id, logger)
        client = DlvmRpcClient(req_ctx, 'localhost', 8888, 300, 0, None)
        arg1 = 2
        arg2 = 3
        ret = client.add(arg1, arg2)
        self.assertEqual(ret, arg1+arg2)

    def test_async_client(self):
        req_id = uuid.uuid4()
        logger = logging.getLogger('rpc_client_logger')
        req_ctx = RequestContext(req_id, logger)
        client = DlvmRpcClient(req_ctx, 'localhost', 8888, 300, 0, None)
        async_add = client.async('add')
        arg1 = 2
        arg2 = 3
        t = async_add(arg1, arg2)
        t.wait()


class DlvmRpcServerTest(unittest.TestCase):

    def setUp(self):
        logger = logging.getLogger('rpc_server_logger')
        rpc_server = DlvmRpcServer('localhost', 8888, logger)

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
                "http://localhost:8888/", allow_none=True) as proxy:
            ret = proxy.add(uuid.uuid4().hex, None, arg1, arg2)
        self.assertEqual(ret, arg1+arg2)
