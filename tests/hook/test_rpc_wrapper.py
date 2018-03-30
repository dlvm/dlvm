import unittest
import logging
import time
from multiprocessing import Process

import rpyc
from rpyc.utils.server import ThreadedServer

from dlvm.hook.rpc_wrapper import RpcServer, RpcClient, RpcRet
from dlvm.common.utils import RequestContext
from dlvm.common.bitmap import BitMap


class RpcWrapperServerTest(unittest.TestCase):

    def setUp(self):

        def start_rpc_server():
            logger = logging.getLogger('dummy_logger')
            rpc_server = RpcServer(
                'test_server', logger, 'localhost', 9522)

            @rpc_server.rpc
            def add(req_ctx, x, y):
                ret = x+y
                return RpcRet((ret,))

            @rpc_server.rpc
            def bitmap_operation(req_ctx, inp_bitmap):
                return RpcRet((inp_bitmap,))
            rpc_server.start()

        self.p = Process(target=start_rpc_server)
        self.p.start()
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_add(self):
        conn = rpyc.connect('localhost', 9522)
        a = 20*1024*1024*1024*1024
        b = 30*1024*1024*1024*1024
        ret, = conn.root.add('0', 0, args=[a, b])
        self.assertEqual(ret, a+b)

    def test_bitmap(self):
        conn = rpyc.connect('localhost', 9522)
        vol_size = 1024*1024*1024*1024
        chunk_size = 2 * 1024 * 1024
        bm_size = vol_size // chunk_size
        bm = BitMap(bm_size)
        for i in range(bm_size):
            if i % 7 != 0:
                bm.set(i)
        ret, = conn.root.bitmap_operation('0', 0, args=[bm])
        for i in range(bm_size):
            test_ret = bm.test(i)
            if i % 7 == 0:
                self.assertFalse(test_ret)
            else:
                self.assertTrue(test_ret)


class RpcWrapperClientTest(unittest.TestCase):

    def setUp(self):
        def start_rpc_server():

            def add(x, y):
                return RpcRet((x+y,))

            class TestService(rpyc.Service):

                def exposed_add(self, req_ctx, expire_time, args):
                    return add(*args)

            t = ThreadedServer(
                TestService, hostname='localhost', port=9522)
            t.start()

        self.p = Process(target=start_rpc_server)
        self.p.start()
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_add(self):
        logger = logging.getLogger('dummy_logger')
        req_ctx = RequestContext('0', logger)
        client = RpcClient(
            req_ctx, 'localhost', 9522, 0, 300)
        a = 20*1024*1024*1024*1024
        b = 30*1024*1024*1024*1024
        ret = client.add(a, b)
        result, = ret.get_value()
        self.assertEqual(result, a+b)
