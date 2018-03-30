import unittest
import logging
import time
from multiprocessing import Process

import rpyc
# from rpyc import AsyncResult

from dlvm.hook.rpc_wrapper import RpcServer, RpcRet
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
        max_size = 16*1024*1024*1024*1024
        chunk_size = 128 * 1024
        bm_size = max_size // chunk_size
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
