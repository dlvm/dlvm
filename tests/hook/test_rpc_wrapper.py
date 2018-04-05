import unittest
import logging
import time
from multiprocessing import Process
from xmlrpc.client import ServerProxy

from dlvm.hook.rpc_wrapper import DlvmRpcServer


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
