#!/usr/bin/env python

import time
from multiprocessing import Process
import unittest
from mock import Mock, patch
from dlvm.dpv.dpv_agent import main
from dlvm.utils.rpc_wrapper import WrapperRpcClient


class RpcServerTest(unittest.TestCase):

    dpv_listener = '127.0.0.1'
    dpv_port = 9522

    @patch('dlvm.dpv.dpv_agent.conf')
    @patch('dlvm.dpv.dpv_agent.queue_init')
    @patch('dlvm.dpv.dpv_agent.loginit')
    def setUp(self, loginit, queue_init, conf):
        conf.dpv_listener = self.dpv_listener
        conf.dpv_port = self.dpv_port
        self.server = Process(target=main)
        self.server.start()
        time.sleep(0.1)

    def tearDown(self):
        self.server.terminate()

    def test_rpc_server(self):
        client = WrapperRpcClient(
            'localhost',
            self.dpv_port,
            5,
        )
        ret = client.ping('hello')
        self.assertEqual(ret, 'hello')
