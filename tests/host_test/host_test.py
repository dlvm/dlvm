#!/usr/bin/env python

import time
from multiprocessing import Process
import unittest
from mock import patch
from dlvm.host.host_agent import main
from dlvm.utils.rpc_wrapper import WrapperRpcClient


class HostAgentTest(unittest.TestCase):

    host_listener = '127.0.0.1'
    host_port = 9523

    @patch('dlvm.host.host_agent.conf')
    @patch('dlvm.host.host_agent.loginit')
    def setUp(self, loginit, conf):
        conf.host_listener = self.host_listener
        conf.host_port = self.host_port
        self.server = Process(target=main)
        self.server.start()
        time.sleep(0.1)

    def tearDown(self):
        self.server.terminate()

    def test_ping(self):
        client = WrapperRpcClient(
            'localhost',
            self.host_port,
            5,
        )
        ret = client.ping('hello')
        self.assertEqual(ret, 'hello')
