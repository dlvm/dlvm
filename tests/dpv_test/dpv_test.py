#!/usr/bin/env python

import distutils.dir_util
import time
from multiprocessing import Process
import unittest
from mock import Mock, patch
from dlvm.dpv.dpv_agent import main, \
    leg_create, leg_delete, leg_export, leg_unexport
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


class RpcFunctionTest(unittest.TestCase):

    tmp_dir = '/tmp/dlvm_test'

    def setUp(self):
        distutils.dir_util.mkpath(self.tmp_dir)

    def tearDown(self):
        distutils.dir_util.remove_tree(self.tmp_dir)

    @patch('dlvm.dpv.dpv_agent.run_dd')
    @patch('dlvm.dpv.dpv_agent.iscsi_create')
    @patch('dlvm.dpv.dpv_agent.lv_create')
    @patch('dlvm.dpv.dpv_agent.DmLinear')
    @patch('dlvm.dpv.dpv_agent.encode_target_name')
    @patch('dlvm.dpv.dpv_agent.dpv_verify')
    @patch('dlvm.dpv.dpv_agent.conf')
    def test_leg_create(
            self, conf, dpv_verify,
            encode_target_name,
            DmLinear,
            iscsi_create, lv_create, run_dd,
    ):
        conf.tmp_dir = self.tmp_dir
        leg_id = '001'
        leg_size = 1024*1024*1024
        dm_context = {
            'thin_block_size': 4*1024*1024,
            'mirror_meta_blocks': 1,
            'mirror_region_size': 4*1024*1024,
            'stripe_number': 1,
            'stripe_chunk_blocks': 1,
            'low_water_mark': 100,
        }
        tran = {
            'major': 1,
            'minor': 0,
        }
        leg_create(leg_id, leg_size, dm_context, tran)

    @patch('dlvm.dpv.dpv_agent.iscsi_delete')
    @patch('dlvm.dpv.dpv_agent.lv_remove')
    @patch('dlvm.dpv.dpv_agent.DmLinear')
    @patch('dlvm.dpv.dpv_agent.encode_target_name')
    @patch('dlvm.dpv.dpv_agent.dpv_verify')
    @patch('dlvm.dpv.dpv_agent.conf')
    def test_leg_delete(
            self, conf, dpv_verify,
            encode_target_name,
            DmLinear,
            iscsi_delete, lv_remove,
    ):
        conf.tmp_dir = self.tmp_dir
        leg_id = '001'
        tran = {
            'major': 1,
            'minor': 0,
        }
        leg_delete(leg_id, tran)

    @patch('dlvm.dpv.dpv_agent.iscsi_export')
    @patch('dlvm.dpv.dpv_agent.encode_target_name')
    @patch('dlvm.dpv.dpv_agent.encode_initiator_name')
    @patch('dlvm.dpv.dpv_agent.dpv_verify')
    def test_leg_export(
            self, dpv_verify,
            encode_target_name,
            encode_initiator_name,
            iscsi_export,
    ):
        leg_id = '001'
        host_name = 'host0'
        tran = {
            'major': 1,
            'minor': 0,
        }
        leg_export(leg_id, host_name, tran)

    @patch('dlvm.dpv.dpv_agent.iscsi_unexport')
    @patch('dlvm.dpv.dpv_agent.encode_target_name')
    @patch('dlvm.dpv.dpv_agent.encode_initiator_name')
    @patch('dlvm.dpv.dpv_agent.dpv_verify')
    def test_leg_unexport(
            self, dpv_verify,
            encode_target_name,
            encode_initiator_name,
            iscsi_unexport,
    ):
        leg_id = '001'
        host_name = 'host0'
        tran = {
            'major': 1,
            'minor': 0,
        }
        leg_unexport(leg_id, host_name, tran)
