#!/usr/bin/env python

import distutils.dir_util
import time
from multiprocessing import Process
import unittest
from mock import Mock, patch
from dlvm.dpv_agent import main, \
    leg_create, leg_delete, leg_export, leg_unexport, \
    fj_leg_export, fj_leg_unexport, fj_login, \
    fj_mirror_start, fj_mirror_stop, fj_mirror_status, \
    dpv_sync, cj_leg_export, cj_leg_unexport, cj_login
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.bitmap import BitMap


class RpcServerTest(unittest.TestCase):

    dpv_listener = '127.0.0.1'
    dpv_port = 9522

    @patch('dlvm.dpv_agent.conf')
    @patch('dlvm.dpv_agent.queue_init')
    @patch('dlvm.dpv_agent.loginit')
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

    @patch('dlvm.dpv_agent.run_dd')
    @patch('dlvm.dpv_agent.iscsi_create')
    @patch('dlvm.dpv_agent.lv_create')
    @patch('dlvm.dpv_agent.DmLinear')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
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
        obt = {
            'major': 1,
            'minor': 0,
        }
        leg_create(leg_id, obt, str(leg_size), dm_context)

    @patch('dlvm.dpv_agent.iscsi_delete')
    @patch('dlvm.dpv_agent.lv_remove')
    @patch('dlvm.dpv_agent.DmLinear')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_leg_delete(
            self, conf, dpv_verify,
            encode_target_name,
            DmLinear,
            iscsi_delete, lv_remove,
    ):
        conf.tmp_dir = self.tmp_dir
        leg_id = '001'
        obt = {
            'major': 1,
            'minor': 0,
        }
        leg_delete(leg_id, obt)

    @patch('dlvm.dpv_agent.iscsi_export')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.encode_initiator_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    def test_leg_export(
            self, dpv_verify,
            encode_target_name,
            encode_initiator_name,
            iscsi_export,
    ):
        leg_id = '001'
        host_name = 'host0'
        obt = {
            'major': 1,
            'minor': 0,
        }
        leg_export(leg_id, obt, host_name)

    @patch('dlvm.dpv_agent.iscsi_unexport')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.encode_initiator_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    def test_leg_unexport(
            self, dpv_verify,
            encode_target_name,
            encode_initiator_name,
            iscsi_unexport,
    ):
        leg_id = '001'
        host_name = 'host0'
        obt = {
            'major': 1,
            'minor': 0,
        }
        leg_unexport(leg_id, obt, host_name)

    @patch('dlvm.dpv_agent.iscsi_export')
    @patch('dlvm.dpv_agent.iscsi_create')
    @patch('dlvm.dpv_agent.DmLinear')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.encode_initiator_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_fj_leg_export(
            self, conf, dpv_verify,
            encode_target_name,
            encode_initiator_name,
            DmLinear,
            iscsi_create,
            iscsi_export,
    ):
        leg_id = '001'
        fj_name = 'fj0'
        src_name = 'dpv1'
        leg_size = 1024*1024*1024
        obt = {
            'major': 1,
            'minor': 0,
        }
        fj_leg_export(leg_id, obt, fj_name, src_name, str(leg_size))

    @patch('dlvm.dpv_agent.iscsi_unexport')
    @patch('dlvm.dpv_agent.iscsi_delete')
    @patch('dlvm.dpv_agent.DmLinear')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.encode_initiator_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_fj_leg_unexport(
            self, conf, dpv_verify,
            encode_target_name,
            encode_initiator_name,
            DmLinear,
            iscsi_delete,
            iscsi_unexport,
    ):
        leg_id = '001'
        fj_name = 'fj0'
        src_name = 'dpv1'
        obt = {
            'major': 1,
            'minor': 0,
        }
        fj_leg_unexport(leg_id, obt, fj_name, src_name)

    @patch('dlvm.dpv_agent.iscsi_login')
    @patch('dlvm.dpv_agent.lv_create')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_fj_login(
            self, conf, dpv_verify,
            encode_target_name,
            lv_create,
            iscsi_login,
    ):
        leg_id = '001'
        fj_name = 'fj0'
        dst_name = 'dpv2'
        dst_id = '002'
        obt = {
            'major': 1,
            'minor': 0,
        }
        fj_login(
            leg_id, obt, fj_name, dst_name, dst_id)

    @patch('dlvm.dpv_agent.Thread')
    @patch('dlvm.dpv_agent.run_dd')
    @patch('dlvm.dpv_agent.DmMirror')
    @patch('dlvm.dpv_agent.DmBasic')
    @patch('dlvm.dpv_agent.iscsi_login')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_fj_mirror_start(
            self, conf, dpv_verify,
            encode_target_name,
            iscsi_login,
            DmBasic, DmMirror,
            run_dd, Thread,
    ):
        conf.tmp_dir = self.tmp_dir
        leg_id = '001'
        fj_name = 'fj0'
        dst_name = 'dpv2'
        dst_id = '002'
        leg_size = 1024*1024*1024
        dmc = {
            'thin_block_size': 2*1024*1024,
        }
        bm_size = leg_size / dmc['thin_block_size']
        bm = BitMap(bm_size).tohexstring()
        obt = {
            'major': 1,
            'minor': 0,
        }
        fj_mirror_start(
            leg_id, obt, fj_name, dst_name, dst_id, str(leg_size), dmc, bm)

    @patch('dlvm.dpv_agent.DmLinear')
    @patch('dlvm.dpv_agent.lv_remove')
    @patch('dlvm.dpv_agent.iscsi_logout')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_fj_mirror_stop(
            self, conf, dpv_verify,
            encode_target_name,
            iscsi_logout, lv_remove,
            DmLinear,
    ):
        leg_id = '001'
        fj_name = 'fj0'
        dst_id = '002'
        leg_size = 1024*1024*1024
        obt = {
            'major': 1,
            'minor': 0,
        }
        fj_mirror_stop(
            leg_id, obt, fj_name, dst_id, str(leg_size))

    @patch('dlvm.dpv_agent.DmMirror')
    @patch('dlvm.dpv_agent.DmBasic')
    @patch('dlvm.dpv_agent.conf')
    def test_fj_mirror_status(
            self, conf,
            DmBasic, DmMirror,
    ):
        dm_mock = Mock()
        DmBasic.return_value = dm_mock
        dm_mock.get_type.return_value = 'raid'
        DmMirror.return_value = dm_mock
        status = {
            'hc0': 'A',
            'hc1': 'A',
            'curr': 30,
            'total': 100,
            'sync_action': None,
            'mismatch_cnt': 0,
        }
        dm_mock.status.return_value = status
        leg_id = '001'
        ret = fj_mirror_status(leg_id)
        self.assertEqual(ret, status)

    @patch('dlvm.dpv_agent.dpv_get_info')
    @patch('dlvm.dpv_agent.lv_get_all')
    @patch('dlvm.dpv_agent.dm_get_all')
    @patch('dlvm.dpv_agent.iscsi_backstore_delete')
    @patch('dlvm.dpv_agent.iscsi_backstore_get_all')
    @patch('dlvm.dpv_agent.iscsi_target_delete')
    @patch('dlvm.dpv_agent.iscsi_target_get_all')
    @patch('dlvm.dpv_agent.run_dd')
    @patch('dlvm.dpv_agent.iscsi_create')
    @patch('dlvm.dpv_agent.lv_get_path')
    @patch('dlvm.dpv_agent.DmLinear')
    @patch('dlvm.dpv_agent.encode_target_name')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.iscsi_unexport')
    @patch('dlvm.dpv_agent.conf')
    def test_dpv_sync(
            self, conf, iscsi_unexport,
            dpv_verify,
            encode_target_name,
            DmLinear,
            iscsi_create, lv_get_path, run_dd,
            iscsi_target_get_all,
            iscsi_target_delete,
            iscsi_backstore_get_all,
            iscsi_backstore_delete,
            dm_get_all,
            lv_get_all,
            dpv_get_info,
    ):
        conf.tmp_dir = self.tmp_dir
        dpv_info = []
        dm_context = {
            'thin_block_size': 4*1024*1024,
            'mirror_meta_blocks': 1,
            'mirror_region_size': 4*1024*1024,
            'stripe_number': 1,
            'stripe_chunk_blocks': 1,
            'low_water_mark': 100,
        }
        leg_info = {
            'leg_id': '001',
            'leg_size': str(1024*1024*1024),
            'dm_context': dm_context,
            'ihost_name': None,
        }
        dpv_info.append(leg_info)
        obt = {
            'major': 1,
            'minor': 0,
        }
        dpv_sync(dpv_info, obt)

    @patch('dlvm.dpv_agent.iscsi_export')
    @patch('dlvm.dpv_agent.iscsi_create')
    @patch('dlvm.dpv_agent.DmLinear')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_cj_leg_export(
            self, conf, dpv_verify, DmLinear,
            iscsi_create, iscsi_export,
    ):
        leg_id = '001'
        cj_name = 'cj0'
        dst_name = 'dpv0'
        leg_size = str(10*1024*1024*1024)
        obt = {
            'major': 1,
            'minor': 0,
        }
        cj_leg_export(
            leg_id, obt, cj_name, dst_name, leg_size)

    @patch('dlvm.dpv_agent.iscsi_unexport')
    @patch('dlvm.dpv_agent.iscsi_delete')
    @patch('dlvm.dpv_agent.DmLinear')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_cj_leg_unexport(
            self, conf, dpv_verify, DmLinear,
            iscsi_delete, iscsi_unexport,
    ):
        leg_id = '001'
        cj_name = 'cj0'
        dst_name = 'dpv0'
        obt = {
            'major': 1,
            'minor': 0,
        }
        cj_leg_unexport(
            leg_id, obt, cj_name, dst_name)

    @patch('dlvm.dpv_agent.lv_create')
    @patch('dlvm.dpv_agent.iscsi_login')
    @patch('dlvm.dpv_agent.DmThin')
    @patch('dlvm.dpv_agent.DmPool')
    @patch('dlvm.dpv_agent.dpv_verify')
    @patch('dlvm.dpv_agent.conf')
    def test_cj_login(
            self, conf, dpv_verify,
            DmPool, DmThin,
            iscsi_login, lv_create,
    ):
        leg_id = '001'
        cj_name = 'cj0'
        src_name = 'dpv1'
        src_id = '002'
        leg_size = str(10*1024*1024*1024)
        obt = {
            'major': 1,
            'minor': 0,
        }
        cj_login(
            leg_id, obt, cj_name, src_name, src_id, leg_size)
