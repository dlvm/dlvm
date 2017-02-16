#!/usr/bin/env python

import time
from multiprocessing import Process
import unittest
from mock import Mock, patch
from dlvm.thost_agent import main, bm_get, \
    dlv_aggregate, dlv_degregate, \
    dlv_suspend, dlv_resume, \
    snapshot_create, snapshot_delete, \
    remirror, leg_remove
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.helper import chunks, dlv_info_encode
from dlvm.utils.bitmap import BitMap


class RpcServerTest(unittest.TestCase):

    thost_listener = '127.0.0.1'
    thost_port = 9523

    @patch('dlvm.thost_agent.conf')
    @patch('dlvm.thost_agent.queue_init')
    @patch('dlvm.thost_agent.loginit')
    def setUp(self, loginit, queue_init, conf):
        conf.thost_listener = self.thost_listener
        conf.thost_port = self.thost_port
        self.server = Process(target=main)
        self.server.start()
        time.sleep(0.1)

    def tearDown(self):
        self.server.terminate()

    def test_rpc_server(self):
        client = WrapperRpcClient(
            'localhost',
            self.thost_port,
            5,
        )
        ret = client.ping('hello')
        self.assertEqual(ret, 'hello')


class RpcFunctionTest(unittest.TestCase):

    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.thost_verify')
    @patch('dlvm.thost_agent.conf')
    @patch('dlvm.thost_agent.queue_init')
    @patch('dlvm.thost_agent.loginit')
    def test_bm_get_simple(
            self,
            loginit,
            queue_init,
            conf,
            thost_verify,
            DmPool,
    ):
        conf.bm_throttle = 0
        status_mock = Mock()
        status_mock.return_value = {
            'used_data': 1,
        }
        dm_mock = Mock()
        dm_mock.status = status_mock
        DmPool.return_value = dm_mock

        dm_context = {
            'thin_block_size': 4*1024*1024,
            'mirror_meta_blocks': 1,
            'mirror_region_size': 4*1024*1024,
            'stripe_number': 1,
            'stripe_chunk_blocks': 1,
            'low_water_mark': 100,
        }
        groups = [{
            'idx': 0,
            'group_size': 16*1024*1024,
            'legs': [{
                'leg_id': '000',
                'idx': 0,
                'leg_size': 16*1024*1024+4*1024*1024,
                'dpv_name': 'dpv0',
            }, {
                'leg_id': '001',
                'idx': 1,
                'leg_size': 16*1024*1024+4*1024*1024,
                'dpv_name': 'dpv1',
            }],
        }, {
            'idx': 1,
            'group_size': 512*1024*1024,
            'legs': [{
                'leg_id': '002',
                'idx': 0,
                'leg_size': 512*1024*1024+4*1024*1024,
                'dpv_name': 'dpv2',
            }, {
                'leg_id': '003',
                'idx': 1,
                'leg_size': 512*1024*1024+4*1024*1024,
                'dpv_name': 'dpv3',
            }],
        }]
        dlv_info = {
            'dlv_size': 1024*1024*1024,
            'data_size': 512*1024*1024,
            'thin_id': 0,
            'dm_context': dm_context,
            'groups': groups,
        }
        dlv_info_encode(dlv_info)
        obt = {
            'major': 1,
            'minor': 0,
        }
        bm_dict = bm_get('dlv0', obt, dlv_info, [], [])
        for group in groups:
            legs = group['legs']
            legs.sort(key=lambda x: x['idx'])
            for leg0, leg1 in chunks(legs, 2):
                bm_size = leg0['leg_size'] / dm_context['thin_block_size']
                key = '%s-%s' % (
                    leg0['leg_id'], leg1['leg_id'])
                val = bm_dict[key]
                bm = BitMap.fromhexstring(val)
                for i in xrange(bm_size):
                    self.assertTrue(bm.test(i))

    @patch('dlvm.thost_agent.Thread')
    @patch('dlvm.thost_agent.iscsi_login')
    @patch('dlvm.thost_agent.DmError')
    @patch('dlvm.thost_agent.DmThin')
    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.DmMirror')
    @patch('dlvm.thost_agent.DmStripe')
    @patch('dlvm.thost_agent.DmLinear')
    @patch('dlvm.thost_agent.DmBasic')
    @patch('dlvm.thost_agent.encode_target_name')
    @patch('dlvm.thost_agent.thost_verify')
    @patch('dlvm.thost_agent.conf')
    @patch('dlvm.thost_agent.report_pool')
    @patch('dlvm.thost_agent.report_multi_legs')
    @patch('dlvm.thost_agent.report_single_leg')
    @patch('dlvm.thost_agent.queue_init')
    @patch('dlvm.thost_agent.loginit')
    def test_dlv_aggregate(
            self, loginit,
            queue_init, report_single_leg,
            report_multi_legs, report_pool,
            conf, thost_verify, encode_target_name,
            DmBasic, DmLinear, DmStripe, DmMirror,
            DmPool, DmThin, DmError,
            iscsi_login, Thread,
    ):
        dm_context = {
            'thin_block_size': 4*1024*1024,
            'mirror_meta_blocks': 1,
            'mirror_region_size': 4*1024*1024,
            'stripe_number': 1,
            'stripe_chunk_blocks': 1,
            'low_water_mark': 100,
        }
        groups = [{
            'idx': 0,
            'group_size': 16*1024*1024,
            'legs': [{
                'leg_id': '000',
                'idx': 0,
                'leg_size': 16*1024*1024+4*1024*1024,
                'dpv_name': 'dpv0',
            }, {
                'leg_id': '001',
                'idx': 1,
                'leg_size': 16*1024*1024+4*1024*1024,
                'dpv_name': 'dpv1',
            }],
        }, {
            'idx': 1,
            'group_size': 512*1024*1024,
            'legs': [{
                'leg_id': '002',
                'idx': 0,
                'leg_size': 512*1024*1024+4*1024*1024,
                'dpv_name': 'dpv2',
            }, {
                'leg_id': '003',
                'idx': 1,
                'leg_size': 512*1024*1024+4*1024*1024,
                'dpv_name': 'dpv3',
            }],
        }]
        dlv_info = {
            'dlv_size': 1024*1024*1024,
            'data_size': 512*1024*1024,
            'thin_id': 0,
            'dm_context': dm_context,
            'groups': groups,
        }
        dlv_info_encode(dlv_info)
        obt = {
            'major': 1,
            'minor': 0,
        }
        dlv_aggregate('dlv0', obt, dlv_info)

    @patch('dlvm.thost_agent.iscsi_logout')
    @patch('dlvm.thost_agent.DmError')
    @patch('dlvm.thost_agent.DmThin')
    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.DmMirror')
    @patch('dlvm.thost_agent.DmStripe')
    @patch('dlvm.thost_agent.DmLinear')
    @patch('dlvm.thost_agent.DmBasic')
    @patch('dlvm.thost_agent.encode_target_name')
    @patch('dlvm.thost_agent.thost_verify')
    @patch('dlvm.thost_agent.conf')
    @patch('dlvm.thost_agent.queue_init')
    @patch('dlvm.thost_agent.loginit')
    def test_dlv_degregate(
            self, loginit, queue_init, conf,
            thost_verify, encode_target_name,
            DmBasic, DmLinear, DmStripe, DmMirror,
            DmPool, DmThin, DmError,
            iscsi_logout,
    ):
        groups = [{
            'idx': 0,
            'legs': [{
                'leg_id': '000',
                'idx': 0,
                'dpv_name': 'dpv0',
            }, {
                'leg_id': '001',
                'idx': 1,
                'dpv_name': 'dpv1',
            }],
        }, {
            'idx': 1,
            'legs': [{
                'leg_id': '002',
                'idx': 0,
                'dpv_name': 'dpv2',
            }, {
                'leg_id': '003',
                'idx': 1,
                'dpv_name': 'dpv3',
            }],
        }]
        dlv_info = {
            'groups': groups,
        }
        dlv_info_encode(dlv_info)
        obt = {
            'major': 1,
            'minor': 0,
        }
        dlv_degregate('dlv0', obt, dlv_info)

    @patch('dlvm.thost_agent.Thread')
    @patch('dlvm.thost_agent.DmError')
    @patch('dlvm.thost_agent.DmThin')
    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.DmMirror')
    @patch('dlvm.thost_agent.DmStripe')
    @patch('dlvm.thost_agent.DmLinear')
    @patch('dlvm.thost_agent.DmBasic')
    @patch('dlvm.thost_agent.thost_verify')
    @patch('dlvm.thost_agent.conf')
    @patch('dlvm.thost_agent.report_pool')
    @patch('dlvm.thost_agent.queue_init')
    @patch('dlvm.thost_agent.loginit')
    def test_dlv_suspend(
            self, loginit, queue_init,
            report_pool, conf, thost_verify,
            DmBasic, DmLinear, DmStripe, DmMirror,
            DmPool, DmThin, DmError,
            Thread,
    ):
        dlv_info = {
            'dlv_size': 1024*1024*1024,
        }
        dlv_info_encode(dlv_info)
        obt = {
            'major': 1,
            'minor': 0,
        }
        dlv_suspend('dlv0', obt, dlv_info)

    @patch('dlvm.thost_agent.Thread')
    @patch('dlvm.thost_agent.DmError')
    @patch('dlvm.thost_agent.DmThin')
    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.DmMirror')
    @patch('dlvm.thost_agent.DmStripe')
    @patch('dlvm.thost_agent.DmLinear')
    @patch('dlvm.thost_agent.DmBasic')
    @patch('dlvm.thost_agent.thost_verify')
    @patch('dlvm.thost_agent.conf')
    @patch('dlvm.thost_agent.report_pool')
    @patch('dlvm.thost_agent.queue_init')
    @patch('dlvm.thost_agent.loginit')
    def test_dlv_resume(
            self, loginit, queue_init,
            report_pool, conf, thost_verify,
            DmBasic, DmLinear, DmStripe, DmMirror,
            DmPool, DmThin, DmError,
            Thread,
    ):
        dm_context = {
            'thin_block_size': 4*1024*1024,
            'mirror_meta_blocks': 1,
            'mirror_region_size': 4*1024*1024,
            'stripe_chunk_blocks': 1,
            'stripe_number': 1,
            'low_water_mark': 100,
        }
        dlv_info = {
            'dlv_size': 1024*1024*1024,
            'data_size': 512*1024*1024,
            'thin_id': 0,
            'dm_context': dm_context,
        }
        dlv_info_encode(dlv_info)
        obt = {
            'major': 1,
            'minor': 0,
        }
        dlv_resume('dlv0', obt, dlv_info)

    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.thost_verify')
    def test_snapshot_create(
            self, thost_verify, DmPool,
    ):
        dlv_name = 'dlv0'
        thin_id = 1
        ori_thin_id = 0
        obt = {
            'major': 1,
            'minor': 0,
        }
        snapshot_create(dlv_name, obt, thin_id, ori_thin_id)

    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.thost_verify')
    def test_snapshot_delete(
            self, thost_verify, DmPool,
    ):
        dlv_name = 'dlv0'
        thin_id = 1
        obt = {
            'major': 1,
            'minor': 0,
        }
        snapshot_delete(dlv_name, obt, thin_id)

    @patch('dlvm.thost_agent.Thread')
    @patch('dlvm.thost_agent.iscsi_login')
    @patch('dlvm.thost_agent.DmError')
    @patch('dlvm.thost_agent.DmThin')
    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.DmMirror')
    @patch('dlvm.thost_agent.DmStripe')
    @patch('dlvm.thost_agent.DmLinear')
    @patch('dlvm.thost_agent.DmBasic')
    @patch('dlvm.thost_agent.encode_target_name')
    @patch('dlvm.thost_agent.thost_verify')
    @patch('dlvm.thost_agent.conf')
    @patch('dlvm.thost_agent.report_pool')
    @patch('dlvm.thost_agent.report_multi_legs')
    @patch('dlvm.thost_agent.report_single_leg')
    @patch('dlvm.thost_agent.queue_init')
    @patch('dlvm.thost_agent.loginit')
    def test_remirror(
            self, loginit,
            queue_init, report_single_leg,
            report_multi_legs, report_pool,
            conf, thost_verify, encode_target_name,
            DmBasic, DmLinear, DmStripe, DmMirror,
            DmPool, DmThin, DmError,
            iscsi_login, Thread,
    ):
        dm_context = {
            'thin_block_size': 4*1024*1024,
            'mirror_meta_blocks': 1,
            'mirror_region_size': 4*1024*1024,
            'stripe_number': 1,
            'stripe_chunk_blocks': 1,
            'low_water_mark': 100,
        }
        groups = [{
            'idx': 0,
            'group_size': 16*1024*1024,
            'legs': [{
                'leg_id': '000',
                'idx': 0,
                'leg_size': 16*1024*1024+4*1024*1024,
                'dpv_name': 'dpv0',
            }, {
                'leg_id': '001',
                'idx': 1,
                'leg_size': 16*1024*1024+4*1024*1024,
                'dpv_name': 'dpv1',
            }],
        }, {
            'idx': 1,
            'group_size': 512*1024*1024,
            'legs': [{
                'leg_id': '002',
                'idx': 0,
                'leg_size': 512*1024*1024+4*1024*1024,
                'dpv_name': 'dpv2',
            }, {
                'leg_id': '003',
                'idx': 1,
                'leg_size': 512*1024*1024+4*1024*1024,
                'dpv_name': 'dpv3',
            }],
        }]
        dlv_info = {
            'dlv_size': 1024*1024*1024,
            'data_size': 512*1024*1024,
            'thin_id': 0,
            'dm_context': dm_context,
            'groups': groups,
        }
        dlv_info_encode(dlv_info)
        obt = {
            'major': 1,
            'minor': 0,
        }
        src_id = '001'
        dst_leg = {
            'leg_id': '004',
            'idx': 0,
            'leg_size': 16*1024*1024+4*1024*1024,
            'dpv_name': 'dpv4',
        }
        remirror('dlv0', obt, dlv_info, src_id, dst_leg)

    @patch('dlvm.thost_agent.iscsi_logout')
    @patch('dlvm.thost_agent.DmError')
    @patch('dlvm.thost_agent.DmThin')
    @patch('dlvm.thost_agent.DmPool')
    @patch('dlvm.thost_agent.DmMirror')
    @patch('dlvm.thost_agent.DmStripe')
    @patch('dlvm.thost_agent.DmLinear')
    @patch('dlvm.thost_agent.DmBasic')
    @patch('dlvm.thost_agent.encode_target_name')
    @patch('dlvm.thost_agent.thost_verify')
    @patch('dlvm.thost_agent.conf')
    @patch('dlvm.thost_agent.queue_init')
    @patch('dlvm.thost_agent.loginit')
    def test_leg_remove(
            self, loginit, queue_init, conf,
            thost_verify, encode_target_name,
            DmBasic, DmLinear, DmStripe, DmMirror,
            DmPool, DmThin, DmError,
            iscsi_logout,
    ):
        groups = [{
            'idx': 0,
            'legs': [{
                'leg_id': '000',
                'idx': 0,
                'dpv_name': 'dpv0',
            }, {
                'leg_id': '001',
                'idx': 1,
                'dpv_name': 'dpv1',
            }],
        }, {
            'idx': 1,
            'legs': [{
                'leg_id': '002',
                'idx': 0,
                'dpv_name': 'dpv2',
            }, {
                'leg_id': '003',
                'idx': 1,
                'dpv_name': 'dpv3',
            }],
        }]
        dlv_info = {
            'groups': groups,
        }
        dlv_info_encode(dlv_info)
        obt = {
            'major': 1,
            'minor': 0,
        }
        leg_remove('dlv0', obt, dlv_info, '001')
