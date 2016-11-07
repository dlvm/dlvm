#!/usr/bin/env python

import time
from multiprocessing import Process
import unittest
from mock import Mock, patch
from dlvm.host.host_agent import main, bm_get
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.helper import chunks
from dlvm.utils.bitmap import BitMap


class RpcServerTest(unittest.TestCase):

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

    def test_rpc_server(self):
        client = WrapperRpcClient(
            'localhost',
            self.host_port,
            5,
        )
        ret = client.ping('hello')
        self.assertEqual(ret, 'hello')


class RpcFunctionTest(unittest.TestCase):

    @patch('dlvm.host.host_agent.DmPool')
    @patch('dlvm.host.host_agent.host_verify')
    @patch('dlvm.host.host_agent.conf')
    @patch('dlvm.host.host_agent.loginit')
    def test_bm_get_simple(self, loginit, conf, host_verify, DmPool):
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
        tran = {
            'major': 1,
            'minor': 0,
        }
        bm_dict = bm_get('dlv0', tran, dlv_info, [], [])
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
