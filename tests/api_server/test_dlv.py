import unittest
from unittest.mock import patch
import json

from dlvm.common.constant import DEFAULT_SNAP_NAME
from dlvm.common.configure import cfg
from dlvm.common.modules import DlvStatus
from dlvm.api_server import app

from tests.utils import DataBaseManager


fake_dpvs = [
    {
        'dpv_name': 'dpv0',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
    },
    {
        'dpv_name': 'dpv1',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
    },
    {
        'dpv_name': 'dpv2',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
    },
    {
        'dpv_name': 'dpv3',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
    },
]

fake_dvg = {'dvg_name': 'dvg0'}

fake_dlv = {
    'dlv_name': 'dlv0',
    'dlv_size': 100*1024*1024*1024,
    'init_size': 50*1024*1024*1024,
    'stripe_number': 1,
    'bm_ignore': False,
    'dvg_name': 'dvg0',
    'groups': [{
        'group_idx': 0,
        'group_size': 20*1024*1024,
        'legs': [{
            'leg_idx': 0,
            'leg_size': 20*1024*1024,
            'dpv_name': 'dpv0',
        }, {
            'leg_idx': 1,
            'leg_size': 20*1024*1024,
            'dpv_name': 'dpv1',
        }],
    }, {
        'group_idx': 1,
        'group_size': 50*1024*1024*1024,
        'legs': [{
            'leg_idx': 0,
            'leg_size': 50*1024*1024*1024,
            'dpv_name': 'dpv0',
        }, {
            'leg_idx': 1,
            'leg_size': 50*1024*1024*1024,
            'dpv_name': 'dpv1',
        }],
    }]
}


class DlvTest(unittest.TestCase):

    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self.dbm = DataBaseManager(cfg.get('database', 'db_uri'))
        self.dbm.setup()

    def tearDown(self):
        self.dbm.teardown()

    def test_dlvs_get(self):
        self.dbm.dvg_create(**fake_dvg)
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
            self.dbm.dvg_extend(
                fake_dvg['dvg_name'], fake_dpv['dpv_name'])
        self.dbm.dlv_create(fake_dlv)
        resp = self.client.get('/dlvs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(len(data['data']), 1)
        dlv = data['data'][0]
        self.assertEqual(dlv['dlv_name'], fake_dlv['dlv_name'])

    @patch('dlvm.wrapper.state_machine.sm_handler')
    def test_dlvs_post(self, sm_handler):
        self.dbm.dvg_create(**fake_dvg)
        headers = {
            'Content-Type': 'application/json',
        }
        raw_data = {
            'dlv_name': 'dlv0',
            'dlv_size': 100*1024*1024*1024,
            'stripe_number': 4,
            'init_size': 50*1024*1024*1024,
            'bm_ignore': False,
            'dvg_name': fake_dvg['dvg_name']
        }
        data = json.dumps(raw_data)
        resp = self.client.post('/dlvs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 201)
        dlv = self.dbm.dlv_get(raw_data['dlv_name'])
        self.assertEqual(dlv.dlv_name, raw_data['dlv_name'])
        self.assertEqual(dlv.dlv_size, raw_data['dlv_size'])
        self.assertEqual(dlv.stripe_number, raw_data['stripe_number'])
        self.assertEqual(dlv.data_size, raw_data['init_size'])
        self.assertEqual(dlv.bm_ignore, raw_data['bm_ignore'])
        self.assertEqual(dlv.dvg_name, raw_data['dvg_name'])
        self.assertEqual(dlv.bm_dirty, False)
        self.assertEqual(dlv.status, DlvStatus.creating)
        snap = self.dbm.snap_get(raw_data['dlv_name'], DEFAULT_SNAP_NAME)
        self.assertEqual(snap.snap_name, DEFAULT_SNAP_NAME)
        self.assertEqual(snap.dlv_name, raw_data['dlv_name'])
        self.assertEqual(sm_handler.apply_async.call_count, 1)

    def test_dlv_get(self):
        self.dbm.dvg_create(**fake_dvg)
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
            self.dbm.dvg_extend(
                fake_dvg['dvg_name'], fake_dpv['dpv_name'])
        self.dbm.dlv_create(fake_dlv)
        path = '/dlvs/{0}'.format(fake_dlv['dlv_name'])
        resp = self.client.get(path)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(data['data']['dlv_name'], fake_dlv['dlv_name'])
        self.assertEqual(len(data['data']['groups']), 2)

    @patch('dlvm.wrapper.state_machine.sm_handler')
    def test_dlv_delete(self, sm_handler):
        self.dbm.dvg_create(**fake_dvg)
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
            self.dbm.dvg_extend(
                fake_dvg['dvg_name'], fake_dpv['dpv_name'])
        self.dbm.dlv_create(fake_dlv)
        dlv1 = self.dbm.dlv_get(fake_dlv['dlv_name'])
        self.assertEqual(dlv1.status, DlvStatus.available)
        path = '/dlvs/{0}'.format(fake_dlv['dlv_name'])
        resp = self.client.delete(path)
        self.assertEqual(resp.status_code, 201)
        self.dbm.update_session()
        dlv2 = self.dbm.dlv_get(fake_dlv['dlv_name'])
        self.assertEqual(dlv2.status, DlvStatus.deleting)
        self.assertEqual(sm_handler.apply_async.call_count, 1)
