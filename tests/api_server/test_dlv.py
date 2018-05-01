import unittest
import json

from dlvm.common.configure import cfg
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

    db_path = '/tmp/dlvm_test.db'
    db_uri = 'sqlite:////tmp/dlvm_test.db'

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
