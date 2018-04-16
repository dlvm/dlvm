import unittest
from unittest.mock import patch
import json
import os

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
]


class DpvTest(unittest.TestCase):

    db_path = '/tmp/dlvm_test.db'
    db_uri = 'sqlite:////tmp/dlvm_test.db'

    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self.dbm = DataBaseManager(self.db_uri)

    def tearDown(self):
        if os.path.isfile(self.db_path):
            os.remove(self.db_path)

    def test_dpvs_get(self):
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
        resp = self.client.get('/dpvs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(len(data['data']), len(fake_dpvs))

    @patch('dlvm.api_server.dpv.DpvClient')
    def test_dpvs_post(self, DpvClient):
        dpv_name = 'dpv0'
        total_size = 512*1024*1024*1024
        free_size = 512*1024*1024*1024
        DpvClient \
            .return_value \
            .dpv_get_info \
            .return_value = {
                'total_size': total_size,
                'free_size': free_size,
            }
        headers = {
            'Content-Type': 'application/json',
        }
        raw_data = {
            'dpv_name': dpv_name,
        }
        data = json.dumps(raw_data)
        resp = self.client.post('/dpvs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dpv = self.dbm.dpv_get(dpv_name)
        self.assertEqual(dpv.dpv_name, dpv_name)
        self.assertEqual(dpv.total_size, total_size)

    def test_dpv_get(self):
        dpv = fake_dpvs[0]
        self.dbm.dpv_create(**dpv)
        dpv_name = dpv['dpv_name']
        total_size = dpv['total_size']
        path = '/dpvs/{0}'.format(dpv_name)
        resp = self.client.get(path)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(data['data']['dpv_name'], dpv_name)
        self.assertEqual(data['data']['total_size'], total_size)

    def test_dpv_delete(self):
        dpv = fake_dpvs[0]
        self.dbm.dpv_create(**dpv)
        dpv_name = dpv['dpv_name']
        dpv1 = self.dbm.dpv_get(dpv_name)
        self.assertEqual(dpv1.dpv_name, dpv_name)
        path = '/dpvs/{0}'.format(dpv_name)
        resp = self.client.delete(path)
        self.assertEqual(resp.status_code, 200)
        dpv2 = self.dbm.dpv_get(dpv_name)
        self.assertEqual(dpv2, None)

    @patch('dlvm.api_server.dpv.DpvClient')
    def test_dpv_update(self, DpvClient):
        dpv = fake_dpvs[0]
        self.dbm.dpv_create(**dpv)
        dpv_name = dpv['dpv_name']
        total_size = 128*1024*1024*1024
        free_size = 128*1024*1024*1024
        DpvClient \
            .return_value \
            .dpv_get_info \
            .return_value = {
                'total_size': total_size,
                'free_size': free_size,
            }
        path = '/dpvs/{0}/update'.format(dpv_name)
        resp = self.client.put(path)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')

        dpv1 = self.dbm.dpv_get(dpv_name)
        self.assertEqual(dpv1.total_size, total_size)
        self.assertEqual(dpv1.free_size, free_size)
