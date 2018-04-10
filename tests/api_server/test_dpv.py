import unittest
from unittest.mock import patch
import json

from dlvm.core.helper import create_all, drop_all
from dlvm.api_server import app

from tests.utils import DataBaseManager


fake_dpvs = [
    {
        'dpv_name': 'dpv0',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
    },
    {
        'dpv_name': 'dpv1',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
    },
    {
        'dpv_name': 'dpv2',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
    },
]


class DpvTest(unittest.TestCase):

    def setUp(self):
        create_all()
        app.config['TESTING'] = True
        self.client = app.test_client()
        self.dbm = DataBaseManager()

    def tearDown(self):
        drop_all()

    def test_dpvs_get(self):
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
        resp = self.client.get('/dpvs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(len(data['body']), len(fake_dpvs))

    @patch('dlvm.api_server.dpv.dpv_async_call')
    def test_dpvs_post(self, dpv_async_call):
        dpv_name = 'dpv0'
        total_size = 512*1024*1024*1024
        free_size = 512*1024*1024*1024
        dpv_async_call \
            .return_value \
            .get_value \
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
