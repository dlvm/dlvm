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

    @patch('dlvm.core.dpv.DpvClient')
    def test_dpvs_post(self, DpvClient):
        dpv_name = 'dpv0'
        total_size = 512*1024*1024*1024
        free_size = 512*1024*1024*1024
        DpvClient\
            .return_value\
            .get_size\
            .return_value\
            .get_value\
            .return_value = ({
                'total_size': total_size,
                'free_size': free_size,
            },)
        headers = {
            'Content-Type': 'application/json',
        }
        raw_data = {
            'dpv_name': dpv_name,
        }
        data = json.dumps(raw_data)
        resp = self.client.post('/dpvs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

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
        body = data['body']
        self.assertEqual(body['dpv_name'], dpv_name)
        self.assertEqual(body['total_size'], total_size)
