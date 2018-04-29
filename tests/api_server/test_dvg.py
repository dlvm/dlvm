import unittest
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

fake_dvg = {'dvg_name': 'dvg0'}


class DvgTest(unittest.TestCase):

    db_path = '/tmp/dlvm_test.db'
    db_uri = 'sqlite:////tmp/dlvm_test.db'

    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self.dbm = DataBaseManager(self.db_uri)

    def tearDown(self):
        if os.path.isfile(self.db_path):
            os.remove(self.db_path)

    def test_dvgs_get(self):
        self.dbm.dvg_create(**fake_dvg)
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
            self.dbm.dvg_extend(
                fake_dvg['dvg_name'], fake_dpv['dpv_name'])
        resp = self.client.get('/dvgs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(len(data['data']), 1)

    def test_dvgs_post(self):
        dvg_name = 'dvg0'
        headers = {
            'Content-Type': 'application/json',
        }
        raw_data = {
            'dvg_name': dvg_name,
        }
        data = json.dumps(raw_data)
        resp = self.client.post('/dvgs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dvg = self.dbm.dvg_get(dvg_name)
        self.assertEqual(dvg.dvg_name, dvg_name)
        self.assertEqual(dvg.total_size, 0)
        self.assertEqual(dvg.free_size, 0)

    def test_dvg_get(self):
        self.dbm.dvg_create(**fake_dvg)
        dvg_name = fake_dvg['dvg_name']
        path = '/dvgs/{0}'.format(dvg_name)
        resp = self.client.get(path)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(data['data']['dvg_name'], dvg_name)

    def test_dvg_delete(self):
        self.dbm.dvg_create(**fake_dvg)
        dvg_name = fake_dvg['dvg_name']
        dvg1 = self.dbm.dvg_get(dvg_name)
        self.assertEqual(dvg1.dvg_name, dvg_name)
        path = '/dvgs/{0}'.format(dvg_name)
        resp = self.client.delete(path)
        self.assertEqual(resp.status_code, 200)
        dvg2 = self.dbm.dvg_get(dvg_name)
        self.assertEqual(dvg2, None)

    def test_dvg_extend(self):
        self.dbm.dvg_create(**fake_dvg)
        self.dbm.dpv_create(**fake_dpvs[0])
        dvg_name = fake_dvg['dvg_name']
        dvg1 = self.dbm.dvg_get(dvg_name)
        self.assertEqual(dvg1.total_size, 0)
        self.assertEqual(dvg1.free_size, 0)
        path = '/dvgs/{0}/extend'.format(dvg_name)
        headers = {
            'Content-Type': 'application/json',
        }
        raw_data = {
            'dpv_name': fake_dpvs[0]['dpv_name'],
        }
        data = json.dumps(raw_data)
        resp = self.client.put(path, headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dvg2 = self.dbm.dvg_get(dvg_name)
        self.assertEqual(dvg2.total_size, fake_dpvs[0]['total_size'])
        self.assertEqual(dvg2.free_size, fake_dpvs[0]['free_size'])

    def test_dvg_reduce(self):
        self.dbm.dvg_create(**fake_dvg)
        self.dbm.dpv_create(**fake_dpvs[0])
        dvg_name = fake_dvg['dvg_name']
        dpv_name = fake_dpvs[0]['dpv_name']
        self.dbm.dvg_extend(dvg_name, dpv_name)
        dvg1 = self.dbm.dvg_get(dvg_name)
        self.assertEqual(dvg1.total_size, fake_dpvs[0]['total_size'])
        self.assertEqual(dvg1.free_size, fake_dpvs[0]['free_size'])
        path = '/dvgs/{0}/reduce'.format(dvg_name)
        headers = {
            'Content-Type': 'application/json',
        }
        raw_data = {
            'dpv_name': dpv_name,
        }
        data = json.dumps(raw_data)
        resp = self.client.put(path, headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dvg2 = self.dbm.dvg_get(dvg_name)
        self.assertEqual(dvg2.total_size, 0)
        self.assertEqual(dvg2.free_size, 0)
