#!/usr/bin/env python

import os
import json
import unittest
from mock import Mock, patch
from dlvm.api_server.routing import create_app
from dlvm.utils.modules import db
from ..utils import FixtureManager

fixture_dpvs = [
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

    db_path = '/tmp/dlvm_test.db'
    db_uri = 'sqlite:////tmp/dlvm_test.db'

    @patch('dlvm.api_server.routing.loginit')
    @patch('dlvm.api_server.routing.conf')
    def setUp(self, conf, loginit):
        conf.db_uri = self.db_uri
        app = create_app()
        app.config['TESTING'] = True
        with app.app_context():
            db.create_all()
        self.app = app
        self.client = app.test_client()
        self.fm = FixtureManager(app)

    def tearDown(self):
        if os.path.isfile(self.db_path):
            os.remove(self.db_path)

    def test_dpvs_get(self):
        for dpv in fixture_dpvs:
            self.fm.dpv_create(**dpv)
        resp = self.client.get('/dpvs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data.decode('utf-8'))
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(len(data['body']), len(fixture_dpvs))

    @patch('dlvm.api_server.dpv.DpvClient')
    def test_dpvs_post(self, DpvClient):
        dpv_name = 'dpv0'
        total_size = 512*1024*1024*1024
        free_size = 512*1024*1024*1024
        client_mock = Mock()
        DpvClient.return_value = client_mock
        get_size_mock = Mock()
        client_mock.get_size = get_size_mock
        get_size_ret_mock = Mock()
        get_size_mock.return_value = get_size_ret_mock
        get_size_ret_mock.value = {
            'total_size': total_size,
            'free_size': free_size,
        }
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'dpv_name': dpv_name,
        }
        data = json.dumps(data)
        resp = self.client.post('/dpvs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dpv = self.fm.dpv_get(dpv_name)
        self.assertEqual(dpv.status, 'available')
        self.assertEqual(dpv.total_size, total_size)
        self.assertEqual(get_size_mock.call_count, 1)

    def test_dpv_get(self):
        dpv = fixture_dpvs[0]
        self.fm.dpv_create(**dpv)
        dpv_name = dpv['dpv_name']
        total_size = dpv['total_size']
        path = '/dpvs/{dpv_name}'.format(
            dpv_name=dpv_name)
        resp = self.client.get(path)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data.decode('utf-8'))
        self.assertEqual(data['message'], 'succeed')
        body = data['body']
        self.assertEqual(body['dpv_name'], dpv_name)
        self.assertEqual(body['total_size'], total_size)

    def test_dpv_delete(self):
        dpv = fixture_dpvs[0]
        self.fm.dpv_create(**dpv)
        dpv_name = dpv['dpv_name']
        path = '/dpvs/{dpv_name}'.format(
            dpv_name=dpv_name)
        resp = self.client.delete(path)
        self.assertEqual(resp.status_code, 200)
        dpv = self.fm.dpv_get(dpv_name)
        self.assertEqual(dpv, None)

    @patch('dlvm.api_server.dpv.DpvClient')
    def test_dpv_resize(self, DpvClient):
        dpv = fixture_dpvs[0]
        self.fm.dpv_create(**dpv)
        dpv_name = dpv['dpv_name']
        total_size = dpv['total_size']
        free_size = dpv['free_size']
        delta_size = 128*1024*1024*1024
        client_mock = Mock()
        DpvClient.return_value = client_mock
        get_size_mock = Mock()
        client_mock.get_size = get_size_mock
        get_size_ret_mock = Mock()
        get_size_mock.return_value = get_size_ret_mock
        get_size_ret_mock.value = {
            'total_size': total_size+delta_size,
            'free_size': free_size+delta_size,
        }
        path = '/dpvs/{dpv_name}/resize'.format(
            dpv_name=dpv_name)
        resp = self.client.put(path)
        self.assertEqual(resp.status_code, 200)
        dpv_obj = self.fm.dpv_get(dpv_name)
        self.assertEqual(dpv_obj.total_size, total_size+delta_size)
        self.assertEqual(dpv_obj.free_size, free_size+delta_size)
