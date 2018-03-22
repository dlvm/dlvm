#!/usr/bin/env python

import os
import json
import unittest
from mock import patch
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

fixture_dvg = {
    'dvg_name': 'dvg0',
}


class DvgTest(unittest.TestCase):

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

    def test_dvgs_get(self):
        self.fm.dvg_create(**fixture_dvg)
        resp = self.client.get('/dvgs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data.decode('utf-8'))
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(len(data['body']), 1)

    def test_dvgs_post(self):
        dvg_name = 'dvg0'
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'dvg_name': dvg_name,
        }
        data = json.dumps(data)
        resp = self.client.post('/dvgs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dvg = self.fm.dvg_get(dvg_name)
        self.assertTrue(dvg is not None)
