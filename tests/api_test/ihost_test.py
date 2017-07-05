#!/usr/bin/env python

import os
import datetime
import json
import unittest
from mock import patch
from dlvm.api_server import create_app
from dlvm.api_server.modules import db
from utils import FixtureManager

timestamp = datetime.datetime(2016, 7, 21, 23, 58, 59)

fixture_ihost = {
    'ihost_name': 'ihost0',
    'in_sync': True,
    'status': 'available',
    'timestamp': timestamp,
}

fixture_obt = {
    't_id': 't0',
    't_owner': 't_owner0',
    't_stage': 0,
    'timestamp': timestamp,
}


class DpvTest(unittest.TestCase):

    db_path = '/tmp/dlvm_test.db'
    db_uri = 'sqlite:////tmp/dlvm_test.db'

    @patch('dlvm.api_server.loginit')
    @patch('dlvm.api_server.conf')
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

    def test_ihosts_get(self):
        self.fm.ihost_create(**fixture_ihost)
        resp = self.client.get('/ihosts')
        self.assertEqual(resp.status_code, 200)

    def test_ihosts_post(self):
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'ihost_name': 'ihost0',
        }
        data = json.dumps(data)
        resp = self.client.post('/ihosts', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_ihost_delete(self):
        self.fm.ihost_create(**fixture_ihost)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'ihost_name': 'ihost0',
        }
        data = json.dumps(data)
        resp = self.client.delete('/ihosts/ihost0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_ihost_unavailable(self):
        self.fm.ihost_create(**fixture_ihost)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'set_unavailable',
        }
        data = json.dumps(data)
        resp = self.client.put('/ihosts/ihost0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        ihost = self.fm.ihost_get('ihost0')
        self.assertEqual(ihost.status, 'unavailable')

    @patch('dlvm.api_server.ihost.IhostClient')
    def test_ihost_available(self, IhostClient):
        self.fm.ihost_create(**fixture_ihost)
        self.fm.ihost_set_status('ihost0', 'unavailable')
        self.fm.obt_create(**fixture_obt)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'set_available',
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.put('/ihosts/ihost0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        ihost = self.fm.ihost_get('ihost0')
        self.assertEqual(ihost.status, 'available')

    def test_ihost_get(self):
        self.fm.ihost_create(**fixture_ihost)
        resp = self.client.get('/ihosts/ihost0')
        self.assertEqual(resp.status_code, 200)
