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

fixture_thost = {
    'thost_name': 'thost0',
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

    def test_thosts_get(self):
        self.fm.thost_create(**fixture_thost)
        resp = self.client.get('/thosts')
        self.assertEqual(resp.status_code, 200)

    def test_thosts_post(self):
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'thost_name': 'thost0',
        }
        data = json.dumps(data)
        resp = self.client.post('/thosts', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_thost_delete(self):
        self.fm.thost_create(**fixture_thost)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'thost_name': 'thost0',
        }
        data = json.dumps(data)
        resp = self.client.delete('/thosts/thost0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_thost_unavailable(self):
        self.fm.thost_create(**fixture_thost)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'set_unavailable',
        }
        data = json.dumps(data)
        resp = self.client.put('/thosts/thost0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        thost = self.fm.thost_get('thost0')
        self.assertEqual(thost.status, 'unavailable')

    @patch('dlvm.api_server.thost.ThostClient')
    def test_thost_available(self, ThostClient):
        self.fm.thost_create(**fixture_thost)
        self.fm.thost_set_status('thost0', 'unavailable')
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
        resp = self.client.put('/thosts/thost0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        thost = self.fm.thost_get('thost0')
        self.assertEqual(thost.status, 'available')

    def test_thost_get(self):
        self.fm.thost_create(**fixture_thost)
        resp = self.client.get('/thosts/thost0')
        self.assertEqual(resp.status_code, 200)
