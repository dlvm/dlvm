#!/usr/bin/env python

import unittest
import os
import json
import datetime
from mock import patch
from dlvm.api_server import create_app
from dlvm.api_server.modules import db
from utils import FixtureManager

timestamp = datetime.datetime(2016, 7, 21, 23, 58, 59)

fixture_obt = {
    't_id': 't0',
    't_owner': 't_owner0',
    't_stage': 0,
    'timestamp': timestamp,
}


class ObtTest(unittest.TestCase):

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

    def test_obts_get(self):
        self.fm.obt_create(**fixture_obt)
        resp = self.client.get('/obts')
        self.assertEqual(resp.status_code, 200)

    def test_obts_post(self):
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            't_id': 't01',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.post('/obts', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_obt_preempt(self):
        self.fm.obt_create(**fixture_obt)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'preempt',
            't_owner': 't_owner0',
            'new_owner': 't_owner1',
        }
        data = json.dumps(data)
        resp = self.client.put('/obts/t0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_obt_delete(self):
        self.fm.obt_create(**fixture_obt)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            't_owner': 't_owner0',
        }
        data = json.dumps(data)
        resp = self.client.delete(
            '/obts/t0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
