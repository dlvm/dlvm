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

fixture_transaction = {
    't_id': 't0',
    't_owner': 't_owner0',
    't_stage': 0,
    'timestamp': timestamp,
}


class TransactionTest(unittest.TestCase):

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

    def test_transactions_get(self):
        self.fm.transaction_create(**fixture_transaction)
        resp = self.client.get('/transactions')
        self.assertEqual(resp.status_code, 200)

    def test_transactions_post(self):
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            't_id': 't01',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.post('/transactions', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_transaction_preempt(self):
        self.fm.transaction_create(**fixture_transaction)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'preempt',
            't_owner': 't_owner0',
            'new_owner': 't_owner1',
        }
        data = json.dumps(data)
        resp = self.client.put('/transactions/t0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_transaction_delete(self):
        self.fm.transaction_create(**fixture_transaction)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            't_owner': 't_owner0',
        }
        data = json.dumps(data)
        resp = self.client.delete(
            '/transactions/t0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
