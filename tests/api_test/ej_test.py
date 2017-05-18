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

fixture_dpvs = [
    {
        'dpv_name': 'dpv0',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
        'timestamp': timestamp,
    },
    {
        'dpv_name': 'dpv1',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
        'timestamp': timestamp,
    },
    {
        'dpv_name': 'dpv2',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
        'timestamp': timestamp,
    },
]

fixture_dvg = {
    'dvg_name': 'dvg0',
}

fixture_dlv = {
    'dlv_name': 'dlv0',
    'dlv_size': 16*1024*1024*1024,
    'data_size': 8*1024*1024*1024,
    'stripe_number': 1,
    'status': 'detached',
    'timestamp': timestamp,
    'dvg_name': 'dvg0',
    'igroups': [
        {
            'group_size': 16*1024*1024,
        },
        {
            'group_size': 8*1024*1024*1024,
        },
    ],
}

fixture_ej = {
    'ej_name': 'ej0',
    'status': 'created',
    'timestamp': timestamp,
    'dlv_name': 'dlv0',
    'ej_size': 8*1024*1024*1024,
}

fixture_ihost = {
    'ihost_name': 'ihost0',
    'status': 'available',
    'timestamp': timestamp,
}

fixture_obt = {
    't_id': 't0',
    't_owner': 't_owner0',
    't_stage': 0,
    'timestamp': timestamp,
}


class EjTest(unittest.TestCase):

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

    def _prepare_dlv(self):
        for dpv in fixture_dpvs:
            self.fm.dpv_create(**dpv)
        self.fm.dvg_create(**fixture_dvg)
        for dpv in fixture_dpvs:
            self.fm.dvg_extend('dvg0', dpv['dpv_name'])
        self.fm.dlv_create(**fixture_dlv)

    def _prepare_ej(self):
        self.fm.ej_create(**fixture_ej)

    def test_ejs_get(self):
        self._prepare_dlv()
        self._prepare_ej()
        resp = self.client.get('/ejs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data['body']), 1)
        ej = data['body'][0]
        self.assertEqual(ej['ej_name'], 'ej0')
        self.assertEqual(ej['status'], 'created')
        self.assertEqual(ej['dlv_name'], 'dlv0')

    @patch('dlvm.api_server.allocator.DpvClient')
    @patch('dlvm.api_server.ej.IhostClient')
    def test_ejs_post(self, IhostClient, DpvClient):
        self._prepare_dlv()
        self.fm.obt_create(**fixture_obt)
        self.fm.ihost_create(**fixture_ihost)
        self.fm.dlv_attach('dlv0', 'ihost0')
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'ej_name': 'ej0',
            'dlv_name': 'dlv0',
            'ej_size': 8*1024*1024*1024,
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.post('/ejs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        ej = self.fm.ej_get('ej0')
        self.assertEqual(ej.status, 'created')

    def test_ej_get(self):
        self._prepare_dlv()
        self._prepare_ej()
        resp = self.client.get('/ejs/ej0')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        ej = data['body']
        self.assertEqual(ej['ej_name'], 'ej0')
        self.assertEqual(ej['group']['group_size'], 8*1024*1024*1024)
