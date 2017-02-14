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
    'partition_count': 1,
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

fixture_fj = {
    'fj_name': 'fj0',
    'status': 'processing',
    'timestamp': timestamp,
    'dlv_name': 'dlv0',
    'g_idx': 1,
    'l_idx': 0,
}

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


class FjTest(unittest.TestCase):

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

    def _prepare_fj(self):
        self.fm.fj_create(**fixture_fj)

    def test_fjs_get(self):
        self._prepare_dlv()
        self._prepare_fj()
        resp = self.client.get('/fjs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data['body']), 1)
        fj = data['body'][0]
        self.assertEqual(fj['fj_name'], 'fj0')
        self.assertEqual(fj['status'], 'processing')
        self.assertEqual(fj['dlv_name'], 'dlv0')

    @patch('dlvm.api_server.fj.DpvClient')
    @patch('dlvm.api_server.fj.ThostClient')
    def test_fjs_post(self, ThostClient, DpvClient):
        self._prepare_dlv()
        self.fm.obt_create(**fixture_obt)
        self.fm.thost_create(**fixture_thost)
        self.fm.dlv_attach('dlv0', 'thost0')
        headers = {
            'Content-Type': 'application/json',
        }
        ori_id = self.fm.dlv_get_leg_id('dlv0', 1, 0)
        data = {
            'fj_name': 'fj0',
            'dlv_name': 'dlv0',
            'ori_id': ori_id,
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.post('/fjs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        fj = self.fm.fj_get('fj0')
        self.assertEqual(fj.status, 'processing')
