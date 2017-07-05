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
        'in_sync': True,
        'status': 'available',
        'timestamp': timestamp,
    },
    {
        'dpv_name': 'dpv1',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'in_sync': True,
        'status': 'available',
        'timestamp': timestamp,
    },
    {
        'dpv_name': 'dpv2',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'in_sync': True,
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

fixture_snapshots = [
    {
        'snap_name': 'snap1',
        'timestamp': timestamp,
        'thin_id': 1,
        'ori_thin_id': 0,
        'status': 'available',
        'dlv_name': 'dlv0',
    },
]

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


class SnapshotTest(unittest.TestCase):

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

    def _prepare_obt(self):
        self.fm.obt_create(**fixture_obt)

    def _prepare_ihost(self):
        self.fm.ihost_create(**fixture_ihost)

    def _prepare_snapshots(self):
        for snapshot in fixture_snapshots:
            self.fm.snapshot_create(**snapshot)

    def test_snapshots_get(self):
        self._prepare_dlv()
        self._prepare_snapshots()
        resp = self.client.get('/dlvs/dlv0/snaps')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data['body']), 2)
        snap0 = data['body'][0]
        self.assertTrue(snap0['snap_name'] in ('base, snap1'))
        snap1 = data['body'][1]
        self.assertTrue(snap1['snap_name'] in ('base, snap1'))

    @patch('dlvm.api_server.snapshot.WrapperRpcClient')
    def test_snapshots_post(self, WrapperRpcClient):
        self._prepare_dlv()
        self._prepare_obt()
        self._prepare_ihost()
        self.fm.dlv_attach('dlv0', 'ihost0')
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'snap_pairs': 'snap1:base',
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.post('/dlvs/dlv0/snaps', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_snapshot_get(self):
        self._prepare_dlv()
        self._prepare_snapshots()
        resp = self.client.get('/dlvs/dlv0/snaps/snap1')
        self.assertEqual(resp.status_code, 200)

    @patch('dlvm.api_server.snapshot.WrapperRpcClient')
    def test_snapshot_delete(self, WrapperRpcClient):
        self._prepare_dlv()
        self._prepare_snapshots()
        self._prepare_obt()
        self._prepare_ihost()
        self.fm.dlv_attach('dlv0', 'ihost0')
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.delete(
            '/dlvs/dlv0/snaps/snap1', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
