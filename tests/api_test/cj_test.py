#!/usr/bin/env python

import unittest
import os
import json
import datetime
from mock import Mock, patch
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

fixture_src_dlv = {
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

fixture_dst_dlv = {
    'dlv_name': 'dlv1',
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

fixture_cj = {
    'cj_name': 'cj0',
    'status': 'processing',
    'timestamp': timestamp,
    'src_dlv_name': 'dlv0',
    'dst_dlv_name': 'dlv1',
    'snap_name': 'base',
}

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


class CjTest(unittest.TestCase):

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
        self.fm.dlv_create(**fixture_src_dlv)
        self.fm.dlv_create(**fixture_dst_dlv)

    def _prepare_cj(self):
        self.fm.cj_create(**fixture_cj)

    def test_cjs_get(self):
        self._prepare_dlv()
        self._prepare_cj()
        resp = self.client.get('/cjs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data['body']), 1)
        fj = data['body'][0]
        self.assertEqual(fj['cj_name'], 'cj0')
        self.assertEqual(fj['status'], 'processing')

    @patch('dlvm.api_server.cj.DpvClient')
    @patch('dlvm.api_server.cj.IhostClient')
    def test_cjs_post(self, IhostClient, DpvClient):
        self._prepare_dlv()
        self.fm.obt_create(**fixture_obt)
        self.fm.ihost_create(**fixture_ihost)
        self.fm.dlv_attach('dlv0', 'ihost0')
        self.fm.dlv_attach('dlv1', 'ihost0')
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'cj_name': 'cj0',
            'src_name': 'dlv0',
            'dst_name': 'dlv1',
            'snap_name': 'base',
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.post('/cjs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        cj = self.fm.cj_get('cj0')
        self.assertEqual(cj.status, 'processing')

    @patch('dlvm.api_server.cj.DpvClient')
    def test_fj_get(self, DpvClient):
        self._prepare_dlv()
        self._prepare_cj()
        client_mock = Mock()
        DpvClient.return_value = client_mock
        cj_mirror_status_mock = Mock()
        client_mock.cj_mirror_status = cj_mirror_status_mock
        cj_mirror_status_mock.return_value = {}
        resp = self.client.get('/cjs/cj0?with_process=true')
        self.assertEqual(resp.status_code, 200)

    @patch('dlvm.api_server.cj.DpvClient')
    def test_cj_cancel(self, DpvClient):
        self._prepare_dlv()
        self.fm.obt_create(**fixture_obt)
        self._prepare_cj()
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'cancel',
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.put('/cjs/cj0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        fj = self.fm.cj_get('cj0')
        self.assertEqual(fj.status, 'canceled')

    @patch('dlvm.api_server.cj.DpvClient')
    def test_cj_finish(self, DpvClient):
        self._prepare_dlv()
        self.fm.obt_create(**fixture_obt)
        self._prepare_cj()
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'finish',
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.put('/cjs/cj0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        fj = self.fm.cj_get('cj0')
        self.assertEqual(fj.status, 'finished')

    def test_cj_delete(self):
        self._prepare_dlv()
        self.fm.obt_create(**fixture_obt)
        self._prepare_cj()
        self.fm.cj_set_status('cj0', 'finished')
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            't_id': 't0',
            't_owner': 't_owner0',
            't_stage': 0,
        }
        data = json.dumps(data)
        resp = self.client.delete('/cjs/cj0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
