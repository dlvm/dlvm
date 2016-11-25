#!/usr/bin/env python

import os
import datetime
import json
import unittest
from mock import patch
from dlvm.api_server import create_app
from dlvm.api_server.modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume, \
    Snapshot, Group, Leg


class DlvTest(unittest.TestCase):

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

    def tearDown(self):
        if os.path.isfile(self.db_path):
            os.remove(self.db_path)

    def _insert_dlvs(self):
        with self.app.app_context():
            dvg = DistributeVolumeGroup(
                dvg_name='dvg0',
                total_size=0,
                free_size=0,
            )
            db.session.add(dvg)
            db.session.commit()

            dlv0 = DistributeLogicalVolume(
                dlv_name='dlv0',
                dlv_size=10*1024*1024*1024,
                partition_count=2,
                status='detached',
                timestamp=datetime.datetime.utcnow(),
                active_snap_name='dlv0/base',
                dvg_name='dvg0',
            )
            dlv1 = DistributeLogicalVolume(
                dlv_name='dlv1',
                dlv_size=10*1024*1024*1024,
                partition_count=2,
                status='detached',
                timestamp=datetime.datetime.utcnow(),
                active_snap_name='dlv1/base',
                dvg_name='dvg0',
            )
            db.session.add(dlv0)
            db.session.add(dlv1)
            db.session.commit()

    def test_dlvs_get(self):
        self._insert_dlvs()
        resp = self.client.get('/dlvs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data['body']), 2)
        dlv0_resp = data['body'][0]
        self.assertTrue(dlv0_resp['dlv_name'] in ('dlv0', 'dlv1'))
        dlv1_resp = data['body'][1]
        self.assertTrue(dlv1_resp['dlv_name'] in ('dlv0', 'dlv1'))
