#!/usr/bin/env python

import os
import datetime
import json
import unittest
from mock import patch
from dlvm.api_server import create_app
from dlvm.api_server.modules import db, DistributePhysicalVolume

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

    def tearDown(self):
        if os.path.isfile(self.db_path):
            os.remove(self.db_path)

    def _insert_dpvs(self):
        with self.app.app_context():
            dpv0 = DistributePhysicalVolume(
                dpv_name='dpv0',
                total_size=512*1024*1024*1024,
                free_size=384*1024*1024*1024,
                status='available',
                timestamp=datetime.datetime.utcnow(),
            )
            dpv1 = DistributePhysicalVolume(
                dpv_name='dpv1',
                total_size=0,
                free_size=0,
                status='unavailable',
                timestamp=datetime.datetime.utcnow(),
            )
            db.session.add(dpv0)
            db.session.add(dpv1)
            db.session.commit()

    def test_dpvs_get(self):
        self._insert_dpvs()
        resp = self.client.get('/dpvs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data['body']), 2)
        dpv0_resp = data['body'][0]
        self.assertTrue(dpv0_resp['dpv_name'] in ('dpv0', 'dpv1'))
        dpv1_resp = data['body'][1]
        self.assertTrue(dpv1_resp['dpv_name'] in ('dpv0', 'dpv1'))
