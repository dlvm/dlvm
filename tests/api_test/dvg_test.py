#!/usr/bin/env python

import os
import json
import unittest
from mock import patch
from dlvm.api_server import create_app
from dlvm.api_server.modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup


class DvgTest(unittest.TestCase):

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

    def test_dvgs_get(self):
        with self.app.app_context():
            dvg = DistributeVolumeGroup(
                dvg_name='dvg0',
                total_size=0,
                free_size=0,
            )
            db.session.add(dvg)
            db.session.commit()
        resp = self.client.get('/dvgs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data['body']), 1)
        dvg0_resp = data['body'][0]
        self.assertEqual(dvg0_resp['dvg_name'], 'dvg0')

    def test_dvgs_post(self):
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'dvg_name': 'dvg0',
        }
        data = json.dumps(data)
        resp = self.client.post('/dvgs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        with self.app.app_context():
            dvg = DistributeVolumeGroup \
                .query \
                .filter_by(dvg_name='dvg0') \
                .one()
        self.assertEqual(dvg.dvg_name, 'dvg0')
        self.assertEqual(dvg.total_size, 0)
