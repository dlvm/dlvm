#!/usr/bin/env python

import os
import json
import unittest
from mock import Mock, patch
from dlvm.api_server.routing import create_app
from dlvm.utils.modules import db, \
    DistributePhysicalVolume
from ..utils import FixtureManager

fixture_dpvs = [
    {
        'dpv_name': 'dpv0',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
    },
    {
        'dpv_name': 'dpv1',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
    },
    {
        'dpv_name': 'dpv2',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
        'status': 'available',
    },
]


class DpvTest(unittest.TestCase):

    db_path = '/tmp/dlvm_test.db'
    db_uri = 'sqlite:////tmp/dlvm_test.db'

    @patch('dlvm.api_server.routing.loginit')
    @patch('dlvm.api_server.routing.conf')
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

    def test_dpvs_get(self):
        for dpv in fixture_dpvs:
            self.fm.dpv_create(**dpv)
        resp = self.client.get('/dpvs')
        self.assertEqual(resp.status_code, 200)
        print(resp.data)
        data = json.loads(resp.data.decode('utf-8'))
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(len(data['body']), len(fixture_dpvs))
