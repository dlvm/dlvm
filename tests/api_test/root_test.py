#!/usr/bin/env python

import os
import unittest
from mock import patch
from dlvm.api_server.routing import create_app
from dlvm.api_server.modules import db


class RootTest(unittest.TestCase):

    db_path = '/tmp/dlvm_test.db'
    # db_uri = 'sqlite:////tmp/dlvm_test.db'
    db_uri = 'sqlite:///' + db_path

    @patch('dlvm.api_server.routing.loginit')
    @patch('dlvm.api_server.routing.conf')
    def setUp(self, conf, loginit):
        conf.db_uri = self.db_uri
        app = create_app()
        app.config['TESTING'] = True
        with app.app_context():
            db.create_all()
        self.app = app.test_client()

    def tearDown(self):
        if os.path.isfile(self.db_path):
            os.remove(self.db_path)

    def test_root_get(self):
        resp = self.app.get('/')
        self.assertEqual(resp.status_code, 200)
