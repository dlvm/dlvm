#!/usr/bin/env python

import os
import json
import unittest
from unittest.mock import patch
from dlvm.api_server.routing import create_app
from dlvm.utils.modules import db
from ..utils import FixtureManager

fixture_dpv = {
    'dpv_name': 'dpv0',
    'total_size': 512*1024*1024*1024,
    'free_size': 512*1024*1024*1024,
    'status': 'available',
}

fixture_dvg = {
    'dvg_name': 'dvg0',
}


class DvgTest(unittest.TestCase):

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

    def test_dvgs_get(self):
        self.fm.dvg_create(**fixture_dvg)
        resp = self.client.get('/dvgs')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data.decode('utf-8'))
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(len(data['body']), 1)

    def test_dvgs_post(self):
        dvg_name = 'dvg0'
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'dvg_name': dvg_name,
        }
        data = json.dumps(data)
        resp = self.client.post('/dvgs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dvg = self.fm.dvg_get(dvg_name)
        self.assertTrue(dvg is not None)

    def test_dvg_get(self):
        self.fm.dpv_create(**fixture_dpv)
        self.fm.dvg_create(**fixture_dvg)
        dvg_name = fixture_dvg['dvg_name']
        dpv_name = fixture_dpv['dpv_name']
        total_size = fixture_dpv['total_size']
        free_size = fixture_dpv['free_size']
        self.fm.dpv_set(dpv_name, 'dvg_name', dvg_name)
        self.fm.dvg_set(dvg_name, 'total_size', total_size)
        self.fm.dvg_set(dvg_name, 'free_size', free_size)
        path = '/dvgs/{dvg_name}'.format(
            dvg_name=dvg_name)
        resp = self.client.get(path)
        self.assertEqual(resp.status_code, 200)

    def test_dvg_delete(self):
        self.fm.dvg_create(**fixture_dvg)
        dvg_name = fixture_dvg['dvg_name']
        path = '/dvgs/{dvg_name}'.format(
            dvg_name=dvg_name)
        resp = self.client.delete(path)
        self.assertEqual(resp.status_code, 200)

    def test_dvg_extend(self):
        self.fm.dpv_create(**fixture_dpv)
        self.fm.dvg_create(**fixture_dvg)
        dvg_name = fixture_dvg['dvg_name']
        dpv_name = fixture_dpv['dpv_name']
        total_size = fixture_dpv['total_size']
        free_size = fixture_dpv['free_size']
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'dpv_name': dpv_name,
        }
        data = json.dumps(data)
        path = '/dvgs/{dvg_name}/extend'.format(
            dvg_name=dvg_name)
        resp = self.client.put(path, headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dvg = self.fm.dvg_get(dvg_name)
        self.assertEqual(dvg.total_size, total_size)
        self.assertEqual(dvg.free_size, free_size)

    def test_dvg_reduce(self):
        self.fm.dpv_create(**fixture_dpv)
        self.fm.dvg_create(**fixture_dvg)
        dvg_name = fixture_dvg['dvg_name']
        dpv_name = fixture_dpv['dpv_name']
        total_size = fixture_dpv['total_size']
        free_size = fixture_dpv['free_size']
        self.fm.dpv_set(dpv_name, 'dvg_name', dvg_name)
        self.fm.dvg_set(dvg_name, 'total_size', total_size)
        self.fm.dvg_set(dvg_name, 'free_size', free_size)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'dpv_name': dpv_name,
        }
        data = json.dumps(data)
        path = '/dvgs/{dvg_name}/reduce'.format(
            dvg_name=dvg_name)
        dvg = self.fm.dvg_get(dvg_name)
        self.assertEqual(dvg.total_size, total_size)
        self.assertEqual(dvg.free_size, free_size)
        resp = self.client.put(path, headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dvg = self.fm.dvg_get(dvg_name)
        self.assertEqual(dvg.total_size, 0)
        self.assertEqual(dvg.free_size, 0)
