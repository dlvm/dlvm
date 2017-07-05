#!/usr/bin/env python

import os
import json
import datetime
import unittest
from mock import patch
from sqlalchemy.orm.exc import NoResultFound
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

    def test_dvg_get(self):
        with self.app.app_context():
            dvg = DistributeVolumeGroup(
                dvg_name='dvg0',
                total_size=1,
                free_size=3,
            )
            db.session.add(dvg)
            db.session.commit()
        resp = self.client.get('/dvgs/dvg0')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual('dvg0', data['body']['dvg_name'])
        self.assertEqual(1, data['body']['total_size'])
        self.assertEqual(3, data['body']['free_size'])

    def test_dvg_extend(self):
        with self.app.app_context():
            dpv = DistributePhysicalVolume(
                dpv_name='dpv0',
                total_size=512*1024*1024*1024,
                free_size=512*1024*1024*1024,
                in_sync=True,
                status='available',
                timestamp=datetime.datetime.utcnow(),
            )
            dvg = DistributeVolumeGroup(
                dvg_name='dvg0',
                total_size=0,
                free_size=0,
            )
            db.session.add(dpv)
            db.session.add(dvg)
            db.session.commit()

        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'extend',
            'dpv_name': 'dpv0',
        }
        data = json.dumps(data)
        resp = self.client.put('/dvgs/dvg0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_dvg_reduce(self):
        with self.app.app_context():
            dpv = DistributePhysicalVolume(
                dpv_name='dpv0',
                total_size=0,
                free_size=0,
                in_sync=True,
                status='available',
                timestamp=datetime.datetime.utcnow(),
            )
            dvg = DistributeVolumeGroup(
                dvg_name='dvg0',
                total_size=0,
                free_size=0,
            )
            dpv.dvg_name = dvg.dvg_name
            db.session.add(dpv)
            db.session.add(dvg)
            db.session.commit()

        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'reduce',
            'dpv_name': 'dpv0',
        }
        data = json.dumps(data)
        resp = self.client.put('/dvgs/dvg0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    def test_dvg_delete(self):
        with self.app.app_context():
            dvg = DistributeVolumeGroup(
                dvg_name='dvg0',
                total_size=0,
                free_size=0,
            )
            db.session.add(dvg)
            db.session.commit()
        resp = self.client.delete('/dvgs/dvg0')
        self.assertEqual(resp.status_code, 200)
        with self.app.app_context():
            try:
                dvg = DistributeVolumeGroup \
                    .query \
                    .with_lockmode('update') \
                    .filter_by(dvg_name='dvg0') \
                    .one()
            except NoResultFound:
                deleted = True
            else:
                deleted = False
        self.assertEqual(deleted, True)
