#!/usr/bin/env python

import os
import datetime
import json
import uuid
import unittest
from mock import Mock, patch
from dlvm.api_server import create_app
from dlvm.api_server.modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume, \
    Snapshot, Group, Leg
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
        self.fm = FixtureManager(app)

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

    @patch('dlvm.api_server.dpv.WrapperRpcClient')
    def test_dpvs_post(self, WrapperRpcClient):
        client_mock = Mock()
        WrapperRpcClient.return_value = client_mock
        get_dpv_info_mock = Mock()
        client_mock.get_dpv_info = get_dpv_info_mock
        get_dpv_info_mock.return_value = {
            'total_size': 512*1024*1024*1024,
            'free_size': 512*1024*1024*1024,
        }
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'dpv_name': 'dpv0',
        }
        data = json.dumps(data)
        resp = self.client.post('/dpvs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        with self.app.app_context():
            dpv = DistributePhysicalVolume \
                .query \
                .filter_by(dpv_name='dpv0') \
                .one()
        self.assertEqual(dpv.status, 'available')
        self.assertEqual(dpv.total_size, 512*1024*1024*1024)
        self.assertEqual(get_dpv_info_mock.call_count, 1)

    def _insert_dpv(self):
        with self.app.app_context():
            dpv = DistributePhysicalVolume(
                dpv_name='dpv0',
                total_size=512*1024*1024*1024,
                free_size=512*1024*1024*1024,
                status='available',
                timestamp=datetime.datetime.utcnow(),
            )
            db.session.add(dpv)
            db.session.commit()

            dvg = DistributeVolumeGroup(
                dvg_name='dvg0',
                total_size=0,
                free_size=0,
            )
            db.session.add(dvg)
            db.session.commit()

            dpv.dvg_name = dvg.dvg_name
            dvg.total_size = 512*1024*1024*1024
            dvg.free_size = 512*1024*1024*1024
            db.session.add(dpv)
            db.session.add(dvg)
            db.session.commit()

            dlv_name = 'dlv0'
            snap_name = '%s/base' % dlv_name
            dlv = DistributeLogicalVolume(
                dlv_name=dlv_name,
                dlv_size=64*1024*1024*1024,
                data_size=64*1024*1024*1024,
                partition_count=2,
                status='detached',
                timestamp=datetime.datetime.utcnow(),
                dvg_name=dvg.dvg_name,
                active_snap_name=snap_name,
            )
            snapshot = Snapshot(
                snap_name=snap_name,
                thin_id=0,
                ori_thin_id=0,
                status='available',
                timestamp=datetime.datetime.utcnow(),
                dlv=dlv,
            )
            db.session.add(dlv)
            db.session.add(snapshot)
            db.session.commit()
            group = Group(
                group_id=uuid.uuid4().hex,
                idx=0,
                group_size=64*1024*1024*1024,
                dlv_name=dlv_name,
            )
            db.session.add(group)
            leg = Leg(
                leg_id=uuid.uuid4().hex,
                idx=0,
                group=group,
                leg_size=64*1024*1024*1024,
                dpv=dpv
            )
            db.session.add(leg)
            db.session.commit()

            db.session.add(dvg)
            db.session.add(dpv)
            db.session.add(leg)
            db.session.commit()

    def test_dpv_get(self):
        self._insert_dpv()
        resp = self.client.get('/dpvs/dpv0')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        dpv = data['body']
        self.assertEqual(dpv['dpv_name'], 'dpv0')
        self.assertEqual(dpv['dvg_name'], 'dvg0')
        self.assertEqual(len(dpv['legs']), 1)
        leg = dpv['legs'][0]
        self.assertEqual(leg['idx'], 0)
        group = leg['group']
        self.assertEqual(group['idx'], 0)

    def test_dpv_delete(self):
        self._insert_dpvs()
        resp = self.client.delete('/dpvs/dpv0')
        self.assertEqual(resp.status_code, 200)

    def test_dpv_unavailable(self):
        for dpv in fixture_dpvs:
            self.fm.dpv_create(**dpv)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'set_unavailable',
        }
        data = json.dumps(data)
        resp = self.client.put('/dpvs/dpv0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        dpv = self.fm.dpv_get('dpv0')
        self.assertEqual(dpv.status, 'unavailable')
