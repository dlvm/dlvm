#!/usr/bin/env python

import os
import datetime
import json
import unittest
from mock import Mock, patch
from dlvm.api_server import create_app
from dlvm.api_server.modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume, \
    Snapshot, Group, Leg, Transaction, Counter


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

    def _prepare_dpvs_and_dvg(self):
        total_size = 512 * 1024 * 1024 * 1024
        free_size = total_size
        with self.app.app_context():
            for i in xrange(4):
                dpv = DistributePhysicalVolume(
                    dpv_name='dpv%s' % i,
                    total_size=total_size,
                    free_size=free_size,
                    status='available',
                    timestamp=datetime.datetime.utcnow(),
                    dvg_name='dvg0',
                )
                db.session.add(dpv)
            dvg = DistributeVolumeGroup(
                dvg_name='dvg0',
                total_size=4*total_size,
                free_size=4*free_size,
            )
            db.session.add(dvg)
            db.session.commit()

    def _prepare_transaction(self, t_id, t_owner, t_stage):
        with self.app.app_context():
            counter = Counter()
            db.session.add(counter)
            t = Transaction(
                t_id=t_id,
                t_owner=t_owner,
                t_stage=t_stage,
                timestamp=datetime.datetime.utcnow(),
                counter=counter,
            )
            db.session.add(t)
            db.session.commit()

    @patch('dlvm.api_server.dlv.WrapperRpcClient')
    def test_dlvs_create_new(self, WrapperRpcClient):
        client_mock = Mock()
        WrapperRpcClient.return_value = client_mock
        leg_create_mock = Mock()
        client_mock.leg_create = leg_create_mock
        self._prepare_dpvs_and_dvg()
        t_id = 't0'
        t_owner = 't_owner'
        t_stage = 0
        self._prepare_transaction(t_id, t_owner, t_stage)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'dlv_name': 'dlv0',
            'dlv_size': 200*1024*1024*1024,
            'init_size': 100*1024*1024*1024,
            'partition_count': 2,
            'dvg_name': 'dvg0',
            't_id': t_id,
            't_owner': t_owner,
            't_stage': t_stage,
        }
        data = json.dumps(data)
        resp = self.client.post('/dlvs', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)
        with self.app.app_context():
            dlv = DistributeLogicalVolume \
                .query \
                .filter_by(dlv_name='dlv0') \
                .one()
        self.assertEqual(dlv.status, 'detached')
        self.assertEqual(leg_create_mock.call_count, 6)
