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
    Snapshot, Group, Leg, OwnerBasedTransaction, Counter, TargetHost
from dlvm.api_server.handler import div_round_up


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
                data_size=10*1024*1024*1024,
                partition_count=2,
                status='detached',
                timestamp=datetime.datetime.utcnow(),
                active_snap_name='dlv0/base',
                dvg_name='dvg0',
            )
            dlv1 = DistributeLogicalVolume(
                dlv_name='dlv1',
                dlv_size=10*1024*1024*1024,
                data_size=10*1024*1024*1024,
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

    def _prepare_dlv(self, status, thost_name=None):
        dlv_name = 'dlv0'
        dlv_size = 200*1024*1024*1024
        init_size = dlv_size
        partition_count = 2
        dvg_name = 'dvg0'
        snap_name = 'dlv0/base'
        with self.app.app_context():
            dlv = DistributeLogicalVolume(
                dlv_name=dlv_name,
                dlv_size=dlv_size,
                data_size=dlv_size,
                partition_count=partition_count,
                active_snap_name=snap_name,
                status=status,
                timestamp=datetime.datetime.utcnow(),
                dvg_name=dvg_name,
                thost_name=thost_name,
            )
            db.session.add(dlv)
            snapshot = Snapshot(
                snap_name=snap_name,
                thin_id=0,
                ori_thin_id=0,
                status='available',
                timestamp=datetime.datetime.utcnow(),
                dlv_name=dlv_name,
            )
            db.session.add(snapshot)
            group = Group(
                group_id=uuid.uuid4().hex,
                idx=0,
                group_size=1024*1024,
                dlv_name=dlv_name,
            )
            db.session.add(group)
            group_size = init_size
            group = Group(
                group_id=uuid.uuid4().hex,
                idx=1,
                group_size=group_size,
                dlv_name=dlv_name,
            )
            db.session.add(group)
            leg_size = div_round_up(group_size, partition_count)
            legs_per_group = 2 * partition_count
            for i in xrange(legs_per_group):
                dpv_name = 'dpv%d' % i
                dpv = DistributePhysicalVolume \
                    .query \
                    .filter_by(dpv_name=dpv_name) \
                    .one()
                dpv.free_size -= leg_size
                db.session.add(dpv)

                dvg = DistributeVolumeGroup \
                    .query \
                    .filter_by(dvg_name='dvg0') \
                    .one()
                dvg.free_size -= leg_size
                db.session.add(dvg)

                leg = Leg(
                    leg_id=uuid.uuid4().hex,
                    idx=i,
                    leg_size=leg_size,
                    group=group,
                    dpv_name=dpv_name,
                )
                db.session.add(leg)

            db.session.commit()

    def _prepare_thost(self, thost_name):
        with self.app.app_context():
            thost = TargetHost(
                thost_name=thost_name,
                status='available',
                timestamp=datetime.datetime.utcnow(),
            )
            db.session.add(thost)
            db.session.commit()

    def _prepare_obt(self, t_id, t_owner, t_stage):
        with self.app.app_context():
            counter = Counter()
            db.session.add(counter)
            obt = OwnerBasedTransaction(
                t_id=t_id,
                t_owner=t_owner,
                t_stage=t_stage,
                timestamp=datetime.datetime.utcnow(),
                counter=counter,
            )
            db.session.add(obt)
            db.session.commit()

    def _prepare_snapshot(
            self, dlv_name, snap_name, thin_id, ori_thin_id):
        with self.app.app_context():
            snap_name = '{dlv_name}/{snap_name}'.format(
                dlv_name=dlv_name, snap_name=snap_name)
            snapshot = Snapshot(
                snap_name=snap_name,
                thin_id=thin_id,
                ori_thin_id=ori_thin_id,
                status='available',
                timestamp=datetime.datetime.utcnow(),
                dlv_name=dlv_name,
            )
            db.session.add(snapshot)
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
        self._prepare_obt(t_id, t_owner, t_stage)
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

    @patch('dlvm.api_server.dlv.WrapperRpcClient')
    def test_dlv_delete(self, WrapperRpcClient):
        client_mock = Mock()
        WrapperRpcClient.return_value = client_mock
        self._prepare_dpvs_and_dvg()
        self._prepare_dlv('detached')
        t_id = 't0'
        t_owner = 't_owner'
        t_stage = 0
        self._prepare_obt(t_id, t_owner, t_stage)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            't_id': t_id,
            't_owner': t_owner,
            't_stage': t_stage,
        }
        data = json.dumps(data)
        resp = self.client.delete('/dlvs/dlv0', headers=headers, data=data)
        print(resp.data)
        self.assertEqual(resp.status_code, 200)

    @patch('dlvm.api_server.dlv.WrapperRpcClient')
    def test_dlv_attach(self, WrapperRpcClient):
        client_mock = Mock()
        WrapperRpcClient.return_value = client_mock
        self._prepare_dpvs_and_dvg()
        self._prepare_thost('thost0')
        self._prepare_dlv('detached')
        t_id = 't0'
        t_owner = 't_owner'
        t_stage = 0
        self._prepare_obt(t_id, t_owner, t_stage)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'attach',
            'thost_name': 'thost0',
            't_id': t_id,
            't_owner': t_owner,
            't_stage': t_stage,
        }
        data = json.dumps(data)
        resp = self.client.put('/dlvs/dlv0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    @patch('dlvm.api_server.dlv.WrapperRpcClient')
    def test_dlv_detach(self, WrapperRpcClient):
        client_mock = Mock()
        WrapperRpcClient.return_value = client_mock
        self._prepare_dpvs_and_dvg()
        self._prepare_thost('thost0')
        self._prepare_dlv('attached', 'thost0')
        t_id = 't0'
        t_owner = 't_owner'
        t_stage = 0
        self._prepare_obt(t_id, t_owner, t_stage)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'detach',
            't_id': t_id,
            't_owner': t_owner,
            't_stage': t_stage,
        }
        data = json.dumps(data)
        resp = self.client.put('/dlvs/dlv0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    @patch('dlvm.api_server.dlv.WrapperRpcClient')
    def test_dlv_set_active(self, WrapperRpcClient):
        client_mock = Mock()
        WrapperRpcClient.return_value = client_mock
        self._prepare_dpvs_and_dvg()
        self._prepare_dlv('detached')
        t_id = 't0'
        t_owner = 't_owner'
        t_stage = 0
        self._prepare_obt(t_id, t_owner, t_stage)
        self._prepare_snapshot('dlv0', 'snap1', 0, 1)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'action': 'set_active',
            'snap_name': 'snap1',
            't_id': t_id,
            't_owner': t_owner,
            't_stage': t_stage,
        }
        data = json.dumps(data)
        resp = self.client.put('/dlvs/dlv0', headers=headers, data=data)
        self.assertEqual(resp.status_code, 200)

    @patch('dlvm.api_server.dlv.WrapperRpcClient')
    def test_dlv_get(self, WrapperRpcClient):
        client_mock = Mock()
        WrapperRpcClient.return_value = client_mock
        self._prepare_dpvs_and_dvg()
        self._prepare_dlv('detached')
        resp = self.client.get('/dlvs/dlv0')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        body = data['body']
        self.assertEqual(body['dlv_name'], 'dlv0')
