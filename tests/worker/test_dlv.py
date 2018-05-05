import unittest
from unittest.mock import patch
import uuid
from datetime import datetime

from dlvm.common.configure import cfg
from dlvm.common.modules import LockType, DlvStatus
from dlvm.wrapper.state_machine import sm_handler, StepType, \
    StateMachineContextSchema

from tests.utils import DataBaseManager


fake_dpvs = [
    {
        'dpv_name': 'dpv0',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
    },
    {
        'dpv_name': 'dpv1',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
    },
    {
        'dpv_name': 'dpv2',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
    },
    {
        'dpv_name': 'dpv3',
        'total_size': 512*1024*1024*1024,
        'free_size': 512*1024*1024*1024,
    },
]

fake_dvg = {'dvg_name': 'dvg0'}

fake_dlv = {
    'dlv_name': 'dlv0',
    'dlv_size': 100*1024*1024*1024,
    'init_size': 50*1024*1024*1024,
    'stripe_number': 1,
    'bm_ignore': False,
    'dvg_name': 'dvg0',
    'groups': [{
        'group_idx': 0,
        'group_size': 20*1024*1024,
        'legs': [{
            'leg_idx': 0,
            'leg_size': 20*1024*1024,
            'dpv_name': 'dpv0',
        }, {
            'leg_idx': 1,
            'leg_size': 20*1024*1024,
            'dpv_name': 'dpv1',
        }],
    }, {
        'group_idx': 1,
        'group_size': 50*1024*1024*1024,
        'legs': [{
            'leg_idx': 0,
            'leg_size': 50*1024*1024*1024,
            'dpv_name': 'dpv0',
        }, {
            'leg_idx': 1,
            'leg_size': 50*1024*1024*1024,
            'dpv_name': 'dpv1',
        }],
    }]
}


class DlvTest(unittest.TestCase):

    def setUp(self):
        self.dbm = DataBaseManager(cfg.get('database', 'db_uri'))
        self.dbm.setup()

    def tearDown(self):
        self.dbm.teardown()

    @patch('dlvm.worker.dlv.dpv_rpc')
    def test_dlv_create(self, dpv_rpc):
        self.dbm.dvg_create(**fake_dvg)
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
            self.dbm.dvg_extend(
                fake_dvg['dvg_name'], fake_dpv['dpv_name'])
        self.dbm.dlv_create(fake_dlv)
        lock_owner = uuid.uuid4().hex
        lock_type = LockType.dlv
        lock_dt = datetime.utcnow()
        lock = self.dbm.lock_create(lock_owner, lock_type, lock_dt)
        dlv_name = fake_dlv['dlv_name']
        self.dbm.dlv_set(dlv_name, 'lock_id', lock.lock_id)
        self.dbm.dlv_set(dlv_name, 'status', DlvStatus.creating)
        mock_wait = dpv_rpc.async_client \
            .return_value \
            .leg_create \
            .return_value \
            .wait
        mock_wait.return_value = None
        req_id = uuid.uuid4()
        sm_ctx = StateMachineContextSchema.nt(
            'dlv_create', 'start', StepType.forward, 0, set(),
            lock.lock_id, lock.lock_owner, lock.lock_dt)
        sm_ctx_d = StateMachineContextSchema().dump(sm_ctx)
        sm_handler(str(req_id), sm_ctx_d, dlv_name)
        self.assertEqual(mock_wait.call_count, 4)
        self.dbm.update_session()
        dlv = self.dbm.dlv_get(dlv_name)
        self.assertEqual(dlv.status, DlvStatus.available)

    @patch('dlvm.worker.dlv.dpv_rpc')
    def test_dlv_delete(self, dpv_rpc):
        self.dbm.dvg_create(**fake_dvg)
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
            self.dbm.dvg_extend(
                fake_dvg['dvg_name'], fake_dpv['dpv_name'])
        self.dbm.dlv_create(fake_dlv)
        lock_owner = uuid.uuid4().hex
        lock_type = LockType.dlv
        lock_dt = datetime.utcnow()
        lock = self.dbm.lock_create(lock_owner, lock_type, lock_dt)
        dlv_name = fake_dlv['dlv_name']
        self.dbm.dlv_set(dlv_name, 'lock_id', lock.lock_id)
        self.dbm.dlv_set(dlv_name, 'status', DlvStatus.deleting)
        mock_wait = dpv_rpc.async_client \
            .return_value \
            .leg_delete \
            .return_value \
            .wait
        mock_wait.return_value = None
        req_id = uuid.uuid4()
        sm_ctx = StateMachineContextSchema.nt(
            'dlv_delete', 'start', StepType.forward, 0, set(),
            lock.lock_id, lock.lock_owner, lock.lock_dt)
        sm_ctx_d = StateMachineContextSchema().dump(sm_ctx)
        sm_handler(str(req_id), sm_ctx_d, dlv_name)
        self.assertEqual(mock_wait.call_count, 4)
        self.dbm.update_session()
        dlv = self.dbm.dlv_get(dlv_name)
        self.assertEqual(dlv, None)
