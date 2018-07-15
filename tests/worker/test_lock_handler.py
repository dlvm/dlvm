import unittest
from unittest.mock import patch
import uuid
from datetime import datetime, timedelta

from dlvm.common.configure import cfg
from dlvm.common.modules import LockType, DlvStatus
from dlvm.worker.lock_handler_task import lock_handler

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
    'dvg_name': 'dvg0',
    'groups': [{
        'group_idx': 0,
        'group_size': 20*1024*1024,
        'legs': [{
            'leg_idx': 0,
            'leg_size': 20*1024*1024,
            'dpv_name': None,
        }, {
            'leg_idx': 1,
            'leg_size': 20*1024*1024,
            'dpv_name': None,
        }],
    }, {
        'group_idx': 1,
        'group_size': 50*1024*1024*1024,
        'legs': [{
            'leg_idx': 0,
            'leg_size': 50*1024*1024*1024,
            'dpv_name': None,
        }, {
            'leg_idx': 1,
            'leg_size': 50*1024*1024*1024,
            'dpv_name': None,
        }],
    }]
}


class LockHanderTest(unittest.TestCase):

    def setUp(self):
        self.dbm = DataBaseManager(
            cfg.get('database', 'db_uri'))
        self.dbm.setup()

    def tearDown(self):
        self.dbm.teardown()

    @patch('dlvm.wrapper.state_machine.sm_handler')
    def test_dlv_create(self, sm_handler):
        self.dbm.dvg_create(**fake_dvg)
        for fake_dpv in fake_dpvs:
            self.dbm.dpv_create(**fake_dpv)
            self.dbm.dvg_extend(
                fake_dvg['dvg_name'], fake_dpv['dpv_name'])
        self.dbm.dlv_create(fake_dlv)
        lock_type = LockType.dlv
        lock_dt = datetime.utcnow().replace(
            microsecond=0) - timedelta(seconds=3600)
        req_id_hex = uuid.uuid4().hex
        lock = self.dbm.lock_create(
            lock_type, lock_dt, req_id_hex)
        dlv_name = fake_dlv['dlv_name']
        self.dbm.dlv_set(dlv_name, 'lock_id', lock.lock_id)
        self.dbm.dlv_set(dlv_name, 'status', DlvStatus.creating)
        lock_handler(1)
        self.assertEqual(sm_handler.apply_async.call_count, 1)
