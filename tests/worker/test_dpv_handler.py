import unittest
from unittest.mock import patch
from datetime import datetime, timedelta

from dlvm.common.configure import cfg
from dlvm.common.modules import DpvStatus
from dlvm.worker.dpv_handler_task import dpv_handler

from tests.utils import DataBaseManager


fake_dpv = {
    'dpv_name': 'dpv0',
    'total_size': 512*1024*1024*1024,
    'free_size': 512*1024*1024*1024,
}


class DpvHandlerTest(unittest.TestCase):

    def setUp(self):
        self.dbm = DataBaseManager(
            cfg.get('database', 'db_uri'))
        self.dbm.setup()

    def tearDown(self):
        self.dbm.teardown()

    @patch('dlvm.worker.dpv_handler_task.dpv_rpc')
    def test_dpv_handler(self, dpv_rpc):
        self.dbm.dpv_create(**fake_dpv)
        self.dbm.dpv_set(
            fake_dpv['dpv_name'], 'status', DpvStatus.recoverable)
        status_dt = datetime.utcnow().replace(
            microsecond=0) - timedelta(seconds=3600)
        self.dbm.dpv_set(
            fake_dpv['dpv_name'], 'status_dt', status_dt)
        dpv_handler(1)
        self.assertEqual(dpv_rpc.sync_client.call_count, 1)
