import unittest
from unittest.mock import patch

from dlvm.worker.helper import get_dm_ctx
from dlvm.dpv_agent import dpv_get_info, leg_create, LegCreateArgSchema, \
    leg_delete, LegDeleteArgSchema, DpvSyncArgSchema, LegInfoSchema, \
    dpv_sync, dpv_ping


class DpvAgentTest(unittest.TestCase):

    @patch('dlvm.dpv_agent.cmd')
    def test_dpv_get_info(self, cmd_mock):
        total_size = 500*1024*1024*1024
        free_size = 200*1024*1024*1024
        cmd_mock.vg_get_size.return_value = (
            total_size, free_size)
        dpv_info = dpv_get_info()
        self.assertEqual(dpv_info.total_size, total_size)
        self.assertEqual(dpv_info.free_size, free_size)

    @patch('dlvm.dpv_agent.cmd')
    def test_leg_create(self, cmd_mock):
        leg_id = 0
        leg_size = 1024*1024*1024
        dm_ctx = get_dm_ctx()
        arg = LegCreateArgSchema.nt(leg_id, leg_size, dm_ctx)
        leg_create(arg)

    @patch('dlvm.dpv_agent.cmd')
    def test_leg_delete(self, cmd_mock):
        arg = LegDeleteArgSchema.nt(0)
        leg_delete(arg)

    @patch('dlvm.dpv_agent.cmd')
    def test_dpv_sync(self, cmd_mock):
        leg_id = 0
        leg_size = 1024*1024*1024
        ihost_name = None
        leg_info = LegInfoSchema.nt(leg_id, leg_size, ihost_name)
        dm_ctx = get_dm_ctx()
        arg = DpvSyncArgSchema.nt([leg_info], dm_ctx)
        dpv_sync(arg)

    def test_dpv_ping(self):
        ret = dpv_ping()
        self.assertEqual(ret, 'ok')
