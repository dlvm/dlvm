import unittest
from unittest.mock import patch


from dlvm.common.schema import DlvInfoSchema
from dlvm.worker.helper import get_dm_ctx
from dlvm.ihost_agent import dlv_aggregate, AggregateArgSchema


fake_dlv_info = {
    'dlv_size': 1024*1024*1024*1024,
    'stripe_number': 2,
    'groups': [{
        'group_id': 0,
        'group_idx': 0,
        'group_size': 1024*1024*1024*10,
        'legs': [{
            'leg_id': 0,
            'leg_idx': 0,
            'leg_size': 1024*1024*12,
            'dpv_name': 'dpv0',
        }, {
            'leg_id': 1,
            'leg_idx': 1,
            'leg_size': 1024*1024*12,
            'dpv_name': 'dpv0',
        }, {
            'leg_id': 2,
            'leg_idx': 2,
            'leg_size': 1024*1024*1024*10,
            'dpv_name': 'dpv0',
        }, {
            'leg_id': 3,
            'leg_idx': 3,
            'leg_size': 1024*1024*1024*10,
            'dpv_name': 'dpv0',
        }]
    }, {
        'group_id': 1,
        'group_idx': 1,
        'group_size': 1024*1024*1024*20,
        'legs': [{
            'leg_id': 4,
            'leg_idx': 0,
            'leg_size': 1024*1024*12,
            'dpv_name': 'dpv0',
        }, {
            'leg_id': 5,
            'leg_idx': 1,
            'leg_size': 1024*1024*12,
            'dpv_name': 'dpv0',
        }, {
            'leg_id': 6,
            'leg_idx': 2,
            'leg_size': 1024*1024*1024*20,
            'dpv_name': 'dpv0',
        }, {
            'leg_id': 7,
            'leg_idx': 3,
            'leg_size': 1024*1024*1024*20,
            'dpv_name': 'dpv0',
        }]
    }]
}


class IhostAgentTest(unittest.TestCase):

    @patch('dlvm.ihost_agent.cmd')
    @patch('dlvm.ihost_agent.backend_local')
    def test_dlv_aggregate(self, backend_local_mock, cmd_mock):
        dm_ctx = get_dm_ctx()
        dlv_info1 = DlvInfoSchema().dump(fake_dlv_info)
        dlv_info = DlvInfoSchema().load(dlv_info1)
        arg = AggregateArgSchema.nt(
            dlv_name='dlv0',
            snap_id='0',
            dlv_info=dlv_info,
            dm_ctx=dm_ctx,
        )
        dlv_aggregate(arg)
