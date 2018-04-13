import unittest
from unittest.mock import patch

from dlvm.dpv_agent import dpv_get_info


class DpvAgentTest(unittest.TestCase):

    @patch('dlvm.dpv_agent.cmd')
    def test_dpv_get_info(self, cmd_mock):
        total_size = 500*1024*1024*1024
        free_size = 200*1024*1024*1024
        cmd_mock.vg_get_size.return_value = (
            total_size, free_size)
        dpv_info = dpv_get_info()
        self.assertEqual(dpv_info['total_size'], total_size)
        self.assertEqual(dpv_info['free_size'], free_size)
