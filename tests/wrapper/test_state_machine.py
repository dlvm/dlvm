import unittest
import os
from datetime import datetime
import uuid

from dlvm.wrapper.state_machine import sm_handler, sm_register, \
    StateMachineContextSchema, UniDirJob, UniDirState, StateMachine, StepType

from tests.utils import DataBaseManager


class Job1(UniDirJob):

    def __init__(self, res_id):
        self.res_id = res_id

    def forward(self):
        pass


foo_sm = {
    'start': UniDirState(Job1, 'stop')
}


@sm_register
class Foo(StateMachine):

    @classmethod
    def get_sm_name(self):
        return 'foo_sm'

    @classmethod
    def get_queue(self):
        return 'foo_sm_queue'

    @classmethod
    def get_sm(self):
        return foo_sm


class StateMachineTest(unittest.TestCase):

    db_path = '/tmp/dlvm_test.db'
    db_uri = 'sqlite:////tmp/dlvm_test.db'

    def setUp(self):
        self.dbm = DataBaseManager(self.db_uri)

    def tearDown(self):
        if os.path.isfile(self.db_path):
            os.remove(self.db_path)

    def test_state_machine(self):
        lock = self.dbm.lock_create(
            uuid.uuid4().hex, 'dlv', datetime.utcnow())
        sm_ctx = StateMachineContextSchema.nt(
            'foo_sm', 'start', StepType.forward, 0,
            set(), lock.lock_id, lock.lock_owner, lock.lock_dt)
        sm_ctx_d = StateMachineContextSchema().dump(sm_ctx)
        sm_handler(uuid.uuid4().hex, sm_ctx_d, 0)
