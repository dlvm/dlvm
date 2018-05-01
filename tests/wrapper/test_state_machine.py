import unittest
from datetime import datetime
import uuid

from dlvm.common.configure import cfg
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

    def setUp(self):
        self.dbm = DataBaseManager(cfg.get('database', 'db_uri'))
        self.dbm.setup()

    def tearDown(self):
        self.dbm.teardown()

    def test_state_machine(self):
        lock = self.dbm.lock_create(
            uuid.uuid4().hex, 'dlv', datetime.utcnow())
        sm_ctx = StateMachineContextSchema.nt(
            'foo_sm', 'start', StepType.forward, 0,
            set(), lock.lock_id, lock.lock_owner, lock.lock_dt)
        sm_ctx_d = StateMachineContextSchema().dump(sm_ctx)
        sm_handler(uuid.uuid4().hex, sm_ctx_d, 0)
