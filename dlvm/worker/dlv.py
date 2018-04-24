from dlvm.common.configure import cfg
from dlvm.wrapper.state_machine import StateMachine


class DlvCreate(StateMachine):

    sm_name = 'dlv_create'
    sm = {}
    queue_name = cfg.get('mq', 'dlv_create_queue')

    def __init__(self, args):
        self.args = args

    def get_args(self):
        return self.args

    @classmethod
    def get_sm_name(cls):
        return cls.sm_name

    @classmethod
    def get_sm(cls):
        return cls.sm

    @classmethod
    def get_queue(cls):
        return cls.queue_name
