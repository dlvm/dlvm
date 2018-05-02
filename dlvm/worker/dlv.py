from dlvm.common.configure import cfg
from dlvm.wrapper.state_machine import StateMachine, sm_register


@sm_register
class DlvCreate(StateMachine):

    sm_name = 'dlv_create'
    queue_name = cfg.get('mq', 'dlv_create_queue')
    sm = {}

    @classmethod
    def get_sm_name(cls):
        return cls.sm_name

    @classmethod
    def get_queue(cls):
        return cls.queue_name

    @classmethod
    def get_sm(cls):
        return cls.sm
