from dlvm.common.configure import cfg
from dlvm.common.modules import DistributeLogicalVolume
from dlvm.wrapper.local_ctx import frontend_local
from dlvm.wrapper.state_machine import StateMachine, sm_register


dlv_create_queue = cfg.get('mq', 'dlv_create_queue')
dlv_delete_queue = cfg.get('mq', 'dlv_delete_queue')


def dlv_create(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    print(dlv)


@sm_register
class DlvCreate(StateMachine):

    sm_name = 'dlv_create'
    queue_name = dlv_create_queue
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


@sm_register
class DlvDelete(StateMachine):

    sm_name = 'dlv_delete'
    queue_name = dlv_delete_queue
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
