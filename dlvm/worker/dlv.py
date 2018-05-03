from dlvm.common.configure import cfg
from dlvm.common.modules import DistributeLogicalVolume
from dlvm.wrapper.local_ctx import frontend_local
from dlvm.wrapper.state_machine import StateMachine, sm_register


dlv_create_queue = cfg.get('mq', 'dlv_create_queue')
dlv_delete_queue = cfg.get('mq', 'dlv_delete_queue')

test_mode = cfg.getboolean('device_mapper', 'test_mode')
dpv_search_overhead = cfg.getint('device_mapper', 'dpv_search_overhead')


def allocate_dpvs_for_group(group, dvg_name, lock):
    dpvs = []
    dpv_name_set = set()
    batch_count = len(group.legs) * dpv_search_overhead
    i = -1
    total_leg_size = 0
    for leg in group.legs:
        i += 1
        if leg.dpv is not None:
            continue
        leg_size = leg.leg_size
        while True:
            if len(dpvs) == 0:
                if test_mode is True:
                    dpvs = select_dpvs(
                        dvg_name, leg_size, batch_count, 0)
                else:
                    dpvs = select_dpvs(
                        dvg_name, leg_size, batch_count, i)
            if len(dpvs) == 0:
                msg = 'allocate dpvs failed, {0} {1}'.format(
                    dvg_name, leg_size)
                raise Exception(msg)
            dpv = dpvs.pop()
            if dpv.dpv_name in dpv_name_set:
                continue
            else:
                if test_mode is False:
                    dpv_name_set.add(dpv.dpv_name)
            dpv = session.query(DistributePhysicalVolume) \
                .filter_by(dpv_name=dpv.dpv_name) \
                .with_lockmode('update') \
                .one()
            if dpv.service_status != ServiceStatus.available:
                continue
            if dpv.disk_status != DiskStatus.available:
                continue
            if dpv.free_size < leg_size:
                continue
            dpv.free_size -= leg_size
            total_leg_size += leg_size
            leg.dpv = dpv
            session.add(dpv)
            session.add(leg)
            break

    dvg = session.query(DistributeVolumeGroup) \
        .filter_by(dvg_name=dvg_name) \
        .with_lockmode('update') \
        .one()
    assert(dvg.free_size >= total_leg_size)
    dvg.free_size -= total_leg_size
    session.add(dvg)
    verify_lock(lock)
    session.commit()

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
