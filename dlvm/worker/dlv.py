import random

from dlvm.common.configure import cfg
from dlvm.common.modules import DistributePhysicalVolume, \
    DistributeVolumeGroup, DistributeLogicalVolume, ServiceStatus, \
    DiskStatus, DlvStatus
from dlvm.wrapper.local_ctx import frontend_local
from dlvm.wrapper.state_machine import StateMachine, sm_register, SmRetry, \
    BiDirState, UniDirState, BiDirJob, UniDirJob
from dlvm.dpv_agent import dpv_rpc, LegCreateArgSchema, LegDeleteArgSchema
from dlvm.worker.helper import get_dm_ctx


dlv_create_queue = cfg.get('mq', 'dlv_create_queue')
dlv_delete_queue = cfg.get('mq', 'dlv_delete_queue')

test_mode = cfg.getboolean('device_mapper', 'test_mode')
dpv_search_overhead = cfg.getint('device_mapper', 'dpv_search_overhead')


def select_dpvs(dvg_name, required_size, batch_count, offset):
    session = frontend_local.session
    dpvs = session.query(DistributePhysicalVolume) \
        .filter_by(dvg_name=dvg_name) \
        .filter_by(service_status=ServiceStatus.available) \
        .filter_by(disk_status=DiskStatus.available) \
        .filter(DistributePhysicalVolume.free_size > required_size) \
        .ordeer_by(DistributePhysicalVolume.free_size.desc()) \
        .limit(batch_count) \
        .offset(offset) \
        .with_entities(DistributePhysicalVolume.dpv_name) \
        .all()
    random.shuffle(dpvs)
    return dpvs


def allocate_dpvs_for_group(dlv, group):
    session = frontend_local.session
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
                        dlv.dvg_name, leg_size, batch_count, 0)
                else:
                    dpvs = select_dpvs(
                        dlv.dvg_name, leg_size, batch_count, i)
            if len(dpvs) == 0:
                msg = 'allocate dpvs failed, {0} {1}'.format(
                    dlv.dvg_name, leg_size)
                raise SmRetry(msg)
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
        .filter_by(dvg_name=dlv.dvg_name) \
        .with_lockmode('update') \
        .one()
    assert(dvg.free_size >= total_leg_size)
    dvg.free_size -= total_leg_size
    session.add(dvg)

    dm_ctx = get_dm_ctx()
    thread_list = []
    for leg in group.legs:
        dpv_name = leg.dpv_name
        ac = dpv_rpc.async_client(dpv_name)
        arg = LegCreateArgSchema.nt(leg.leg_id, leg.leg_size, dm_ctx)
        t = ac.leg_create(arg)
        thread_list.append(t)
    err_list = []
    for t in thread_list:
        err = t.wait()
        err_list.append(err)
    for err in err_list:
        if err:
            raise SmRetry()


def dlv_create(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    for group in dlv.groups:
        allocate_dpvs_for_group(dlv, group)
    dlv.status = DlvStatus.available
    session.add(dlv)


def release_dpvs_from_group(dlv, group):
    session = frontend_local.session
    dpv_dict = {}
    thread_list = []
    for leg in group.legs:
        dpv_name = leg.dpv_name
        if dpv_name is None:
            continue
        dpv = session.query(DistributePhysicalVolume) \
            .filter_by(dpv_name=dpv_name) \
            .with_lockmode('update') \
            .one()
        dpv_dict[dpv_name] = dpv
        if dpv.service_status == ServiceStatus.available:
            ac = dpv_rpc.async_client(dpv_name)
            arg = LegDeleteArgSchema.nt(leg.leg_id)
            t = ac.leg_delete(arg)
            thread_list.append(t)
    err_list = []
    for t in thread_list:
        err = t.wait()
        err_list.append(err)
    for err in err_list:
        if err:
            raise err
    total_free_size = 0
    for leg in group.legs:
        if leg.dpv_name is None:
            continue
        dpv = dpv_dict[leg.dpv_name]
        leg_size = leg.leg_size
        dpv.free_size += leg_size
        session.add(dpv)
        leg.dpv_name = None
        session.add(leg)
        total_free_size += leg_size

    dvg = session.query(DistributeVolumeGroup) \
        .filter_by(dvg_name=dlv.dvg_name) \
        .with_lockmode('update') \
        .one()
    dvg.free_size += total_free_size
    session.add(dvg)


def dlv_release(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    for group in dlv.groups:
        release_dpvs_from_group(dlv, group)


def dlv_failed(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    assert(dlv.status == DlvStatus.creating)
    dlv.status = DlvStatus.failed
    session.add(dlv)


def dlv_delete(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    assert(dlv.status == DlvStatus.deleting)
    for group in dlv.groups:
        for leg in group.legs:
            assert(leg.dpv is None)
            session.delete(leg)
        session.delete(group)
    for snapshot in dlv.snapshots:
        session.delete(snapshot)
    session.delete(dlv)


class DlvCreateJob(BiDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_create(self.dlv_name)

    def backward(self):
        dlv_release(self.dlv_name)


class DlvFailedJob(UniDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_failed(self.dlv_name)


dlv_create_sm = {
    'start': BiDirState(DlvCreateJob, 'stop', 'retry1'),
    'retry1': BiDirState(DlvCreateJob, 'stop', 'failed'),
    'failed': UniDirState(DlvFailedJob, 'stop'),
}


@sm_register
class DlvCreate(StateMachine):

    sm_name = 'dlv_create'
    queue_name = dlv_create_queue
    sm = dlv_create_sm

    @classmethod
    def get_sm_name(cls):
        return cls.sm_name

    @classmethod
    def get_queue(cls):
        return cls.queue_name

    @classmethod
    def get_sm(cls):
        return cls.sm


class DlvReleaseJob(UniDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_release(self.dlv_name)


class DlvDeleteJob(UniDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_delete(self.dlv_name)


dlv_delete_sm = {
    'start': UniDirState(DlvReleaseJob, 'delete'),
    'delete': UniDirState(DlvDeleteJob, 'stop'),
}


@sm_register
class DlvDelete(StateMachine):

    sm_name = 'dlv_delete'
    queue_name = dlv_delete_queue
    sm = dlv_delete_sm

    @classmethod
    def get_sm_name(cls):
        return cls.sm_name

    @classmethod
    def get_queue(cls):
        return cls.queue_name

    @classmethod
    def get_sm(cls):
        return cls.sm
