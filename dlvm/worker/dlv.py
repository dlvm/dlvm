from dlvm.common.configure import cfg
from dlvm.common.utils import chunks
from dlvm.common.modules import DistributePhysicalVolume, \
    DistributeVolumeGroup, DistributeLogicalVolume, DlvStatus
from dlvm.wrapper.local_ctx import frontend_local
from dlvm.wrapper.state_machine import StateMachine, sm_register, SmRetry, \
    BiDirState, UniDirState, BiDirJob, UniDirJob
from dlvm.dpv_agent import dpv_rpc, LegCreateArgSchema, LegDeleteArgSchema
from dlvm.worker.helper import get_dm_ctx
from dlvm.worker.allocator import Allocator, AllocationError


dlv_create_queue = cfg.get('mq', 'dlv_create_queue')
dlv_delete_queue = cfg.get('mq', 'dlv_delete_queue')


def allocate_dpvs_for_group(dvg, group, allocator):
    session = frontend_local.session
    legs = sorted(group.legs, key=lambda x: x.leg_idx)
    for leg0, leg1 in chunks(legs, 2):
        assert(leg0.leg_size == leg1.leg_size)
        assert(leg0.dpv_name is None)
        assert(leg1.dpv_name is None)
        dpv0, dpv1 = allocator.get_pair(leg0.leg_size)
        dpv0 = session.query(DistributePhysicalVolume) \
            .filter_by(dpv_name=dpv0.dpv_name) \
            .with_lockmode('update') \
            .one()
        assert(dpv0.free_size >= leg0.leg_size)
        dpv0.free_size -= leg0.leg_size
        leg0.dpv = dpv0
        session.add(dpv0)
        session.add(leg0)
        assert(dvg.free_size >= leg0.leg_size)
        dvg.free_size -= leg0.leg_size
        dpv1 = session.query(DistributePhysicalVolume) \
            .filter_by(dpv_name=dpv1.dpv_name) \
            .with_lockmode('update') \
            .one()
        assert(dpv1.free_size >= leg1.leg_size)
        leg1.dpv = dpv1
        session.add(dpv1)
        session.add(leg1)
        assert(dvg.free_size >= leg1.leg_size)
        dvg.free_size -= leg1.leg_size


def dlv_allocate_dpv(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    dvg = session.query(DistributeVolumeGroup) \
        .filter_by(dvg_name=dlv.dvg_name) \
        .with_lockmode('update') \
        .one()
    allocator = Allocator(session, dlv.dvg_name)
    try:
        for group in dlv.groups:
            allocate_dpvs_for_group(dvg, group, allocator)
        dlv.status = DlvStatus.available
    except AllocationError as e:
        raise SmRetry(e.message)
    else:
        session.add(dlv)
        session.add(dvg)


def dlv_create_leg(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    dm_ctx = get_dm_ctx()
    thread_list = []
    for group in dlv.groups:
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


def dlv_delete_leg(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    thread_list = []
    for group in dlv.groups:
        for leg in group.legs:
            dpv_name = leg.dpv_name
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


def dlv_release_dpv(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one()
    dvg = session.query(DistributeVolumeGroup) \
        .filter_by(dvg_name=dlv.dvg_name) \
        .with_lockmode('update') \
        .one()
    for group in dlv.groups:
        for leg in group.legs:
            if leg.dpv_name is None:
                continue
            dpv = session.query(DistributePhysicalVolume) \
                .filter_by(dpv_name=leg.dpv_name) \
                .with_lockmode('update') \
                .one()
            dpv.free_size += leg.leg_size
            session.add(dpv)
            leg.dpv_name = None
            session.add(leg)
            dvg.free_size += leg.leg_size
    session.add(dvg)


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
        for gsnap in group.gsnaps:
            session.delete(gsnap)
        session.delete(group)
    for snapshot in dlv.snapshots:
        session.delete(snapshot)
    session.delete(dlv)


class DlvAllocateDpvJob(BiDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_allocate_dpv(self.dlv_name)

    def backward(self):
        # if dlv_allocate_dpv failed, all db actions will rollback
        # so nothing to do
        pass


class DlvCreateLegJob(BiDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_create_leg(self.dlv_name)

    def backward(self):
        dlv_delete_leg(self.dlv_name)


class DlvDeleteLegJob(UniDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_delete_leg(self.dlv_name)


class DlvReleaseDpvJob(UniDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_release_dpv(self.dlv_name)


class DlvFailedJob(UniDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_failed(self.dlv_name)


dlv_create_sm = {
    'start': BiDirState(DlvAllocateDpvJob, 'create_leg', 'failed'),
    'create_leg': BiDirState(DlvCreateLegJob, 'stop', 'retry'),
    'retry': UniDirState(DlvReleaseDpvJob, 'allocate_2'),
    'allocate_2': BiDirState(DlvAllocateDpvJob, 'create_leg_2', 'failed'),
    'create_leg_2': BiDirState(DlvCreateLegJob, 'stop', 'release'),
    'release': UniDirState(DlvReleaseDpvJob, 'failed'),
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
        dlv_release_dpv(self.dlv_name)


class DlvDeleteJob(UniDirJob):

    def __init__(self, dlv_name):
        self.dlv_name = dlv_name

    def forward(self):
        dlv_delete(self.dlv_name)


dlv_delete_sm = {
    'start': UniDirState(DlvDeleteLegJob, 'release'),
    'release': UniDirState(DlvReleaseDpvJob, 'delete'),
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
