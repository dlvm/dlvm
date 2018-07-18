import enum

from dlvm.common.configure import cfg
from dlvm.common.modules import DistributePhysicalVolume, \
    DpvStatus


low_size = cfg.getsize('allocator', 'low_size')
middle_size = cfg.getsize('allocator', 'middle_size')
high_size = cfg.getsize('allocator', 'high_size')

bound_list = [low_size, middle_size, high_size, 2**62]


class AllocationError(Exception):

    def __init__(self, message):
        self.message = message
        super(AllocationError, self).__init__(message)


class Stage(enum.Enum):
    first = 0
    second = 1


class Allocator():

    def __init__(self, session, dvg_name):
        self.session = session
        self.dvg_name = dvg_name
        self.locations = set()
        self.stage = Stage.first
        self.skip_dpv_name = None

    def select_by_range(self, required_size, max_size, exclude):
        query = self.session.query(DistributePhysicalVolume) \
                .filter_by(dvg_name=self.dvg_name) \
                .filter_by(status=DpvStatus.available) \
                .filter(DistributePhysicalVolume.free_size.between(
                    required_size, max_size))
        if exclude is not None:
            query = query.filter(
                DistributePhysicalVolume.location != exclude)
        if self.stage == Stage.first:
            query = query.filter(
                DistributePhysicalVolume.location.notin_(list(self.locations)))
        if self.skip_dpv_name is not None:
            query = query.filter(
                DistributePhysicalVolume.dpv_name != self.skip_dpv_name)
        dpv = query.order_by(DistributePhysicalVolume.free_size.desc()) \
            .limit(1) \
            .with_entities(
                DistributePhysicalVolume.dpv_name,
                DistributePhysicalVolume.free_size,
                DistributePhysicalVolume.location) \
            .one_or_none()
        return dpv

    def get_dpv(self, required_size, exclude):
        for max_size in bound_list:
            dpv = self.select_by_range(required_size, max_size, exclude)
            if dpv is not None:
                return dpv
        self.stage = Stage.second
        for max_size in bound_list:
            dpv = self.select_by_range(required_size, max_size, exclude)
            if dpv is not None:
                return dpv

        msg = 'allocate dpv failed, {0}, {1} {2}'.format(
            self.dvg_name, required_size, exclude)
        raise AllocationError(msg)

    def update_locations(self, dpv):
        if dpv.location is not None:
            self.locations.add(dpv.location)

    def get_pair(self, required_size):
        self.skip_dpv_name = None
        dpv0 = self.get_dpv(required_size, None)
        self.update_locations(dpv0)
        dpv1 = self.get_dpv(required_size, dpv0.location)
        if dpv1.dpv_name == dpv0.dpv_name and \
           dpv1.free_size < 2*required_size:
            self.skip_dpv_name = dpv0.dpv_name
            dpv1 = self.get_dpv(required_size, dpv0.location)
        self.update_locations(dpv1)
        return (dpv0, dpv1)
