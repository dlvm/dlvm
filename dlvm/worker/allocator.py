import enum

from dlvm.common.configure import cfg
from dlvm.common.modules import DistributePhysicalVolume, \
    ServiceStatus, DiskStatus


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
        self.locations = []
        self.stage = Stage.first

    def select_by_range(self, required_size, max_size, exclude):
        query = self.session.query(DistributePhysicalVolume) \
                .filter_by(dvg_name=self.dvg_name) \
                .filter_by(service_status=ServiceStatus.available) \
                .filter_by(disk_status=DiskStatus.available) \
                .filter(DistributePhysicalVolume.free_size.between(
                    required_size, max_size))
        if exclude is not None:
            query = query.filter(
                DistributePhysicalVolume.location.isnot(exclude))
        if self.stage == Stage.first:
            query = query.filter(
                DistributePhysicalVolume.location.notin_(self.locations))
        dpv = query.order_by(DistributePhysicalVolume.free_size.desc()) \
            .limit(1) \
            .with_entities(
                DistributePhysicalVolume.dpv_name,
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

    def get_pair(self, required_size):
        dpv0 = self.get_dpv(required_size, None)
        dpv1 = self.get_dpv(required_size, dpv0.location)
        return (dpv0, dpv1)
