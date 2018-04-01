from dlvm.common.database import Session
from dlvm.core.modules import DistributePhysicalVolume, DistributeVolumeGroup


class DataBaseManager():

    def __init__(self):
        self.session = Session()

    def dpv_create(
            self, dpv_name,
            total_size, free_size, status):
        dpv = DistributePhysicalVolume(
            dpv_name=dpv_name,
            total_size=total_size,
            free_size=free_size,
            status=status)
        self.session.add(dpv)
        self.session.commit()

    def dpv_get(self, dpv_name):
        dpv = self.session.query(DistributePhysicalVolume) \
              .filter_by(dpv_name=dpv_name) \
              .one_or_none()
        return dpv

    def dpv_set(self, dpv_name, name, value):
        dpv = self.session.query(DistributePhysicalVolume) \
              .filter_by(dpv_name=dpv_name) \
              .one()
        setattr(dpv, name, value)
        self.session.add(dpv)
        self.session.commit()

    def dvg_create(self, dvg_name):
        dvg = DistributeVolumeGroup(
            dvg_name=dvg_name,
            total_size=0,
            free_size=0,
        )
        self.session.add(dvg)
        self.session.commit()
