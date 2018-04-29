from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.modules import Base, DistributePhysicalVolume, \
    DistributeVolumeGroup, DistributeLogicalVolume, DlvStatus, \
    ServiceStatus, DiskStatus, Lock


class DataBaseManager():

    def __init__(self, db_uri):
        engine = create_engine(db_uri)
        Session = sessionmaker(bind=engine)
        self.Session = Session
        Base.metadata.create_all(engine)

    def dpv_create(
            self, dpv_name,
            total_size, free_size):
        session = self.Session()
        dpv = DistributePhysicalVolume(
            dpv_name=dpv_name,
            total_size=total_size,
            free_size=free_size,
            service_status=ServiceStatus.available,
            disk_status=DiskStatus.available)
        session.add(dpv)
        session.commit()

    def dpv_get(self, dpv_name):
        session = self.Session()
        dpv = session.query(DistributePhysicalVolume) \
            .filter_by(dpv_name=dpv_name) \
            .one_or_none()
        return dpv

    def dpv_set(self, dpv_name, name, value):
        session = self.Session()
        dpv = session.query(DistributePhysicalVolume) \
            .filter_by(dpv_name=dpv_name) \
            .one()
        setattr(dpv, name, value)
        session.add(dpv)
        session.commit()

    def dvg_create(self, dvg_name):
        session = self.Session()
        dvg = DistributeVolumeGroup(
            dvg_name=dvg_name,
            total_size=0,
            free_size=0,
        )
        session.add(dvg)
        session.commit()

    def dvg_extend(self, dvg_name, dpv_name):
        session = self.Session()
        dvg = session.query(DistributeVolumeGroup) \
            .filter_by(dvg_name=dvg_name) \
            .one()
        dpv = session.query(DistributePhysicalVolume) \
            .filter_by(dpv_name=dpv_name) \
            .one()
        dpv.dvg_name = dvg_name
        session.add(dpv)
        dvg.free_size += dpv.free_size
        dvg.total_size += dpv.total_size
        session.add(dvg)
        session.commit()

    def dvg_get(self, dvg_name):
        session = self.Session()
        dvg = session.query(DistributeVolumeGroup) \
            .filter_by(dvg_name=dvg_name) \
            .one_or_none()
        return dvg

    def dlv_create(
            self, dlv_name, dlv_size, data_size, stripe_number,
            dvg_name, igroups):
        session = self.Session()
        snap_name = '%s/base' % dlv_name
        dlv = DistributeLogicalVolume(
            dlv_name=dlv_name,
            dlv_size=dlv_size,
            data_size=data_size,
            stripe_number=stripe_number,
            status=DlvStatus.available,
            active_snap_name=snap_name,
            dvg_name=dvg_name,
        )
        session.add(dlv)
        session.commit()

    def lock_create(self, lock_owner, lock_type, lock_dt):
        session = self.Session()
        lock = Lock(
            lock_owner=lock_owner,
            lock_type=lock_type,
            lock_dt=lock_dt,
        )
        session.add(lock)
        session.commit()
        return lock
