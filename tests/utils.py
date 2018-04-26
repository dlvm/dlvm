from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.modules import Base, DistributePhysicalVolume, \
    DistributeVolumeGroup, DistributeLogicalVolume, DlvStatus, \
    ServiceStatus, DiskStatus, Lock


class DataBaseManager():

    def __init__(self, db_uri):
        engine = create_engine(db_uri)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        Base.metadata.create_all(engine)

    def dpv_create(
            self, dpv_name,
            total_size, free_size):
        dpv = DistributePhysicalVolume(
            dpv_name=dpv_name,
            total_size=total_size,
            free_size=free_size,
            service_status=ServiceStatus.available,
            disk_status=DiskStatus.available)
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

    def dvg_extend(self, dvg_name, dpv_name):
        dvg = self.session.query(DistributeVolumeGroup) \
            .query \
            .filter_by(dvg_name=dvg_name) \
            .one()
        dpv = self.session.query(DistributePhysicalVolume) \
            .query \
            .filter_by(dpv_name=dpv_name) \
            .one()
        dpv.dvg_name = dvg_name
        self.session.add(dpv)
        dvg.free_size += dpv.free_size
        dvg.total_size += dpv.total_size
        self.session.add(dvg)
        self.session.commit()

    def dlv_create(
            self, dlv_name, dlv_size, data_size, stripe_number,
            dvg_name, igroups):
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
        print(dlv)

    def lock_create(self, lock_owner, lock_type, lock_dt):
        lock = Lock(
            lock_owner=lock_owner,
            lock_type=lock_type,
            lock_dt=lock_dt,
        )
        self.session.add(lock)
        self.session.commit()
        return lock
