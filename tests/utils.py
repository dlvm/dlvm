from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.constant import DEFAULT_SNAP_NAME
from dlvm.common.modules import Base, DistributePhysicalVolume, \
    DistributeVolumeGroup, DistributeLogicalVolume, DlvStatus, \
    ServiceStatus, DiskStatus, Lock, SnapStatus, Snapshot, Group, Leg


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

    def dlv_create(self, dlv_info):
        session = self.Session()
        snap_name = DEFAULT_SNAP_NAME
        snap_id = '%s%s' % (dlv_info['dlv_name'], snap_name)
        dlv = DistributeLogicalVolume(
            dlv_name=dlv_info['dlv_name'],
            dlv_size=dlv_info['dlv_size'],
            data_size=dlv_info['init_size'],
            stripe_number=dlv_info['stripe_number'],
            status=DlvStatus.available,
            active_snap_id=snap_id,
            dvg_name=dlv_info['dvg_name'],
        )
        session.add(dlv)
        snap = Snapshot(
            snap_id=snap_id,
            snap_name=snap_name,
            thin_id=0,
            ori_thin_id=0,
            status=SnapStatus.available,
            dlv_name=dlv_info['dlv_name'],
        )
        session.add(snap)

        dvg = session.query(DistributeVolumeGroup) \
            .filter_by(dvg_name=dlv_info['dvg_name']) \
            .one()

        for igroup in dlv_info['groups']:
            group = Group(
                group_idx=igroup['group_idx'],
                group_size=igroup['group_size'],
                dlv_name=dlv_info['dlv_name'],
            )
            session.add(group)
            for ileg in igroup['legs']:
                leg = Leg(
                    leg_idx=ileg['leg_idx'],
                    leg_size=ileg['leg_size'],
                    group_id=group.group_id,
                    dpv_name=ileg['dpv_name'],
                )
                session.add(leg)
                dpv = session.query(DistributePhysicalVolume) \
                    .filter_by(dpv_name=ileg['dpv_name']) \
                    .one()
                dpv.free_size -= ileg['leg_size']
                session.add(dpv)
                dvg.free_size -= ileg['leg_size']
            session.add(dvg)
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
