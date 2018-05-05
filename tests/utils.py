from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.constant import DEFAULT_SNAP_NAME
from dlvm.common.configure import cfg
from dlvm.common.modules import Base, DistributePhysicalVolume, \
    DistributeVolumeGroup, DistributeLogicalVolume, DlvStatus, \
    ServiceStatus, DiskStatus, Lock, SnapStatus, Snapshot, Group, Leg


thin_block_size = cfg.getsize('device_mapper', 'thin_block_size')


class DataBaseManager():

    def __init__(self, db_uri):
        engine = create_engine(db_uri)
        Session = sessionmaker(bind=engine)
        self.engine = engine
        self.Session = Session
        self.db_uri = db_uri
        self.session = Session()

    def setup(self):
        idx = self.db_uri.rfind('/')
        base_uri = self.db_uri[:idx]
        db_name = self.db_uri[idx+1:]
        engine = create_engine(base_uri)
        connection = engine.connect()
        connection.execute('CREATE DATABASE {0}'.format(db_name))
        connection.close()
        Base.metadata.create_all(self.engine)

    def teardown(self):
        if self.session is not None:
            self.session.close()
        idx = self.db_uri.rfind('/')
        base_uri = self.db_uri[:idx]
        db_name = self.db_uri[idx+1:]
        engine = create_engine(base_uri)
        connection = engine.connect()
        connection.execute('DROP DATABASE {0}'.format(db_name))
        connection.close()

    def update_session(self):
        if self.session is not None:
            self.session.close()
        self.session = self.Session()

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
            .filter_by(dvg_name=dvg_name) \
            .one()
        dpv = self.session.query(DistributePhysicalVolume) \
            .filter_by(dpv_name=dpv_name) \
            .one()
        dpv.dvg_name = dvg_name
        self.session.add(dpv)
        dvg.free_size += dpv.free_size
        dvg.total_size += dpv.total_size
        self.session.add(dvg)
        self.session.commit()

    def dvg_get(self, dvg_name):
        dvg = self.session.query(DistributeVolumeGroup) \
            .filter_by(dvg_name=dvg_name) \
            .one_or_none()
        return dvg

    def dlv_create(self, dlv_info):
        snap_name = DEFAULT_SNAP_NAME
        snap_id = '%s%s' % (dlv_info['dlv_name'], snap_name)

        dlv = DistributeLogicalVolume(
            dlv_name=dlv_info['dlv_name'],
            dlv_size=dlv_info['dlv_size'],
            data_size=dlv_info['init_size'],
            stripe_number=dlv_info['stripe_number'],
            status=DlvStatus.available,
            bm_ignore=dlv_info['bm_ignore'],
            bm_dirty=False,
            dvg_name=dlv_info['dvg_name'],
            active_snap_name=snap_name,
        )
        self.session.add(dlv)

        snap = Snapshot(
            snap_id=snap_id,
            snap_name=snap_name,
            thin_id=0,
            ori_thin_id=0,
            status=SnapStatus.available,
            thin_mapping=bytes(),
            dlv_name=dlv_info['dlv_name'],
        )
        self.session.add(snap)

        dvg = self.session.query(DistributeVolumeGroup) \
            .filter_by(dvg_name=dlv_info['dvg_name']) \
            .one()

        for igroup in dlv_info['groups']:
            bitmap_size = igroup['group_size'] // (thin_block_size * 8)
            group = Group(
                group_idx=igroup['group_idx'],
                group_size=igroup['group_size'],
                dlv_name=dlv_info['dlv_name'],
                bitmap=bytes((0x0,)*bitmap_size)
            )
            self.session.add(group)
            for ileg in igroup['legs']:
                leg = Leg(
                    leg_idx=ileg['leg_idx'],
                    leg_size=ileg['leg_size'],
                    group=group,
                    dpv_name=ileg['dpv_name'],
                )
                self.session.add(leg)
                dpv = self.session.query(DistributePhysicalVolume) \
                    .filter_by(dpv_name=ileg['dpv_name']) \
                    .one()
                dpv.free_size -= ileg['leg_size']
                self.session.add(dpv)
                dvg.free_size -= ileg['leg_size']
            self.session.add(dvg)
            self.session.commit()

    def dlv_get(self, dlv_name):
        dlv = self.session.query(DistributeLogicalVolume) \
            .filter_by(dlv_name=dlv_name) \
            .one_or_none()
        return dlv

    def dlv_set(self, dlv_name, field_name, field_value):
        dlv = self.session.query(DistributeLogicalVolume) \
            .filter_by(dlv_name=dlv_name) \
            .one()
        setattr(dlv, field_name, field_value)
        self.session.add(dlv)
        self.session.commit()

    def snap_get(self, dlv_name, snap_name):
        snap_id = '{0}/{1}'.format(dlv_name, snap_name)
        snap = self.session.query(Snapshot) \
            .filter_by(snap_id=snap_id) \
            .one_or_none()
        return snap

    def lock_create(self, lock_owner, lock_type, lock_dt):
        lock = Lock(
            lock_owner=lock_owner,
            lock_type=lock_type,
            lock_dt=lock_dt,
        )
        self.session.add(lock)
        self.session.commit()
        return lock
