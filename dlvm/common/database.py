import os
import json

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from dlvm.common.constant import LC_PATH, SQLALCHEMY_CFG_FILE, \
    LOCK_HANDLER_NAME, DPV_HANDLER_NAME
from dlvm.common.configure import cfg
from dlvm.common.modules import Base, Lock, LockType, \
    DistributeLogicalVolume, MonitorLock, \
    DistributePhysicalVolume

sqlalchemy_cfg_path = os.path.join(LC_PATH, SQLALCHEMY_CFG_FILE)

if os.path.isfile(sqlalchemy_cfg_path):
    with open(sqlalchemy_cfg_path) as f:
        sqlalchemy_kwargs = json.load(f)
else:
    sqlalchemy_kwargs = {}

engine = create_engine(cfg.get('database', 'db_uri'), **sqlalchemy_kwargs)
Session = sessionmaker(bind=engine)


def verify_lock(session, lock_id, lock_dt):
    lock = session.query(Lock) \
        .filter_by(lock_id=lock_id) \
        .with_lockmode('update') \
        .one()
    assert(lock.lock_dt == lock_dt)
    return lock


def acquire_lock(session, lock_id, lock_dt, old_lock_dt):
    lock = verify_lock(session, lock_id, old_lock_dt)
    lock.lock_dt = lock_dt
    session.add(lock)
    session.commit()


def remove_dlv_lock(session, lock, res_id):
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=res_id) \
        .with_lockmode('update') \
        .one_or_none()
    if dlv is not None:
        assert(dlv.lock is not None)
        assert(dlv.lock.lock_id == lock.lock_id)
        dlv.lock = None
        session.add(dlv)


def remove_dpv_lock(session, lock, res_id):
    dlvs = session.query(DistributeLogicalVolume) \
        .filter_by(lock_id=lock.lock_id) \
        .all()
    for dlv in dlvs:
        assert(dlv.lock is not None)
        assert(dlv.lock.lock_id == lock.lock_id)
        dlv.lock = None
        session.add(dlv)
    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(dpv_name=res_id) \
        .with_lockmode('update') \
        .one()
    assert(dpv.lock is not None)
    assert(dpv.lock.lock_id == lock.lock_id)
    dpv.lock = None
    session.add(dpv)


lock_remove_dict = {
    LockType.dlv: remove_dlv_lock,
    LockType.dpv: remove_dpv_lock,
}


def release_lock(session, lock_id, lock_dt, res_id):
    lock = verify_lock(session, lock_id, lock_dt)
    remove_func = lock_remove_dict[lock.lock_type]
    remove_func(session, lock, res_id)
    session.delete(lock)
    session.commit()


class GeneralQuery():

    def __init__(self, session, obj_cls):
        self.session = session
        self.obj_cls = obj_cls
        self.order_fields = []
        self.is_fields = {}
        self.isnot_fields = {}
        self.offset = None
        self.limit = None

    def set_offset(self, offset):
        self.offset = offset

    def set_limit(self, limit):
        self.limit = limit

    def add_order_field(self, order_name, reverse):
        self.order_fields.append((order_name, reverse))

    def add_is_field(self, field_name, value):
        self.is_fields[field_name] = value

    def add_isnot_field(self, field_name, value):
        self.isnot_fields[field_name] = value

    def query(self):
        query = self.session.query(self.obj_cls)
        filter_list = []
        for order_name, reverse in self.order_fields:
            order_field = getattr(self.obj_cls, order_name)
            if reverse is True:
                order_attr = order_field.desc()
            else:
                order_attr = order_field.asc()
            filter_list.append(order_attr)
        query = query.order_by(*filter_list)
        for field_name in self.is_fields:
            field = getattr(self.obj_cls, field_name)
            value = self.is_fields[field_name]
            query = query.filter(field.is_(value))
        for field_name in self.isnot_fields:
            field = getattr(self.obj_cls, field_name)
            value = self.isnot_fields[field_name]
            query = query.filter(field.isnot(value))
        if self.offset is not None:
            query = query.offset(self.offset)
        if self.limit is not None:
            query = query.limit(self.limit)
        return query.all()


def create_monitor_lock(name):
    session = Session()
    try:
        ml = MonitorLock(name=name)
        session.add(ml)
        session.commit()
    except IntegrityError:
        session.rollback()
    finally:
        session.close()


def create_all():
    Base.metadata.create_all(engine)
    create_monitor_lock(LOCK_HANDLER_NAME)
    create_monitor_lock(DPV_HANDLER_NAME)


def drop_all():
    Base.metadata.drop_all(engine)
