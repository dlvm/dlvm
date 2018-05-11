import os
import json
import uuid
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.constant import LC_PATH, SQLALCHEMY_CFG_FILE
from dlvm.common.configure import cfg
from dlvm.common.modules import Base, Lock, LockType, \
    DistributeLogicalVolume

sqlalchemy_cfg_path = os.path.join(LC_PATH, SQLALCHEMY_CFG_FILE)

if os.path.isfile(sqlalchemy_cfg_path):
    with open(sqlalchemy_cfg_path) as f:
        sqlalchemy_kwargs = json.load(f)
else:
    sqlalchemy_kwargs = {}

engine = create_engine(cfg.get('database', 'db_uri'), **sqlalchemy_kwargs)
Session = sessionmaker(bind=engine)


def verify_lock(session, lock_id, lock_owner):
    lock = session.query(Lock) \
        .filter_by(lock_id=lock_id) \
        .with_lockmode('update') \
        .one()
    assert(lock.lock_owner == lock_owner)
    return lock


def acquire_lock(session, lock_id, lock_owner):
    new_owner = uuid.uuid4().hex
    lock = verify_lock(session, lock_id, lock_owner)
    lock_dt = datetime.utcnow()
    lock.lock_owner = new_owner
    lock.lock_dt = lock_dt
    session.add(lock)
    session.commit()
    return new_owner, lock_dt


def remove_lock(session, lock, res_id):
    if lock.lock_type == LockType.dlv:
        dlv = session.query(DistributeLogicalVolume) \
            .filter_by(dlv_name=res_id) \
            .with_lockmode('update') \
            .one_or_none()
        if dlv is not None:
            dlv.lock = None
            session.add(dlv)
    else:
        msg = 'unknown lock type: %s' % lock.lock_type
        raise Exception(msg)


def release_lock(session, lock_id, lock_owner, res_id):
    lock = verify_lock(session, lock_id, lock_owner)
    remove_lock(session, lock, res_id)
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


def create_all():
    Base.metadata.create_all(engine)


def drop_all():
    Base.metadata.drop_all(engine)
