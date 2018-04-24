import os
import json
import uuid
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.constant import LC_PATH, SQLALCHEMY_CFG_FILE
from dlvm.common.configure import cfg
from dlvm.common.modules import Lock

sqlalchemy_cfg_path = os.path.join(LC_PATH, SQLALCHEMY_CFG_FILE)

if os.path.isfile(sqlalchemy_cfg_path):
    with open(sqlalchemy_cfg_path) as f:
        sqlalchemy_kwargs = json.load(f)
else:
    sqlalchemy_kwargs = {}

engine = create_engine(cfg.get('database', 'db_uri'), **sqlalchemy_kwargs)
Session = sessionmaker(bind=engine)


def acquire_lock(session, lock_id, lock_owner):
    new_owner = uuid.uuid4().hex
    lock = session.query(Lock) \
        .filter_by(lock_id=lock_id) \
        .with_lockmode('update') \
        .one()
    assert(lock.lock_owner == lock_owner)
    lock_dt = datetime.utcnow()
    lock.lock_owner = new_owner
    lock.lock_dt = lock_dt
    session.add(lock)
    session.commit()
    return new_owner, lock_dt


def release_lock(session, lock_id, lock_owner, commit):
    lock = session.query(Lock) \
        .filter_by(lock_id=lock_id) \
        .with_lockmode('update') \
        .one()
    assert(lock.lock_owner == lock_owner)
    session.delete(lock)
    if commit is True:
        session.commit()
