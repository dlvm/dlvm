import sys
import uuid
from collections import namedtuple
from datetime import datetime, timedelta
from logging import getLogger, LoggerAdapter

from dlvm.common.constant import LOCK_HANDLER_NAME, MONITOR_LOGGER_NAME
from dlvm.common.configure import cfg
from dlvm.common.utils import RequestContext, ExcInfo
from dlvm.common.modules import Lock, LockType, MonitorLock, \
    DlvStatus, DistributeLogicalVolume, DistributePhysicalVolume
from dlvm.common.database import Session, release_lock
from dlvm.wrapper.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook
from dlvm.wrapper.local_ctx import frontend_local
from dlvm.worker.monitor_ctx import MonitorContext
from dlvm.worker.dlv import DlvCreate, DlvDelete, DlvAttach, DlvDetach

ori_logger = getLogger(MONITOR_LOGGER_NAME)
monitor_hook_list = build_hook_list('monitor_hook')

expire_seconds = cfg.getint('lock', 'expire_seconds')
grace_seconds = cfg.getint('lock', 'grace_seconds')
total_seconds = expire_seconds + grace_seconds

lock_seconds = timedelta(seconds=total_seconds)


handler_dict = {}


def register_lock_handler(lock_type):

    def wrapper(func):
        handler_dict[lock_type] = func
        return func

    return wrapper


@register_lock_handler(LockType.dpv.value)
def dpv_lock_handler(lock_nt):
    session = frontend_local.session
    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(lock_id=lock_nt.lock_id) \
        .one()
    release_lock(
        session, lock_nt.lock_id, lock_nt.lock_dt, dpv.dpv_name)


@register_lock_handler(LockType.dlv.value)
def dlv_lock_handler(lock_nt):
    session = frontend_local.session
    lock = session.query(Lock) \
        .filter_by(lock_id=lock_nt.lock_id) \
        .with_lockmode('update') \
        .one()
    assert(lock.lock_dt == lock_nt.lock_dt)
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(lock_id=lock.lock_id) \
        .with_lockmode('update') \
        .one()
    if dlv.status == DlvStatus.creating:
        sm = DlvCreate(dlv.dlv_name)
        sm.start(lock)
    elif dlv.status == DlvStatus.attaching:
        sm = DlvAttach(dlv.dlv_name)
        sm.start(lock)
    elif dlv.status == DlvStatus.detaching:
        sm = DlvDetach(dlv.dlv_name)
        sm.start(lock)
    elif dlv.status == DlvStatus.deleting:
        sm = DlvDelete(dlv.dlv_name)
        sm.start(lock)
    else:
        frontend_local.req_ctx.logger('DlvStatus: %s' % dlv.status)


LockNt = namedtuple('LockNt', [
    'lock_id', 'lock_type', 'lock_dt', 'req_id_hex'])


def lock_handler(batch):
    session = Session()
    logger = LoggerAdapter(ori_logger, {'req_id': None})
    req_ctx = RequestContext(None, logger)
    hook_ctx = MonitorContext(req_ctx, 'lock_handler', None)
    hook_ret_dict = run_pre_hook(
        'monitor', monitor_hook_list, hook_ctx)
    current_dt = datetime.utcnow().replace(microsecond=0)
    expire_dt = current_dt - lock_seconds
    try:
        session.query(MonitorLock) \
            .filter_by(name=LOCK_HANDLER_NAME) \
            .with_lockmode('update') \
            .one()
        locks = session.query(Lock) \
            .filter(Lock.lock_dt < expire_dt) \
            .order_by(Lock.lock_dt.asc()) \
            .limit(batch) \
            .all()
        lock_nt_list = []
        for ilock in locks:
            lock = session.query(Lock) \
                .filter_by(lock_id=ilock.lock_id) \
                .with_lockmode('update') \
                .one()
            assert(lock.lock_dt < expire_dt)
            lock.lock_dt = current_dt
            session.add(lock)
            lock_nt = LockNt(
                lock_id=lock.lock_id,
                lock_type=lock.lock_type,
                lock_dt=lock.lock_dt,
                req_id_hex=lock.req_id_hex,
            )
            lock_nt_list.append(lock_nt)
        session.commit()
    except Exception:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        session.rollback()
        run_error_hook(
            'monitor', monitor_hook_list, hook_ctx,
            hook_ret_dict, exc_info)
    else:
        run_post_hook(
            'monitor', monitor_hook_list, hook_ctx,
            hook_ret_dict, lock_nt_list)
    finally:
        session.close()

    for lock_nt in lock_nt_list:
        req_id = uuid.UUID(hex=lock_nt.req_id_hex)
        logger = LoggerAdapter(ori_logger, {'req_id': req_id})
        req_ctx = RequestContext(req_id, logger)
        frontend_local.req_ctx = req_ctx
        session = Session()
        frontend_local.session = session
        hook_ctx = MonitorContext(
            req_ctx, 'lock_handler', lock_nt)
        hook_ret_dict = run_pre_hook(
            'monitor', monitor_hook_list, hook_ctx)
        try:
            handler_dict[lock_nt.lock_type.value](lock_nt)
        except Exception:
            etype, value, tb = sys.exc_info()
            exc_info = ExcInfo(etype, value, tb)
            session.rollback()
            run_error_hook(
                'monitor', monitor_hook_list, hook_ctx,
                hook_ret_dict, exc_info)
        else:
            run_post_hook(
                'monitor', monitor_hook_list, hook_ctx,
                hook_ret_dict, None)
        finally:
            session.close()
