import sys
import uuid
from datetime import datetime, timedelta
from logging import getLogger, LoggerAdapter

from dlvm.common.constant import DPV_HANDLER_NAME, MONITOR_LOGGER_NAME
from dlvm.common.configure import cfg
from dlvm.common.error import RpcError
from dlvm.common.utils import RequestContext, ExcInfo
from dlvm.common.modules import Lock, LockType, MonitorLock, \
    DpvStatus, DistributeLogicalVolume, \
    DistributePhysicalVolume
from dlvm.common.database import Session, release_lock
from dlvm.wrapper.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook
from dlvm.wrapper.local_ctx import frontend_local
from dlvm.worker.monitor_ctx import MonitorContext
from dlvm.worker.helper import get_dm_ctx
from dlvm.dpv_agent import dpv_rpc, DpvSyncArgSchema, \
    LegInfoSchema


ori_logger = getLogger(MONITOR_LOGGER_NAME)
monitor_hook_list = build_hook_list('monitor_hook')

expire_seconds = cfg.getint('lock', 'expire_seconds')
grace_seconds = cfg.getint('lock', 'grace_seconds')
total_seconds = expire_seconds + grace_seconds

lock_seconds = timedelta(seconds=total_seconds)


def sync_one_dpv(dpv_name):
    session = frontend_local.session
    logger = frontend_local.req_ctx.logger
    client = dpv_rpc.sync_client(dpv_name)
    try:
        client.ping()
    except RpcError:
        logger.debug('dpv ping failed')
        return
    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(dpv_name=dpv_name) \
        .with_lockmode('update') \
        .one()
    dlv_list = []
    dlv_name_list = []
    dm_ctx = get_dm_ctx()
    arg = DpvSyncArgSchema.nt(dpv_info=[], dm_ctx=dm_ctx)
    if dpv.lock is not None:
        logger.debug('dpv has lock')
        return
    for leg in dpv.legs:
        if leg.group is None:
            logger.debug('leg group is None: %s', leg.leg_id)
            return
        dlv_name = leg.group.dlv_name
        dlv = session.query(DistributeLogicalVolume) \
            .filter_by(dlv_name=dlv_name) \
            .with_lockmode('update') \
            .one()
        if len(dlv.fjs) != 0:
            logger.debug('dlv has fjs: %s', dlv.dlv_name)
            return
        if len(dlv.src_cjs) != 0:
            logger.debug('dlv has src_cjs: %s', dlv.dlv_name)
            return
        if dlv.dst_cj is not None:
            logger.debug('dlv has dst_cj: %s', dlv.dlv_name)
            return
        if dlv.lock is not None:
            logger.debug('dlv has lock: %s', dlv.dlv_name)
            return
        leg_info = LegInfoSchema.nt(leg.leg_id, leg.leg_size, dlv.ihost_name)
        arg.dpv_info.append(leg_info)
        dlv_list.append(dlv)
        dlv_name_list.append(dlv.dlv_name)
    lock = Lock(
        lock_type=LockType.dpv,
        lock_dt=datetime.utcnow().replace(microsecond=0),
        req_id_hex=frontend_local.req_ctx.req_id.hex,
    )
    session.add(lock)
    dpv.lock = lock
    session.add(dpv)
    for dlv in dlv_list:
        dlv.lock = lock
        session.add(dlv)
    session.commit()
    try:
        client.dpv_sync(arg)
    finally:
        release_lock(
            session, lock.lock_id, lock.lock_dt, dpv.dpv_name)


def dpv_handler(batch):
    session = Session()
    logger = LoggerAdapter(ori_logger, {'req_id': None})
    req_ctx = RequestContext(None, logger)
    hook_ctx = MonitorContext(req_ctx, 'dpv_handler', None)
    hook_ret_dict = run_pre_hook(
        'monitor', monitor_hook_list, hook_ctx)
    current_dt = datetime.utcnow().replace(microsecond=0)
    expire_dt = current_dt - lock_seconds
    try:
        session.query(MonitorLock) \
            .filter_by(name=DPV_HANDLER_NAME) \
            .with_lockmode('update') \
            .one()
        dpvs = session.query(DistributePhysicalVolume) \
            .filter(DistributePhysicalVolume.status_dt < expire_dt) \
            .filter_by(lock_id=None) \
            .filter_by(status=DpvStatus.recoverable) \
            .order_by(DistributePhysicalVolume.status_dt.asc()) \
            .limit(batch) \
            .all()
        dpv_list = []
        for idpv in dpvs:
            dpv = session.query(DistributePhysicalVolume) \
                .filter_by(dpv_name=idpv.dpv_name) \
                .with_lockmode('update') \
                .one()
            assert(dpv.status_dt < expire_dt)
            assert(dpv.status == DpvStatus.recoverable)
            assert(dpv.lock_id is None)
            dpv.status_dt = current_dt
            session.add(dpv)
            session.commit()
            dpv_list.append(dpv.dpv_name)
    except Exception:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        session.rollback()
        run_error_hook(
            'monitor', monitor_hook_list, hook_ctx,
            hook_ret_dict, exc_info)
        dpv_list = []
    else:
        run_post_hook(
            'monitor', monitor_hook_list, hook_ctx,
            hook_ret_dict, dpv_list)
    finally:
        session.close()

    for dpv_name in dpv_list:
        req_id = uuid.uuid4()
        logger = LoggerAdapter(ori_logger, {'req_id': req_id})
        req_ctx = RequestContext(req_id, logger)
        frontend_local.req_ctx = req_ctx
        session = Session()
        frontend_local.session = session
        hook_ctx = MonitorContext(
            req_ctx, 'dpv_handler', dpv_name)
        hook_ret_dict = run_pre_hook(
            'monitor', monitor_hook_list, hook_ctx)
        try:
            sync_one_dpv(dpv.dpv_name)
        except Exception:
            etype, value, tb = sys.exc_info()
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
