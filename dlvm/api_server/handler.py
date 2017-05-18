#!/usr/bin/env python

from collections import OrderedDict
import uuid
import datetime
import logging
import socket
from xmlrpclib import Fault
from types import MethodType
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.error import ObtConflictError, ObtMissError, \
    DpvError, IhostError
from dlvm.utils.configure import conf
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.helper import dlv_info_encode
from modules import db, DistributePhysicalVolume, DistributeVolumeGroup, \
    DistributeLogicalVolume, Snapshot, OwnerBasedTransaction, Counter

logger = logging.getLogger('dlvm_api')


class DpvClient(object):

    def __init__(self, dpv_name):
        self.client = WrapperRpcClient(
            str(dpv_name),
            conf.dpv_port,
            conf.dpv_timeout,
        )
        self.dpv_name = dpv_name

    def __getattr__(self, name):
        def wrapper_func(self, *args, **kwargs):
            func = getattr(self.client, name)
            try:
                logger.info(
                    'dpv call: %s %s %s %s',
                    name,
                    self.dpv_name,
                    args,
                    kwargs,
                )
                ret = func(*args, **kwargs)
                logger.info(
                    'dpv ret: %s %s',
                    self.dpv_name,
                    ret,
                )
                return ret
            except socket.error, socket.timeout:
                logger.error('connect to dpv failed: %s', self.dpv_name)
                raise DpvError(self.dpv_name)
            except Fault as e:
                if 'ObtConflict' in str(e):
                    raise ObtConflictError()
                else:
                    logger.error('dpv rpc failed: %s', e)
                    raise DpvError(self.dpv_name)
        setattr(self, name, MethodType(wrapper_func, self, self.__class__))
        return getattr(self, name)


class IhostClient(object):

    def __init__(self, ihost_name):
        self.client = WrapperRpcClient(
            str(ihost_name),
            conf.ihost_port,
            conf.ihost_timeout,
        )
        self.ihost_name = ihost_name

    def __getattr__(self, name):
        def wrapper_func(self, *args, **kwargs):
            func = getattr(self.client, name)
            try:
                logger.info(
                    'ihost call: %s %s %s %s',
                    name,
                    self.ihost_name,
                    args,
                    kwargs,
                )
                ret = func(*args, **kwargs)
                logger.info(
                    'ihost ret: %s %s',
                    self.ihost_name,
                    ret,
                )
                return ret
            except socket.error, socket.timeout:
                logger.error('connect to ihost failed: %s', self.ihost_name)
                raise IhostError(self.ihost_name)
            except Fault as e:
                if 'ObtConflict' in str(e):
                    raise ObtConflictError()
                else:
                    logger.error('ihost rpc failed: %s', e)
                    raise IhostError(self.ihost_name)
        setattr(self, name, MethodType(wrapper_func, self, self.__class__))
        return getattr(self, name)


def handle_dlvm_request(params, parser, handler):
    request_id = uuid.uuid4().hex
    response = OrderedDict()
    response['request_id'] = request_id
    if parser:
        args = parser.parse_args()
    else:
        args = None
    logger.info('request_id=%s, params=%s, args=%s, handler=%s',
                request_id, params, args, handler.__name__)
    try:
        body, return_code = handler(params, args)
    except ObtConflictError:
        db.session.rollback()
        logger.warning('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'obt_conflict',
        }
        return_code = 400
        response['body'] = body
    except ObtMissError:
        db.session.rollback()
        logger.warning('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'obt_miss',
        }
        return_code = 400
        response['body'] = body
    except:
        db.session.rollback()
        logger.error('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'internal_error',
        }
        return_code = 500
        response['body'] = body
    finally:
        db.session.close()
        logger.info('request_id=%s\nbody=%s\nreturn_code=%d',
                    request_id, body, return_code)
        response['body'] = body
        return response, return_code


def make_body(message, context=None):
    body = {'message': message}
    if context is not None:
        body['context'] = context
    return body


def check_limit(limit):
    def _check_limit(val):
        val = int(val)
        if val > limit:
            val = limit
        return val
    return _check_limit


def dpv_get_by_name(dpv_name):
    dpv = DistributePhysicalVolume \
        .query \
        .with_lockmode('update') \
        .filter_by(dpv_name=dpv_name) \
        .one()
    return dpv


def dvg_get_by_name(dvg_name):
    dvg = DistributeVolumeGroup \
        .query \
        .with_lockmode('update') \
        .filter_by(dvg_name=dvg_name) \
        .one()
    return dvg


def dlv_get_by_name(dlv_name):
    dlv = DistributeLogicalVolume \
        .query \
        .with_lockmode('update') \
        .filter_by(dlv_name=dlv_name) \
        .one()
    return dlv


def snapshot_get_by_name(snap_name):
    snapshot = Snapshot \
        .query \
        .with_lockmode('update') \
        .filter_by(snap_name=snap_name) \
        .one()
    return snapshot


def obt_get(t_id, t_owner, t_stage):
    try:
        obt = OwnerBasedTransaction \
            .query \
            .with_lockmode('update') \
            .filter_by(t_id=t_id) \
            .one()
    except NoResultFound:
        raise ObtMissError()
    if obt.t_owner != t_owner:
        raise ObtConflictError()
    counter = Counter()
    db.session.delete(obt.counter)
    obt.counter = counter
    obt.t_stage = t_stage
    db.session.add(obt)
    db.session.commit()
    try:
        obt = OwnerBasedTransaction \
            .query \
            .with_lockmode('update') \
            .filter_by(t_id=t_id) \
            .one()
    except NoResultFound:
        raise ObtMissError()
    if obt.t_owner != t_owner:
        raise ObtConflictError()
    obt.minor_count = 0
    return obt


def obt_refresh(obt):
    try:
        obt1 = OwnerBasedTransaction \
            .query \
            .with_lockmode('update') \
            .filter_by(t_id=obt.t_id) \
            .one()
    except NoResultFound:
        raise ObtMissError()
    if obt1.t_owner != obt.t_owner:
        raise ObtConflictError()
    obt1.timestamp = datetime.datetime.utcnow()
    db.session.add(obt1)


def dlv_get(dlv_name, t_id, t_owner, t_stage):
    obt = obt_get(t_id, t_owner, t_stage)
    dlv = dlv_get_by_name(dlv_name)
    if dlv.obt is None:
        dlv.obt = obt
        db.session.add(dlv)
        db.session.commit()
        return dlv, obt
    else:
        if dlv.obt.t_id != t_id:
            raise ObtConflictError()
        return dlv, obt


def obt_encode(obt):
    obt.minor_count += 1
    return {
        'major': str(obt.count),
        'minor': str(obt.minor_count),
    }


def div_round_up(dividend, divisor):
    return (dividend+divisor-1) / divisor


def get_dm_context():
    return {
        'thin_block_size': conf.thin_block_size,
        'mirror_meta_blocks': conf.mirror_meta_blocks,
        'mirror_region_size': conf.mirror_region_size,
        'stripe_chunk_blocks': conf.stripe_chunk_blocks,
        'low_water_mark': conf.low_water_mark,
    }


def get_dlv_info(dlv):
    dlv_info = {}
    dlv_info['dlv_name'] = dlv.dlv_name
    dlv_info['dlv_size'] = dlv.dlv_size
    dm_context = get_dm_context()
    dm_context['stripe_number'] = dlv.stripe_number
    dlv_info['dm_context'] = dm_context
    dlv_info['data_size'] = dlv.data_size
    snapshot = Snapshot \
        .query \
        .filter_by(snap_name=dlv.active_snap_name) \
        .with_entities(Snapshot.thin_id) \
        .one()
    dlv_info['thin_id'] = snapshot.thin_id
    dlv_info['groups'] = []
    for group in dlv.groups:
        igroup = {}
        igroup['group_id'] = group.group_id
        igroup['idx'] = group.idx
        igroup['group_size'] = group.group_size
        igroup['legs'] = []
        for leg in group.legs:
            ileg = {}
            ileg['dpv_name'] = leg.dpv_name
            ileg['leg_id'] = leg.leg_id
            ileg['idx'] = leg.idx
            ileg['leg_size'] = leg.leg_size
            igroup['legs'].append(ileg)
        dlv_info['groups'].append(igroup)
    dlv_info_encode(dlv_info)
    return dlv_info


dlv_detach_list = []


def dlv_detach_register(func):
    dlv_detach_list.append(func)
    return func


def dlv_detach_check(dlv):
    for func in dlv_detach_list:
        func(dlv)


dlv_delete_list = []


def dlv_delete_register(func):
    dlv_delete_list.append(func)
    return func


def dlv_delete_check(dlv):
    for func in dlv_delete_list:
        func(dlv)
