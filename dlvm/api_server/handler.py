#!/usr/bin/env python

from collections import OrderedDict
import uuid
import datetime
import logging
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.error import ObtConflictError, ObtMissError
from dlvm.utils.configure import conf
from modules import db, DistributePhysicalVolume, DistributeVolumeGroup, \
    DistributeLogicalVolume, Snapshot, OwnerBasedTransaction, Counter

logger = logging.getLogger('dlvm_api')


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
        'major': obt.count,
        'minor': obt.minor_count,
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
