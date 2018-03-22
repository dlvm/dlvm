#!/usr/bin/env python

import logging
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from dlvm.utils.error import DpvError, LimitExceedError, \
    ResourceDuplicateError, ResourceNotFoundError, ResourceBusyError
from dlvm.utils.modules import db, \
    DistributePhysicalVolume
from dlvm.api_server.handler import general_query, DpvClient


logger = logging.getLogger('dlvm_api')


def handle_dpvs_get(request_id, args, path_args):
    if args['limit'] > conf.dpv_list_limit:
        raise LimitExceedError(args['limit'], conf.dpv_list_limit)
    dpvs = general_query(
        DistributePhysicalVolume, args, ['status', 'locked'])
    return dpvs


def handle_dpvs_post(request_id, args, path_args):
    dpv_name = args['dpv_name']
    client = DpvClient(dpv_name, 0)
    try:
        ret = client.get_size()
        ret.wait()
        dpv_info = ret.value
        total_size = dpv_info['total_size']
        free_size = dpv_info['free_size']
    except Exception:
        logger.error('request_id=%s failed', exc_info=True)
        raise DpvError(dpv_name)
    dpv = DistributePhysicalVolume(
        dpv_name=args['dpv_name'],
        total_size=total_size,
        free_size=free_size,
        status='available',
    )
    db.session.add(dpv)
    try:
        db.session.commit()
    except IntegrityError:
        raise ResourceDuplicateError('dpv', dpv_name)
    return None


def handle_dpv_get(request_id, args, path_args):
    dpv_name = path_args[0]
    try:
        dpv = DistributePhysicalVolume \
              .query \
              .filter_by(dpv_name=dpv_name) \
              .one()
    except NoResultFound:
        raise ResourceNotFoundError('dpv', dpv_name)
    return dpv


def handle_dpv_delete(request_id, args, path_args):
    dpv_name = path_args[0]
    try:
        dpv = DistributePhysicalVolume \
              .query \
              .filter_by(dpv_name=dpv_name) \
              .one()
    except NoResultFound:
        return None
    if dpv.dvg_name is not None:
        raise ResourceBusyError(
            'dpv', dpv_name, 'dvg', dpv.dvg_name)
    db.session.delete(dpv)
    db.session.commit()
    return None
