#!/usr/bin/env python

import logging
from sqlalchemy.exc import IntegrityError
from dlvm.utils.configure import conf
from dlvm.utils.error import DpvError, \
    ResourceDuplicateError, ExceedLimitError
from dlvm.utils.modules import db, \
    DistributePhysicalVolume
from dlvm.api_server.handler import general_query, DpvClient


logger = logging.getLogger('dlvm_api')


def handle_dpvs_get(request_id, args, path_args):
    if args['limit'] > conf.dpv_list_limit:
        raise ExceedLimitError(args['limit'], conf.dpv_list_limit)
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
