#!/usr/bin/env python

import logging
import traceback
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from dlvm.utils.error import LimitExceedError, \
    ResourceDuplicateError, ResourceNotFoundError, ResourceBusyError
from dlvm.utils.modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume
from dlvm.api_server.handler import general_query


logger = logging.getLogger('dlvm_api')


def handle_dvgs_get(request_id, args, path_args):
    if args['limit'] > conf.dvg_list_limit:
        raise LimitExceedError(args['limit'], conf.dpv_list_limit)
    dvgs = general_query(
        DistributeVolumeGroup, args, [])
    return dvgs


def handle_dvgs_post(request_id, args, path_args):
    dvg_name = args['dvg_name']
    dvg = DistributeVolumeGroup(
        dvg_name=dvg_name,
        total_size=0,
        free_size=0,
    )
    db.session.add(dvg)
    try:
        db.session.commit()
    except IntegrityError:
        raise ResourceDuplicateError('dvg', dvg_name, traceback.format_exc())
    return None


def handle_dvg_get(request_id, args, path_args):
    dvg_name = path_args[0]
    try:
        dvg = DistributeVolumeGroup \
              .query \
              .filter_by(dvg_name=dvg_name) \
              .one()
    except NoResultFound:
        raise ResourceNotFoundError(
            'dvg', dvg_name, traceback.format_exc())
    return dvg


def handle_dvg_delete(request_id, args, path_args):
    dvg_name = path_args[0]
    try:
        dvg = DistributeVolumeGroup \
              .query \
              .with_lockmode('update') \
              .filter_by(dvg_name=dvg_name) \
              .one()
    except NoResultFound:
        return None

    dpvs = dvg \
        .dpvs \
        .with_entities(DistributePhysicalVolume.dpv_name) \
        .limit(1) \
        .all()
    if len(dpvs) > 0:
        raise ResourceBusyError(
            'dvg', dvg_name, 'dpv', dpvs[0].dpv_name)

    dlvs = dvg \
        .dlvs \
        .with_entities(DistributeLogicalVolume.dlv_name) \
        .limit(1) \
        .all()
    if len(dlvs) > 0:
        raise ResourceBusyError(
            'dvg', dvg_name, 'dlv', dlvs[0].dlv_name)

    assert(dvg.total_size == 0)
    assert(dvg.free_size == 0)

    db.session.delete(dvg)
    db.session.commit()
    return None
