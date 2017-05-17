#!/usr/bin/env python

import random
import logging
from dlvm.utils.constant import dpv_search_overhead
from dlvm.utils.error import NoEnoughDpvError
from modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup
from handler import get_dm_context, \
    obt_refresh, obt_encode, \
    DpvClient


logger = logging.getLogger('dlvm_api')


def select_dpvs(dvg_name, required_size, batch_count, offset):
    logger.debug(
        'select_dpvs: %s %d %d %d',
        dvg_name, required_size, batch_count, offset)
    dpvs = DistributePhysicalVolume \
        .query \
        .filter_by(dvg_name=dvg_name) \
        .filter_by(status='available') \
        .filter(DistributePhysicalVolume.free_size > required_size) \
        .order_by(DistributePhysicalVolume.free_size.desc()) \
        .limit(batch_count) \
        .offset(offset) \
        .with_entities(DistributePhysicalVolume.dpv_name) \
        .all()
    random.shuffle(dpvs)
    return dpvs


def allocate_dpvs_for_group(group, dvg_name, obt, test_mode):
    dpvs = []
    dpv_name_set = set()
    batch_count = len(group.legs) * dpv_search_overhead
    i = -1
    total_leg_size = 0
    for leg in group.legs:
        i += 1
        if leg.dpv is not None:
            continue
        leg_size = leg.leg_size
        while True:
            if len(dpvs) == 0:
                if test_mode is True:
                    dpvs = select_dpvs(
                        dvg_name, leg_size, batch_count, 0)
                else:
                    dpvs = select_dpvs(
                        dvg_name, leg_size, batch_count, i)
            if len(dpvs) == 0:
                logger.warning(
                    'allocate dpvs failed, %s %d',
                    dvg_name, leg.leg_size)
                raise NoEnoughDpvError()
            dpv = dpvs.pop()
            if dpv.dpv_name in dpv_name_set:
                continue
            else:
                if test_mode is not True:
                    dpv_name_set.add(dpv.dpv_name)
            dpv = DistributePhysicalVolume \
                .query \
                .with_lockmode('update') \
                .filter_by(dpv_name=dpv.dpv_name) \
                .one()
            if dpv.status != 'available':
                continue
            if dpv.free_size < leg_size:
                continue
            dpv.free_size -= leg_size
            total_leg_size += leg_size
            leg.dpv = dpv
            db.session.add(dpv)
            db.session.add(leg)
            break

    dvg = DistributeVolumeGroup \
        .query \
        .with_lockmode('update') \
        .filter_by(dvg_name=dvg_name) \
        .one()
    assert(dvg.free_size >= total_leg_size)
    dvg.free_size -= total_leg_size
    db.session.add(dvg)
    obt_refresh(obt)
    db.session.commit()

    dm_context = get_dm_context()
    for leg in group.legs:
        dpv_name = leg.dpv_name
        client = DpvClient(dpv_name)
        client.leg_create(
            leg.leg_id,
            obt_encode(obt),
            str(leg.leg_size),
            dm_context,
        )


def free_dpvs_from_group(group, dvg_name, obt):
    dpv_dict = {}
    for leg in group.legs:
        dpv_name = leg.dpv_name
        if dpv_name is None:
            continue
        dpv = DistributePhysicalVolume \
            .query \
            .with_lockmode('update') \
            .filter_by(dpv_name=dpv_name) \
            .one()
        dpv_dict[dpv_name] = dpv
        if dpv.status == 'available':
            client = DpvClient(dpv_name)
            client.leg_delete(
                leg.leg_id,
                obt_encode(obt),
            )

    total_free_size = 0
    for leg in group.legs:
        leg_size = leg.leg_size
        dpv_name = leg.dpv_name
        db.session.delete(leg)
        if dpv_name is None:
            continue
        dpv = dpv_dict[dpv_name]
        if dpv.status == 'available':
            dpv.free_size += leg_size
            total_free_size += leg_size
            db.session.add(dpv)
    dvg = DistributeVolumeGroup \
        .query \
        .with_lockmode('update') \
        .filter_by(dvg_name=dvg_name) \
        .one()
    dvg.free_size += total_free_size
    db.session.add(dvg)
    db.session.delete(group)
