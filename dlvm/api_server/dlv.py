#!/usr/bin/env python

from collections import OrderedDict
import uuid
import logging
import random
import socket
import datetime
from xmlrpclib import Fault
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.configure import conf
from dlvm.utils.constant import dpv_search_overhead
from dlvm.utils.error import NoEnoughDpvError, DpvError, \
    ObtConflictError, DlvStatusError, HasFjError, \
    ThostError, SnapNameError, SnapshotStatusError
from modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume, \
    Snapshot, Group, Leg
from handler import handle_dlvm_request, make_body, check_limit, \
    get_dm_context, div_round_up, dlv_get, \
    obt_get, obt_refresh, obt_encode


logger = logging.getLogger('dlvm_api')


dlvs_get_parser = reqparse.RequestParser()
dlvs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
dlvs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.dlv_list_limit),
    default=conf.dlv_list_limit,
    location='args',
)
dlvs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'dlv_name',
        'dlv_size',
    ),
    default='dlv_name',
    location='args',
)
dlvs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
dlvs_get_parser.add_argument(
    'dvg_name',
    type=str,
    location='args',
)


dlv_summary_fields = OrderedDict()
dlv_summary_fields['dlv_name'] = fields.String
dlv_summary_fields['dlv_size'] = fields.Integer
dlv_summary_fields['data_size'] = fields.Integer
dlv_summary_fields['partition_count'] = fields.Integer
dlv_summary_fields['status'] = fields.String
dlv_summary_fields['timestamp'] = fields.String
dlv_summary_fields['dvg_name'] = fields.String
dlv_summary_fields['thost_name'] = fields.String
dlv_summary_fields['active_snap_name'] = fields.String
dlv_summary_fields['t_id'] = fields.String
dlvs_get_fields = OrderedDict()
dlvs_get_fields['dlvs'] = fields.List(fields.Nested(dlv_summary_fields))


def handle_dlvs_get(params, args):
    order_field = getattr(DistributeLogicalVolume, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = DistributeLogicalVolume.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = DistributeLogicalVolume.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['dvg_name']:
        query = query.filter_by(dvg_name=args['dvg_name'])
    query = query.limit(args['limit'])
    dlvs = query.all()
    body = marshal({'dlvs': dlvs}, dlvs_get_fields)
    return body['dlvs'], 200


dlvs_post_parser = reqparse.RequestParser()
dlvs_post_parser.add_argument(
    'dlv_name',
    type=str,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    'dlv_size',
    type=int,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    'partition_count',
    type=int,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    'src_dlv_name',
    type=str,
    location='json',
)
dlvs_post_parser.add_argument(
    'dvg_name',
    type=str,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


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


def allocate_dpvs_for_group(group, dlv_name, dvg_name, obt):
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
                if conf.cross_dpv is True:
                    dpvs = select_dpvs(
                        dvg_name, leg_size, batch_count, i)
                else:
                    dpvs = select_dpvs(
                        dvg_name, leg_size, batch_count, 0)
            if len(dpvs) == 0:
                logger.warning(
                    'allocate dpvs failed, %s %s %d',
                    dlv_name, dvg_name, leg.leg_size)
                raise NoEnoughDpvError()
            dpv = dpvs.pop()
            if dpv.dpv_name in dpv_name_set:
                continue
            else:
                if conf.cross_dpv is True:
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
        try:
            client = WrapperRpcClient(
                str(dpv_name),
                conf.dpv_port,
                conf.dpv_timeout,
            )
            client.leg_create(
                leg.leg_id,
                leg.leg_size,
                dm_context,
                obt_encode(obt),
            )
        except socket.error, socket.timeout:
            logger.error('connect to dpv failed: %s', dpv_name)
            raise DpvError(dpv_name)
        except Fault as e:
            if 'ObtConflict' in str(e):
                raise ObtConflictError()
            else:
                logger.error('dpv rpc failed: %s', e)
                raise DpvError(dpv_name)


def dlv_create_new(dlv_name, t_id, t_owner, t_stage):
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        for group in dlv.groups:
            allocate_dpvs_for_group(
                group, dlv.dlv_name, dlv.dvg_name, obt)
    except DpvError as e:
        dlv.status = 'create_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    except NoEnoughDpvError:
        dlv.status = 'create_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', 'no_enough_dpv'), 500
    else:
        dlv.status = 'detached'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def handle_dlvs_create_new(params, args):
    dlv_name = args['dlv_name']
    dlv_size = args['dlv_size']
    partition_count = args['partition_count']
    dvg_name = args['dvg_name']
    snap_name = '%s/base' % dlv_name
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    init_size = dlv_size / conf.init_factor
    if init_size > conf.init_max:
        init_size = conf.init_max
    elif init_size < conf.init_min:
        init_size = conf.init_min
    obt = obt_get(t_id, t_owner, t_stage)

    dlv = DistributeLogicalVolume(
        dlv_name=dlv_name,
        dlv_size=dlv_size,
        data_size=init_size,
        partition_count=partition_count,
        status='creating',
        timestamp=datetime.datetime.utcnow(),
        dvg_name=dvg_name,
        active_snap_name=snap_name,
        t_id=obt.t_id,
    )
    db.session.add(dlv)
    snapshot = Snapshot(
        snap_name=snap_name,
        timestamp=datetime.datetime.utcnow(),
        thin_id=0,
        ori_thin_id=0,
        status='available',
        dlv_name=dlv_name,
    )
    db.session.add(snapshot)
    thin_meta_size = conf.thin_meta_factor * div_round_up(
        dlv_size, conf.thin_block_size)
    if thin_meta_size < conf.thin_meta_min:
        thin_meta_size = conf.thin_meta_min
    group = Group(
        group_id=uuid.uuid4().hex,
        idx=0,
        group_size=thin_meta_size,
        dlv_name=dlv_name,
    )
    db.session.add(group)
    leg_size = thin_meta_size + conf.mirror_meta_size
    leg_size = div_round_up(leg_size, conf.lvm_unit) * conf.lvm_unit
    for i in xrange(2):
        leg = Leg(
            leg_id=uuid.uuid4().hex,
            idx=i,
            group=group,
            leg_size=leg_size,
        )
        db.session.add(leg)
    group_size = init_size
    group = Group(
        group_id=uuid.uuid4().hex,
        idx=1,
        group_size=group_size,
        dlv_name=dlv_name,
    )
    db.session.add(group)
    leg_size = div_round_up(
        group_size, partition_count) + conf.mirror_meta_size
    leg_size = div_round_up(leg_size, conf.lvm_unit) * conf.lvm_unit
    legs_per_group = 2 * partition_count
    for i in xrange(legs_per_group):
        leg = Leg(
            leg_id=uuid.uuid4().hex,
            idx=i,
            group=group,
            leg_size=leg_size,
        )
        db.session.add(leg)
    obt_refresh(obt)
    db.session.commit()
    return dlv_create_new(dlv_name, t_id, t_owner, t_stage)


def handle_dlvs_clone(params, args):
    pass


def handle_dlvs_post(params, args):
    if args['src_dlv_name'] is None:
        return handle_dlvs_create_new(params, args)
    else:
        return handle_dlvs_clone(params, args)


class Dlvs(Resource):

    def get(self):
        return handle_dlvm_request(None, dlvs_get_parser, handle_dlvs_get)

    def post(self):
        return handle_dlvm_request(None, dlvs_post_parser, handle_dlvs_post)


dlv_delete_parser = reqparse.RequestParser()
dlv_delete_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
dlv_delete_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
dlv_delete_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


CAN_DELETE_STATUS = (
    'detached',
    'create_failed',
    'delete_failed',
    'deleting',
    'creating',
)


def free_dpvs_from_group(group, dlv_name, dvg_name, obt):
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
        if dpv.status != 'available':
            continue
        try:
            client = WrapperRpcClient(
                str(dpv_name),
                conf.dpv_port,
                conf.dpv_timeout,
            )
            client.leg_delete(
                leg.leg_id,
                obt_encode(obt),
            )
        except socket.error, socket.timeout:
            logger.error('connect to dpv failed: %s', dpv_name)
            raise DpvError(dpv_name)
        except Fault as e:
            if 'ObtConflict' in str(e):
                raise ObtConflictError()
            else:
                logger.error('dpv rpc failed: %s', e)
                raise DpvError(dpv_name)

    total_free_size = 0
    for leg in group.legs:
        leg_size = leg.leg_size
        dpv_name = leg.dpv_name
        db.session.delete(leg)
        if dpv_name is None:
            continue
        dpv = dpv_dict[dpv_name]
        if dpv.status != 'available':
            continue
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


def dlv_delete(dlv, obt):
    if dlv.status not in CAN_DELETE_STATUS:
        raise DlvStatusError(dlv.status)
    if len(dlv.fjs) != 0:
        raise HasFjError()
    dlv.status = 'deleting'
    dlv.timestamp = datetime.datetime.utcnow()
    obt_refresh(obt)
    db.session.commit()
    for group in dlv.groups:
        free_dpvs_from_group(group, dlv.dlv_name, dlv.dvg_name, obt)
    for snapshot in dlv.snapshots:
        db.session.delete(snapshot)
    db.session.delete(dlv)
    obt_refresh(obt)
    db.session.commit()


def handle_dlv_delete(params, args):
    dlv_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        dlv_delete(dlv, obt)
    except DpvError as e:
        dlv.status = 'delete_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    except HasFjError:
        return make_body('has_fj'), 400
    else:
        return make_body('success'), 200


dlv_put_parser = reqparse.RequestParser()
dlv_put_parser.add_argument(
    'action',
    type=str,
    choices=('attach', 'detach', 'set_active'),
    required=True,
    location='json',
)
dlv_put_parser.add_argument(
    'thost_name',
    type=str,
    location='json',
)
dlv_put_parser.add_argument(
    'snap_name',
    type=str,
    location='json',
)
dlv_put_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
dlv_put_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
dlv_put_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def do_attach(dlv, obt):
    dlv_info = {}
    dlv_info['dlv_name'] = dlv.dlv_name
    dlv_info['dlv_size'] = dlv.dlv_size
    dm_context = get_dm_context()
    dm_context['stripe_number'] = dlv.partition_count
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
            dpv_name = leg.dpv_name
            dpv = DistributePhysicalVolume \
                .query \
                .with_lockmode('update') \
                .filter_by(dpv_name=dpv_name) \
                .one()
            if dpv.status != 'available':
                continue
            try:
                client = WrapperRpcClient(
                    str(dpv_name),
                    conf.dpv_port,
                    conf.dpv_timeout,
                )
                client.leg_export(
                    leg.leg_id,
                    dlv.thost_name,
                    obt_encode(obt),
                )
            except socket.error, socket.timeout:
                logger.error('connect to dpv failed: %s', dpv_name)
                continue
            except Fault as e:
                if 'ObtConflict' in str(e):
                    raise ObtConflictError()
                else:
                    logger.error('dpv rpc failed: %s', e)
                    continue
        dlv_info['groups'].append(igroup)

    try:
        client = WrapperRpcClient(
            str(dlv.thost_name),
            conf.thost_port,
            conf.thost_timeout,
        )
        client.dlv_aggregate(
            dlv.dlv_name,
            obt_encode(obt),
            dlv_info,
        )
    except socket.error, socket.timeout:
        logger.error('connect to thost failed: %s', dlv.thost_name)
        raise ThostError(dlv.thost_name)
    except Fault as e:
        if 'ObtConflict' in str(e):
            raise ObtConflictError()
        else:
            logger.error('thost rpc failed: %s', e)
            raise ThostError(dlv.thost_name)


def dlv_attach(dlv, thost_name, obt):
    if dlv.status != 'detached':
        raise DlvStatusError(dlv.status)
    dlv.status = 'attaching'
    dlv.timestamp = datetime.datetime.utcnow()
    dlv.thost_name = thost_name
    db.session.add(dlv)
    obt_refresh(obt)
    db.session.commit()
    return do_attach(dlv, obt)


def handle_dlv_attach(dlv_name, thost_name, t_id, t_owner, t_stage):
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        dlv_attach(dlv, thost_name, obt)
    except ThostError as e:
        dlv.status = 'attach_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('thost_failed', e.message), 500
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    else:
        dlv.status = 'attached'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        for group in dlv.groups:
            db.session.add(group)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


CAN_DETACH_STATUS = (
    'attached',
    'attach_failed',
    'detach_failed',
)


def do_detach(dlv, obt):
    dlv_info = {}
    dlv_info['dlv_name'] = dlv.dlv_name
    dm_context = get_dm_context()
    dm_context['stripe_number'] = dlv.partition_count
    dlv_info['dm_context'] = dm_context
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
        igroup['legs'] = []
        for leg in group.legs:
            ileg = {}
            ileg['dpv_name'] = leg.dpv_name
            ileg['leg_id'] = leg.leg_id
            ileg['idx'] = leg.idx
            igroup['legs'].append(ileg)
        dlv_info['groups'].append(igroup)

    try:
        client = WrapperRpcClient(
            str(dlv.thost_name),
            conf.thost_port,
            conf.thost_timeout,
        )
        client.dlv_degregate(
            dlv.dlv_name,
            obt_encode(obt),
            dlv_info,
        )
    except socket.error, socket.timeout:
        logger.error('connect to thost failed: %s', dlv.thost_name)
        raise ThostError(dlv.thost_name)
    except Fault as e:
        if 'ObtConflict' in str(e):
            raise ObtConflictError()
        else:
            logger.error('thost rpc failed: %s', e)
            raise ThostError(dlv.thost_name)

    for group in dlv.groups:
        for leg in group.legs:
            dpv_name = leg.dpv_name
            dpv = DistributePhysicalVolume \
                .query \
                .with_lockmode('update') \
                .filter_by(dpv_name=dpv_name) \
                .one()
            if dpv.status != 'available':
                continue
            try:
                client = WrapperRpcClient(
                    str(dpv_name),
                    conf.dpv_port,
                    conf.dpv_timeout,
                )
                client.leg_unexport(
                    leg.leg_id,
                    dlv.thost_name,
                    obt_encode(obt),
                )
            except socket.error, socket.timeout:
                logger.error('connect to dpv failed: %s', dpv_name)
                raise DpvError(dlv.thost_name)
            except Fault as e:
                if 'ObtConflict' in str(e):
                    raise ObtConflictError()
                else:
                    raise DpvError(dlv.thost_name)


def dlv_detach(dlv, obt):
    if dlv.status not in CAN_DETACH_STATUS:
        raise DlvStatusError(dlv.status)
    dlv.status = 'detaching'
    dlv.timestamp = datetime.datetime.utcnow()
    db.session.add(dlv)
    obt_refresh(obt)
    db.session.commit()
    return do_detach(dlv, obt)


def handle_dlv_detach(dlv_name, t_id, t_owner, t_stage):
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        dlv_detach(dlv, obt)
    except DpvError as e:
        dlv.status = 'attach_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    except ThostError as e:
        dlv.status = 'attach_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('thost_failed', e.message), 500
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    else:
        dlv.status = 'detached'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def dlv_set_active(dlv, snap_name, obt):
    full_snap_name = '{dlv_name}/{snap_name}'.format(
        dlv_name=dlv.dlv_name, snap_name=snap_name)
    if dlv.status != 'detached':
        raise DlvStatusError(dlv.status)
    try:
        snapshot = Snapshot \
            .query \
            .filter_by(snap_name=full_snap_name) \
            .one()
    except NoResultFound:
        raise SnapNameError(full_snap_name)
    if snapshot.status != 'available':
        raise SnapshotStatusError(snapshot.status)
    dlv.active_snap_name = full_snap_name
    db.session.add(dlv)
    obt_refresh(obt)
    db.session.commit()


def handle_dlv_set_active(dlv_name, snap_name, t_id, t_owner, t_stage):
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        dlv_set_active(dlv, snap_name, obt)
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    except SnapNameError as e:
        return make_body('invalid_snap_name', e.message), 400
    except SnapshotStatusError as e:
        return make_body('invalid_snap_status', e.message), 400
    else:
        return make_body('success'), 200


def handle_dlv_put(params, args):
    dlv_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    if args['action'] == 'attach':
        if 'thost_name' not in args:
            return make_body('no_thost_name'), 400
        else:
            return handle_dlv_attach(
                dlv_name, args['thost_name'], t_id, t_owner, t_stage)
    elif args['action'] == 'detach':
        return handle_dlv_detach(
            dlv_name, t_id, t_owner, t_stage)
    elif args['action'] == 'set_active':
        if 'snap_name' not in args:
            return make_body('no_snap_name'), 400
        else:
            return handle_dlv_set_active(
                dlv_name, args['snap_name'], t_id, t_owner, t_stage)
    else:
        assert(False)


class Dlv(Resource):

    def put(self, dlv_name):
        return handle_dlvm_request(
            [dlv_name],
            dlv_put_parser,
            handle_dlv_put,
        )

    def delete(self, dlv_name):
        return handle_dlvm_request(
            [dlv_name],
            dlv_delete_parser,
            handle_dlv_delete,
        )
