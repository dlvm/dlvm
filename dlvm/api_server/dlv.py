#!/usr/bin/env python

from collections import OrderedDict
import uuid
import logging
import random
import socket
import datetime
from xmlrpclib import Fault
from flask_restful import reqparse, Resource, fields, marshal
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.configure import conf
from dlvm.utils.constant import dpv_search_overhead
from dlvm.utils.error import NoEnoughDpvError, DpvError, \
    TransactionConflictError
from modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume, \
    Snapshot, Group, Leg
from handler import handle_dlvm_request, make_body, check_limit, \
    get_dm_context, div_round_up, dlv_get, \
    transaction_get, transaction_refresh, transaction_encode


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
dlv_summary_fields['partition_count'] = fields.Integer
dlv_summary_fields['status'] = fields.String
dlv_summary_fields['timestamp'] = fields.String
dlv_summary_fields['dvg_name'] = fields.String
dlv_summary_fields['host_name'] = fields.String
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
    'owner',
    type=str,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    'stage',
    type=int,
    required=True,
    location='json',
)


def select_dpvs(dvg_name, required_size, batch_count, offset):
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


def allocate_dpvs_for_group(group, dlv_name, dvg_name, transaction):
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
                dpvs = select_dpvs(
                    dvg_name, leg_size, batch_count, i)
            if len(dpvs) == 0:
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
    transaction_refresh(transaction)
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
                transaction_encode(transaction),
            )
        except socket.error, socket.timeout:
            logger.error('connect to dpv failed: %s', dpv_name)
            raise DpvError(dpv_name)
        except Fault as e:
            if 'TransactionConflict' in str(e):
                raise TransactionConflictError()
            else:
                logger.error('dpv rpc failed: %s', e)
                raise DpvError(dpv_name)


def dlv_create_new(dlv_name, t_id, owner):
    try:
        dlv, transaction = dlv_get(dlv_name, t_id, owner)
        for group in dlv.groups:
            allocate_dpvs_for_group(
                group, dlv.dlv_name, dlv.dvg_name, transaction)
    except DpvError as e:
        dlv.status = 'create_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        transaction_refresh(transaction)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    else:
        dlv.status = 'detached'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        transaction_refresh(transaction)
        db.session.commit()
        return make_body('success'), 200


def handle_dlvs_create_new(params, args):
    dlv_name = args['dlv_name']
    dlv_size = args['dlv_size']
    partition_count = args['partition_count']
    dvg_name = args['dvg_name']
    snap_name = '%s/base' % dlv_name
    t_id = args['t_id']
    owner = args['owner']
    stage = args['stage']
    init_size = dlv_size / conf.init_factor
    if init_size > conf.init_max:
        init_size = conf.init_max
    elif init_size < conf.init_min:
        init_size = conf.init_min
    transaction = transaction_get(t_id, owner, stage)

    dlv = DistributeLogicalVolume(
        dlv_name=dlv_name,
        dlv_size=dlv_size,
        partition_count=partition_count,
        status='creating',
        timestamp=datetime.datetime.utcnow(),
        first_attach=True,
        dvg_name=dvg_name,
        active_snap_name=snap_name,
        t_id=transaction.t_id,
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
        dlv_size, conf.block_size)
    group = Group(
        group_id=uuid.uuid4().hex,
        idx=0,
        group_size=thin_meta_size,
        nosync=True,
        dlv_name=dlv_name,
    )
    db.session.add(group)
    leg_size = thin_meta_size + conf.mirror_meta_size
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
        idx=1,
        group_size=group_size,
        nosync=True,
        dlv_name=dlv_name,
    )
    db.session.add(group)
    leg_size = div_round_up(
        group_size, partition_count) + conf.mirror_meta_size
    legs_per_group = 2 * partition_count
    for i in xrange(legs_per_group):
        leg = Leg(
            idx=i,
            group=group,
            leg_size=leg_size,
        )
        db.session.add(leg)
    transaction_refresh(transaction)
    db.session.commit()
    return dlv_create_new(dlv_name, t_id, owner)


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
