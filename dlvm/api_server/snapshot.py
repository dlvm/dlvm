#!/usr/bin/env python

from collections import OrderedDict
import logging
import random
import socket
import datetime
from xmlrpclib import Fault
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.configure import conf
from dlvm.utils.error import ObtConflictError, \
    DlvStatusError, IhostError, ThinMaxRetryError, \
    SnapshotStatusError, DeleteActiveSnapshotError
from dlvm.utils.constant import max_thin_id, max_thin_id_retry
from modules import db, Snapshot
from handler import handle_dlvm_request, make_body, check_limit, \
    dlv_get, obt_refresh, obt_encode


logger = logging.getLogger('dlvm_api')


class SnapName(fields.Raw):
    def format(self, value):
        return value.split('/')[1]


snapshot_summary_fields = OrderedDict()
snapshot_summary_fields['snap_name'] = SnapName(attribute='snap_name')
snapshot_summary_fields['timestamp'] = fields.DateTime
snapshot_summary_fields['thin_id'] = fields.Integer
snapshot_summary_fields['ori_thin_id'] = fields.Integer
snapshot_summary_fields['status'] = fields.String
snapshots_get_fields = OrderedDict()
snapshots_get_fields['snapshots'] = fields.List(
    fields.Nested(snapshot_summary_fields))

snapshots_get_parser = reqparse.RequestParser()
snapshots_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
snapshots_get_parser.add_argument(
    'limit',
    type=check_limit(conf.snapshot_list_limit),
    default=conf.snapshot_list_limit,
    location='args',
)
snapshots_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'snap_name',
        'timestamp',
        'thin_id',
        'ori_thin_id',
    ),
    default='snap_name',
    location='args',
)
snapshots_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
snapshots_get_parser.add_argument(
    'status',
    type=str,
    choices=(
        'creating',
        'create_failed',
        'available',
        'deleting',
        'delete_failed',
    ),
    location='args',
)


def handle_snapshots_get(params, args):
    dlv_name = params[0]
    order_field = getattr(Snapshot, args['order_by'])
    prev = args['prev']
    query = Snapshot.query.filter_by(dlv_name=dlv_name)
    if args['reverse'] == 'true':
        query = query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['status']:
        query = query.filter_by(status=args['status'])
    query = query.limit(args['limit'])
    snapshots = query.all()
    body = marshal({'snapshots': snapshots}, snapshots_get_fields)
    return body['snapshots'], 200


def convert_snap_pairs(val):
    snap_pairs = []
    pairs = val.split(',')
    for pair in pairs:
        snap_name, ori_snap_name = pair.split(':')
        snap_pair = {
            'snap_name': snap_name,
            'ori_snap_name': ori_snap_name,
        }
        snap_pairs.append(snap_pair)
    return snap_pairs


snapshots_post_parser = reqparse.RequestParser()
snapshots_post_parser.add_argument(
    'snap_pairs',
    type=convert_snap_pairs,
    required=True,
    location='json',
)
snapshots_post_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
snapshots_post_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
snapshots_post_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def snapshot_create(dlv, snap_name, ori_snap_name):
    retry_count = 0
    thin_id = random.randint(1, max_thin_id)
    while retry_count < max_thin_id_retry:
        try:
            snapshot = Snapshot \
                .query \
                .with_entities(Snapshot.snap_name) \
                .filter_by(thin_id=thin_id) \
                .filter_by(dlv_name=dlv.dlv_name) \
                .one()
        except NoResultFound:
            break
        retry_count += 1
        thin_id = random.randint(1, max_thin_id)
    if retry_count >= max_thin_id_retry:
        raise ThinMaxRetryError()
    ori_snapshot = Snapshot \
        .query \
        .with_entities(Snapshot.thin_id) \
        .filter_by(snap_name=ori_snap_name) \
        .one()
    snapshot = Snapshot(
        snap_name=snap_name,
        timestamp=datetime.datetime.utcnow(),
        thin_id=thin_id,
        ori_thin_id=ori_snapshot.thin_id,
        status='creating',
        dlv_name=dlv.dlv_name,
    )
    db.session.add(snapshot)
    return snapshot


def snapshots_create(dlv, snap_pairs, obt):
    if dlv.status != 'attached':
        raise DlvStatusError(dlv.status)
    snapshots = []
    for snap_pair in snap_pairs:
        snap_name = '%s/%s' % (dlv.dlv_name, snap_pair['snap_name'])
        ori_snap_name = '%s/%s' % (dlv.dlv_name, snap_pair['ori_snap_name'])
        snapshot = snapshot_create(dlv, snap_name, ori_snap_name)
        db.session.add(snapshot)
        snapshots.append(snapshot)
    obt_refresh(obt)
    db.session.commit()
    return snapshots


def snapshots_take(dlv, snapshots, obt):
    try:
        client = WrapperRpcClient(
            str(dlv.ihost_name),
            conf.ihost_port,
            conf.ihost_timeout,
        )
        for snapshot in snapshots:
            client.snapshot_create(
                dlv.dlv_name,
                obt_encode(obt),
                snapshot.thin_id,
                snapshot.ori_thin_id,
            )
    except socket.error, socket.timeout:
        logger.error('connect to ihost failed: %s', dlv.ihost_name)
        raise IhostError(dlv.ihost_name)
    except Fault as e:
        if 'ObtConflict' in str(e):
            raise ObtConflictError()
        else:
            logger.error('ihost rpc failed: %s', e)
            raise IhostError(dlv.ihost_name)


def snapshots_complete(dlv, snapshots, status, obt):
    for snapshot in snapshots:
        snapshot.status = status
        db.session.add(snapshot)
    obt_refresh(obt)
    db.session.commit()


def handle_snapshots_post(params, args):
    dlv_name = params[0]
    snap_pairs = args['snap_pairs']
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        snapshots = snapshots_create(dlv, snap_pairs, obt)
        snapshots_take(dlv, snapshots, obt)
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    except SnapshotStatusError as e:
        return make_body('invalid_snapshot_status', e.message), 400
    except IhostError as e:
        snapshots_complete(dlv, snapshots, 'create_failed', obt)
        return make_body('ihost_failed', e.message), 500
    else:
        snapshots_complete(dlv, snapshots, 'available', obt)
        return make_body('success'), 200


class Snaps(Resource):

    def get(self, dlv_name):
        return handle_dlvm_request(
            [dlv_name], snapshots_get_parser, handle_snapshots_get)

    def post(self, dlv_name):
        return handle_dlvm_request(
            [dlv_name], snapshots_post_parser, handle_snapshots_post)


CAN_DELETE_STATUS = (
    'available',
    'deleting',
    'delete_failed',
    'create_failed',
)


def get_snapshot_by_name(snap_name):
    snapshot = Snapshot \
        .query \
        .filter_by(snap_name=snap_name) \
        .one()
    return snapshot


def snapshot_delete(dlv, snapshot, obt):
    if dlv.status != 'attached':
        raise DlvStatusError(dlv.status)
    if snapshot.status not in CAN_DELETE_STATUS:
        raise SnapshotStatusError(snapshot.status)
    if dlv.active_snap_name == snapshot.snap_name:
        raise DeleteActiveSnapshotError()
    snapshot.status = 'deleting'
    db.session.add(snapshot)
    obt_refresh(obt)
    db.session.commit()
    try:
        client = WrapperRpcClient(
            str(dlv.ihost_name),
            conf.ihost_port,
            conf.ihost_timeout,
        )
        client.snapshot_delete(
            dlv.dlv_name,
            obt_encode(obt),
            snapshot.thin_id,
        )
    except socket.error, socket.timeout:
        logger.error('connect to ihost failed: %s', dlv.ihost_name)
        raise IhostError(dlv.ihost_name)
    except Fault as e:
        if 'ObtConflict' in str(e):
            raise ObtConflictError()
        else:
            logger.error('ihost rpc failed: %s', e)
            raise IhostError(dlv.ihost_name)


snapshot_delete_parser = reqparse.RequestParser()
snapshot_delete_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
snapshot_delete_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
snapshot_delete_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def handle_snapshot_delete(params, args):
    dlv_name = params[0]
    snap_name = '%s/%s' % (dlv_name, params[1])
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        snapshot = get_snapshot_by_name(snap_name)
        snapshot_delete(dlv, snapshot, obt)
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    except SnapshotStatusError as e:
        return make_body('invalid_snapshot_status', e.message), 400
    except DeleteActiveSnapshotError:
        return make_body('can_not_delete_active_snapshot'), 400
    except IhostError as e:
        snapshot.status = 'create_failed'
        db.session.add(snapshot)
        obt_refresh(obt)
        db.session.commit()
        return make_body('ihost_failed', e.message), 500
    else:
        db.session.delete(snapshot)
        obt_refresh(obt)
        db.session.commit()
        return make_body('succes'), 200


snapshot_fields = OrderedDict()
snapshot_fields['snap_name'] = SnapName(attribute='snap_name')
snapshot_fields['timestamp'] = fields.DateTime
snapshot_fields['thin_id'] = fields.Integer
snapshot_fields['ori_thin_id'] = fields.Integer
snapshot_fields['status'] = fields.String


def handle_snapshot_get(params, args):
    dlv_name = params[0]
    snap_name = '%s/%s' % (dlv_name, params[1])
    try:
        snapshot = Snapshot \
            .query \
            .filter_by(snap_name=snap_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    return marshal(snapshot, snapshot_fields), 200


class Snap(Resource):

    def get(self, dlv_name, snap_name):
        return handle_dlvm_request(
            [dlv_name, snap_name],
            None,
            handle_snapshot_get,
        )

    def delete(self, dlv_name, snap_name):
        return handle_dlvm_request(
            [dlv_name, snap_name],
            snapshot_delete_parser,
            handle_snapshot_delete,
        )
