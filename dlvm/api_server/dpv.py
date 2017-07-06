#!/usr/bin/env python

from collections import OrderedDict
import logging
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.configure import conf
from dlvm.utils.error import ObtConflictError, DpvError
from modules import db, DistributePhysicalVolume, DistributeVolumeGroup
from handler import handle_dlvm_request, make_body, check_limit, \
    obt_get, obt_refresh, obt_encode, DpvClient, get_dm_context


logger = logging.getLogger('dlvm_api')


dpvs_get_parser = reqparse.RequestParser()
dpvs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
dpvs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.dpv_list_limit),
    default=conf.dpv_list_limit,
    location='args',
)
dpvs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'dpv_name',
        'total_size', 'free_size',
    ),
    default='dpv_name',
    location='args',
)
dpvs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
dpvs_get_parser.add_argument(
    'status',
    type=str,
    choices=('available', 'unavailable'),
    location='args',
)
dpvs_get_parser.add_argument(
    'dvg_name',
    type=str,
    location='args',
)


dpv_summary_fields = OrderedDict()
dpv_summary_fields['dpv_name'] = fields.String
dpv_summary_fields['total_size'] = fields.Integer
dpv_summary_fields['free_size'] = fields.Integer
# dpv_summary_fields['in_sync'] = fields.Boolean
dpv_summary_fields['status'] = fields.String
dpv_summary_fields['dvg_name'] = fields.String
dpvs_get_fields = OrderedDict()
dpvs_get_fields['dpvs'] = fields.List(fields.Nested(dpv_summary_fields))


def handle_dpvs_get(params, args):
    order_field = getattr(DistributePhysicalVolume, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = DistributePhysicalVolume.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = DistributePhysicalVolume.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['status']:
        query = query.filter_by(status=args['status'])
    if args['dvg_name']:
        query = query.filter_by(dvg_name=args['dvg_name'])
    query = query.limit(args['limit'])
    dpvs = query.all()
    body = marshal({'dpvs': dpvs}, dpvs_get_fields)
    return body['dpvs'], 200


dpvs_post_parser = reqparse.RequestParser()
dpvs_post_parser.add_argument(
    'dpv_name',
    type=str,
    required=True,
    location='json',
)


def handle_dpvs_post(params, args):
    client = WrapperRpcClient(
        args['dpv_name'], conf.dpv_port, conf.dpv_timeout)
    dpv_info = client.dpv_get_info()
    dpv = DistributePhysicalVolume(
        dpv_name=args['dpv_name'],
        total_size=int(dpv_info['total_size']),
        free_size=int(dpv_info['free_size']),
        in_sync=True,
        status='available',
        timestamp=datetime.datetime.utcnow(),
    )
    db.session.add(dpv)
    try:
        db.session.commit()
    except IntegrityError:
        return make_body('duplicate_dpv'), 400
    else:
        return make_body('success'), 200


class Dpvs(Resource):

    def get(self):
        return handle_dlvm_request(None, dpvs_get_parser, handle_dpvs_get)

    def post(self):
        return handle_dlvm_request(None, dpvs_post_parser, handle_dpvs_post)


group_fields = OrderedDict()
group_fields['group_id'] = fields.String
group_fields['idx'] = fields.Integer
group_fields['group_size'] = fields.Integer
group_fields['dlv_name'] = fields.String

leg_fields = OrderedDict()
leg_fields['leg_id'] = fields.String
leg_fields['idx'] = fields.Integer
leg_fields['group'] = fields.Nested(group_fields)

dpv_fields = OrderedDict()
dpv_fields['dpv_name'] = fields.String
dpv_fields['total_size'] = fields.Integer
dpv_fields['free_size'] = fields.Integer
# dpv_fields['in_sync'] = fields.Boolean
dpv_fields['status'] = fields.String
dpv_fields['dvg_name'] = fields.String
dpv_fields['legs'] = fields.List(fields.Nested(leg_fields))


def handle_dpv_get(params, args):
    dpv_name = params[0]
    try:
        dpv = DistributePhysicalVolume \
            .query \
            .with_lockmode('update') \
            .filter_by(dpv_name=dpv_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    return marshal(dpv, dpv_fields), 200


def handle_dpv_delete(params, args):
    dpv_name = params[0]
    try:
        dpv = DistributePhysicalVolume \
            .query \
            .with_lockmode('update') \
            .filter_by(dpv_name=dpv_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    else:
        if dpv.dvg:
            return make_body('dpv_busy', dpv.dvg.dvg_name), 403
        db.session.delete(dpv)
        db.session.commit()
        return make_body('success'), 200


dpv_put_parser = reqparse.RequestParser()
dpv_put_parser.add_argument(
    'action',
    type=str,
    choices=('set_available', 'set_unavailable'),
    required=True,
    location='json',
)
dpv_put_parser.add_argument(
    't_id',
    type=str,
    location='json',
)
dpv_put_parser.add_argument(
    't_owner',
    type=str,
    location='json',
)
dpv_put_parser.add_argument(
    't_stage',
    type=int,
    location='json',
)


def handle_dpv_availalbe(dpv_name, t_id, t_owner, t_stage):
    obt = obt_get(t_id, t_owner, t_stage)
    dpv = DistributePhysicalVolume \
        .query \
        .with_lockmode('update') \
        .filter_by(dpv_name=dpv_name) \
        .one()
    if dpv.status != 'unavailable':
        return make_body('invalid_dpv_status', dpv.status), 400
    dpv_info = []
    for leg in dpv.legs:
        if leg.group is not None and leg.group.dlv is not None:
            skip = False
        else:
            skip = True
        if skip is True:
            continue
        dlv = leg.group.dlv
        if dlv.obt is not None and dlv.obt.t_id != obt.t_id:
            logger.error(
                'conflict obt: %s %s %s %s',
                dlv.obt.t_id,
                dlv.obt.t_owner,
                dlv.obt.t_stage,
                dlv.obt.annotation,
            )
            raise ObtConflictError()
        else:
            dlv.obt = obt
            db.session.add(dlv)
        leg_info = {
            'leg_id': leg.leg_id,
            'leg_size': str(leg.leg_size),
            'dm_context': get_dm_context(),
            'ihost_name': dlv.ihost_name,
        }
        dpv_info.append(leg_info)
    obt_refresh(obt)
    db.session.commit()
    dpv_client = DpvClient(dpv_name)
    try:
        dpv_size_info = dpv_client.dpv_sync(dpv_info, obt_encode(obt))
    except DpvError as e:
        return make_body('dpv_failed', e.message), 500
    else:
        total_size = int(dpv_size_info['total_size'])
        free_size = int(dpv_size_info['free_size'])
        if dpv.dvg_name is not None:
            dvg = DistributeVolumeGroup \
                .query \
                .with_lockmode('update') \
                .filter_by(dvg_name=dpv.dvg_name) \
                .one()
            dvg.total_size = dvg.total_size - dpv.total_size + total_size
            dvg.free_size = dvg.free_size - dpv.free_size + free_size
            db.session.add(dvg)
        dpv.total_size = total_size
        dpv.free_size = free_size
        dpv.status = 'available'
        db.session.add(dpv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def handle_dpv_unavailable(dpv_name):
    dpv = DistributePhysicalVolume \
        .query \
        .with_lockmode('update') \
        .filter_by(dpv_name=dpv_name) \
        .one()
    dpv.status = 'unavailable'
    db.session.add(dpv)
    db.session.commit()
    return make_body('success'), 200


def handle_dpv_put(params, args):
    dpv_name = params[0]
    if args['action'] == 'set_available':
        t_id = args.get('t_id')
        if t_id is None:
            return make_body('no_t_id'), 400
        t_owner = args.get('t_owner')
        if t_owner is None:
            return make_body('no_t_owner'), 400
        t_stage = args.get('t_stage')
        if t_stage is None:
            return make_body('no_t_stage'), 400
        return handle_dpv_availalbe(
            dpv_name, t_id, t_owner, t_stage)
    elif args['action'] == 'set_unavailable':
        return handle_dpv_unavailable(dpv_name)
    else:
        assert(False)


class Dpv(Resource):

    def get(self, dpv_name):
        return handle_dlvm_request([dpv_name], None, handle_dpv_get)

    def put(self, dpv_name):
        return handle_dlvm_request([dpv_name], dpv_put_parser, handle_dpv_put)

    def delete(self, dpv_name):
        return handle_dlvm_request([dpv_name], None, handle_dpv_delete)
