#!/usr/bin/env python

from collections import OrderedDict
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.rpc_wrapper import WrapperRpcClient
from dlvm.utils.configure import conf
from modules import db, DistributePhysicalVolume
from handler import handle_dlvm_request, make_body, check_limit

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
    dpv_info = client.get_dpv_info(None)
    dpv = DistributePhysicalVolume(
        dpv_name=args['dpv_name'],
        total_size=dpv_info['total_size'],
        free_size=dpv_info['free_size'],
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
group_fields['snap_name'] = fields.String

leg_fields = OrderedDict()
leg_fields['leg_id'] = fields.String
leg_fields['idx'] = fields.Integer
leg_fields['thin_id'] = fields.Integer
leg_fields['group'] = fields.Nested(group_fields)

dpv_fields = OrderedDict()
dpv_fields['dpv_name'] = fields.String
dpv_fields['total_size'] = fields.Integer
dpv_fields['free_size'] = fields.Integer
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
        return make_body('not_exist', 404)
    else:
        if dpv.dvg:
            return make_body('dpv_busy', dpv.dvg.dvg_name), 403
        db.session.delete(dpv)
        db.session.commit()
        return make_body('success'), 200


class Dpv(Resource):

    def get(self, dpv_name):
        return handle_dlvm_request([dpv_name], None, handle_dpv_get)

    def delete(self, dpv_name):
        return handle_dlvm_request([dpv_name], None, handle_dpv_delete)
