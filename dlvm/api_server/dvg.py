#!/usr/bin/env python

from collections import OrderedDict
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from dlvm.utils.configure import conf
from modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume
from handler import handle_dlvm_request, make_body, check_limit


dvgs_get_parser = reqparse.RequestParser()
dvgs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
dvgs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.dvg_list_limit),
    default=conf.dvg_list_limit,
    location='args',
)
dvgs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'dvg_name',
        'total_size',
        'free_size',
    ),
    default='dvg_name',
    location='args',
)
dvgs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)

dvg_summary_fields = OrderedDict()
dvg_summary_fields['dvg_name'] = fields.String
dvg_summary_fields['total_size'] = fields.Integer
dvg_summary_fields['free_size'] = fields.Integer
dvgs_get_fields = OrderedDict()
dvgs_get_fields['dvgs'] = fields.List(fields.Nested(dvg_summary_fields))


def handle_dvgs_get(params, args):
    order_field = getattr(DistributeVolumeGroup, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = DistributeVolumeGroup.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = DistributeVolumeGroup.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    query = query.limit(args['limit'])
    dvgs = query.all()
    body = marshal({'dvgs': dvgs}, dvgs_get_fields)
    return body['dvgs'], 200


dvgs_post_parser = reqparse.RequestParser()
dvgs_post_parser.add_argument(
    'dvg_name',
    type=str,
    required=True,
    location='json',
)


def handle_dvgs_post(params, args):
    dvg = DistributeVolumeGroup(
        dvg_name=args['dvg_name'],
        total_size=0,
        free_size=0,
    )
    db.session.add(dvg)
    db.session.commit()
    return make_body('success'), 200


class Dvgs(Resource):

    def get(self):
        return handle_dlvm_request(None, dvgs_get_parser, handle_dvgs_get)

    def post(self):
        return handle_dlvm_request(None, dvgs_post_parser, handle_dvgs_post)
