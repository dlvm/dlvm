#!/usr/bin/env python

import uuid
from collections import OrderedDict
import logging
import random
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from dlvm.utils.constant import dpv_search_overhead
from dlvm.utils.error import NoEnoughDpvError, DpvError, \
    DlvStatusError, \
    IhostError, FjStatusError, \
    DependenceCheckError
from modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, \
    Leg, CloneJob
from handler import handle_dlvm_request, make_body, check_limit, \
    get_dm_context, dlv_get, \
    DpvClient, IhostClient, \
    obt_refresh, obt_encode, \
    dlv_detach_register, dlv_delete_register, \
    get_dlv_info


logger = logging.getLogger('dlvm_api')


cj_summary_fields = OrderedDict()
cj_summary_fields['cj_name'] = fields.String
cj_summary_fields['timestamp'] = fields.DateTime
cj_summary_fields['status'] = fields.String
cjs_get_fields = OrderedDict()
cjs_get_fields['cjs'] = fields.List(
    fields.Nested(cj_summary_fields))


cjs_get_parser = reqparse.RequestParser()
cjs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
cjs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.cj_list_limit),
    default=conf.cj_list_limit,
    location='args',
)
cjs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'cj_name',
        'timestamp',
    ),
    default='cj_name',
    location='args',
)
cjs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
cjs_get_parser.add_argument(
    'status',
    type=str,
    choices=(
        'creating',
        'create_failed',
        'canceling',
        'cancel_failed',
        'processing',
        'finishing',
        'finish_failed',
    ),
    location='args',
)


def handle_cjs_get(params, args):
    order_field = getattr(CloneJob, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = CloneJob.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = CloneJob.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['status']:
        query = query.filter_by(status=args['status'])
    query = query.limit(args['limit'])
    cjs = query.all()
    body = marshal({'cjs': cjs}, cjs_get_fields)
    return body['cjs'], 200


class Cjs(Resource):

    def get(self):
        return handle_dlvm_request(None, cjs_get_parser, handle_cjs_get)
