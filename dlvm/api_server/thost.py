#!/usr/bin/env python

from collections import OrderedDict
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from modules import db, TargetHost
from handler import handle_dlvm_request, make_body, check_limit


thost_summary_fields = OrderedDict()
thost_summary_fields['thost_name'] = fields.String
thost_summary_fields['status'] = fields.String
thost_summary_fields['timestamp'] = fields.DateTime
thosts_get_fields = OrderedDict()
thosts_get_fields['thosts'] = fields.List(
    fields.Nested(thost_summary_fields))

thosts_get_parser = reqparse.RequestParser()
thosts_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
thosts_get_parser.add_argument(
    'limit',
    type=check_limit(conf.thost_list_limit),
    default=conf.thost_list_limit,
    location='args',
)
thosts_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'thost_name',
        'timestamp',
    ),
    default='thost_name',
    location='args',
)
thosts_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
thosts_get_parser.add_argument(
    'status',
    type=str,
    choices=(
        'available',
        'unavailable',
    ),
    location='args',
)


def handle_thosts_get(params, args):
    order_field = getattr(TargetHost, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = TargetHost.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = TargetHost.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['status']:
        query = query.filter_by(status=args['status'])
    query = query.limit(args['limit'])
    thosts = query.all()
    body = marshal({'thosts': thosts}, thosts_get_fields)
    return body['thosts'], 200


thosts_post_parser = reqparse.RequestParser()
thosts_post_parser.add_argument(
    'thost_name',
    type=str,
    required=True,
    location='json',
)


def handle_thosts_post(params, args):
    thost_name = args['thost_name']
    thost = TargetHost(
        thost_name=thost_name,
        status='available',
        timestamp=datetime.datetime.utcnow(),
    )
    try:
        db.session.add(thost)
        db.session.commit()
    except IntegrityError:
        return make_body('duplicate_thost'), 400
    else:
        return make_body('success'), 200


class Thosts(Resource):

    def get(self):
        return handle_dlvm_request(
            None, thosts_get_parser, handle_thosts_get)

    def post(self):
        return handle_dlvm_request(
            None, thosts_post_parser, handle_thosts_post)


def handle_thost_delete(params, args):
    thost_name = params[0]
    try:
        thost = TargetHost \
            .query \
            .with_lockmode('update') \
            .filter_by(thost_name=thost_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist', 404)
    if len(thost.dlvs) > 0:
        return make_body('thost_busy'), 403
    db.session.delete(thost)
    db.session.commit()
    return make_body('success'), 200


class Thost(Resource):

    def delete(self, thost_name):
        return handle_dlvm_request(
            [thost_name],
            None,
            handle_thost_delete,
        )
