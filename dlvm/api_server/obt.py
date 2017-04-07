#!/usr/bin/env python

from collections import OrderedDict
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from modules import db, OwnerBasedTransaction, Counter
from handler import handle_dlvm_request, make_body, check_limit


obt_summary_fields = OrderedDict()
obt_summary_fields['t_id'] = fields.String
obt_summary_fields['t_owner'] = fields.String
obt_summary_fields['t_stage'] = fields.Integer
obt_summary_fields['timestamp'] = fields.DateTime
obt_summary_fields['count'] = fields.Integer
obts_get_fields = OrderedDict()
obts_get_fields['obts'] = fields.List(
    fields.Nested(obt_summary_fields))

obts_get_parser = reqparse.RequestParser()
obts_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
obts_get_parser.add_argument(
    'limit',
    type=check_limit(conf.obt_list_limit),
    default=conf.obt_list_limit,
    location='args',
)
obts_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        't_id',
        't_owner',
        'timestamp',
    ),
    default='t_id',
    location='args',
)
obts_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)


def handle_obts_get(params, args):
    order_field = getattr(OwnerBasedTransaction, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = OwnerBasedTransaction \
            .query \
            .order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = OwnerBasedTransaction \
            .query \
            .order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    query = query.limit(args['limit'])
    obts = query.all()
    body = marshal({'obts': obts}, obts_get_fields)
    return body['obts'], 200


obts_post_parser = reqparse.RequestParser()
obts_post_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
obts_post_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
obts_post_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)
obts_post_parser.add_argument(
    'annotation',
    type=str,
    default='',
    location='json',
)


def handle_obts_post(params, args):
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    annotation = args['annotation']
    counter = Counter()
    obt = OwnerBasedTransaction(
        t_id=t_id,
        t_owner=t_owner,
        t_stage=t_stage,
        annotation=annotation,
        timestamp=datetime.datetime.utcnow(),
        counter=counter,
    )
    db.session.add(obt)
    db.session.commit()
    return make_body('success'), 200


class Obts(Resource):

    def get(self):
        return handle_dlvm_request(
            None,
            obts_get_parser,
            handle_obts_get,
        )

    def post(self):
        return handle_dlvm_request(
            None,
            obts_post_parser,
            handle_obts_post,
        )


obt_put_parser = reqparse.RequestParser()
obt_put_parser.add_argument(
    'action',
    type=str,
    choices=('preempt', 'annotation'),
    required=True,
    location='json',
)
obt_put_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
obt_put_parser.add_argument(
    'new_owner',
    type=str,
    required=True,
    location='json',
)


def handle_obt_preempt(params, args):
    t_id = params[0]
    old_owner = args['t_owner']
    if 'new_owner' not in args:
        return make_body('miss_new_owner'), 400
    new_owner = args['new_owner']
    try:
        obt = OwnerBasedTransaction \
            .query \
            .with_lockmode('update') \
            .filter_by(t_id=t_id) \
            .one()
    except NoResultFound:
        return make_body('not_exist', 404)
    if obt.t_owner != old_owner:
        return make_body('wrong_owner', obt.t_owner), 400
    obt.t_owner = new_owner
    db.session.add(obt)
    db.session.commit()
    return make_body('success'), 200


def handle_obt_put(params, args):
    if args['action'] == 'preempt':
        return handle_obt_preempt(params, args)


obt_delete_parser = reqparse.RequestParser()
obt_delete_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)


def handle_obt_delete(params, args):
    t_id = params[0]
    t_owner = args['t_owner']
    try:
        obt = OwnerBasedTransaction \
            .query \
            .with_lockmode('update') \
            .filter_by(t_id=t_id) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    if obt.t_owner != t_owner:
        return make_body('wrong_owner', obt.t_owner), 400
    for dlv in obt.dlvs:
        dlv.obt = None
        db.session.add(dlv)
    db.session.delete(obt)
    db.session.commit()
    return make_body('success'), 200


obt_fields = OrderedDict()
obt_fields['t_id'] = fields.String
obt_fields['t_owner'] = fields.String
obt_fields['t_stage'] = fields.Integer
obt_fields['timestamp'] = fields.DateTime
obt_fields['count'] = fields.Integer
obt_fields['annotation'] = fields.String


def handle_obt_get(params, args):
    t_id = params[0]
    try:
        obt = OwnerBasedTransaction \
            .query \
            .filter_by(t_id=t_id) \
            .one()
    except NoResultFound:
        return make_body('not_exist', 404)
    body = marshal(obt, obt_fields)
    return body, 200


class Obt(Resource):

    def get(self, t_id):
        return handle_dlvm_request(
            [t_id],
            None,
            handle_obt_get,
        )

    def put(self, t_id):
        return handle_dlvm_request(
            [t_id],
            obt_put_parser,
            handle_obt_put,
        )

    def delete(self, t_id):
        return handle_dlvm_request(
            [t_id],
            obt_delete_parser,
            handle_obt_delete,
        )
