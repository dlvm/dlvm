#!/usr/bin/env python

from collections import OrderedDict
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from modules import db, Transaction, Counter
from handler import handle_dlvm_request, make_body, check_limit


transaction_summary_fields = OrderedDict()
transaction_summary_fields['t_id'] = fields.String
transaction_summary_fields['t_owner'] = fields.String
transaction_summary_fields['t_stage'] = fields.Integer
transaction_summary_fields['timestamp'] = fields.DateTime
transaction_summary_fields['count'] = fields.Integer
transactions_get_fields = OrderedDict()
transactions_get_fields['transactions'] = fields.List(
    fields.Nested(transaction_summary_fields))

transactions_get_parser = reqparse.RequestParser()
transactions_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
transactions_get_parser.add_argument(
    'limit',
    type=check_limit(conf.transaction_list_limit),
    default=conf.transaction_list_limit,
    location='args',
)
transactions_get_parser.add_argument(
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
transactions_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)


def handle_transactions_get(params, args):
    order_field = getattr(Transaction, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = Transaction.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = Transaction.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    query = query.limit(args['limit'])
    transactions = query.all()
    body = marshal({'transactions': transactions}, transactions_get_fields)
    return body['transactions'], 200


transactions_post_parser = reqparse.RequestParser()
transactions_post_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
transactions_post_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
transactions_post_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def handle_transactions_post(params, args):
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    counter = Counter()
    t = Transaction(
        t_id=t_id,
        t_owner=t_owner,
        t_stage=t_stage,
        timestamp=datetime.datetime.utcnow(),
        counter=counter,
    )
    db.session.add(t)
    db.session.commit()
    return make_body('success'), 200


class Transactions(Resource):

    def get(self):
        return handle_dlvm_request(
            None,
            transactions_get_parser,
            handle_transactions_get,
        )

    def post(self):
        return handle_dlvm_request(
            None,
            transactions_post_parser,
            handle_transactions_post,
        )

transaction_put_parser = reqparse.RequestParser()
transaction_put_parser.add_argument(
    'action',
    type=str,
    choices=('preempt', 'annotation'),
    required=True,
    location='json',
)
transaction_put_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
transaction_put_parser.add_argument(
    'new_owner',
    type=str,
    required=True,
    location='json',
)


def handle_transaction_preempt(params, args):
    t_id = params[0]
    old_owner = args['t_owner']
    if 'new_owner' not in args:
        return make_body('miss_new_owner'), 400
    new_owner = args['new_owner']
    try:
        t = Transaction \
            .query \
            .with_lockmode('update') \
            .filter_by(t_id=t_id) \
            .one()
    except NoResultFound:
        return make_body('not_exist', 404)
    if t.t_owner != old_owner:
        return make_body('wrong_owner', t.t_owner), 400
    t.t_owner = new_owner
    db.session.add(t)
    db.session.commit()
    return make_body('success'), 200


def handle_transaction_put(params, args):
    if args['action'] == 'preempt':
        return handle_transaction_preempt(params, args)


transaction_delete_parser = reqparse.RequestParser()
transaction_delete_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)


def handle_transaction_delete(params, args):
    t_id = params[0]
    t_owner = args['t_owner']
    try:
        t = Transaction \
            .query \
            .with_lockmode('update') \
            .filter_by(t_id=t_id) \
            .one()
    except NoResultFound:
        return make_body('not_exist', 404)
    if t.t_owner != t_owner:
        return make_body('wrong_owner', t.t_owner), 400
    db.session.delete(t)
    db.session.commit()
    return make_body('success'), 200


class Tran(Resource):

    def put(self, t_id):
        return handle_dlvm_request(
            [t_id],
            transaction_put_parser,
            handle_transaction_put,
        )

    def delete(self, t_id):
        return handle_dlvm_request(
            [t_id],
            transaction_delete_parser,
            handle_transaction_delete,
        )
