#!/usr/bin/env python

from collections import OrderedDict
from flask import Flask
from flask_restful import Api, Resource, reqparse, fields, marshal_with
from dlvm.utils.configure import conf

from dlvm.utils.loginit import loginit
from dlvm.utils.modules import db
from dlvm.api_server.handler import handle_dlvm_request
from dlvm.api_server.dpv import handle_dpvs_get, handle_dpvs_post


dpv_fields = OrderedDict()
dpv_fields['dpv_name'] = fields.String
dpv_fields['total_size'] = fields.Integer
dpv_fields['free_size'] = fields.Integer
dpv_fields['status'] = fields.String
dpv_fields['dvg_name'] = fields.String
dpv_fields['lock_id'] = fields.String
dpv_fields['lock_timestamp'] = fields.Integer


def handle_root_get(request_id, params, args):
    return ['dpvs', 'dvgs', 'dlvs', 'fjs', 'ejs', 'cjs', 'rjs', 'mjs']


root_get_fields = OrderedDict()
root_get_fields['request_id'] = fields.String
root_get_fields['message'] = fields.String
root_get_fields['body'] = fields.List(fields.String)


def root_get(args):
    return handle_dlvm_request(handle_root_get, None, None)


class Root(Resource):

    @marshal_with(root_get_fields)
    def get(self):
        return handle_dlvm_request(
            handle_root_get, None, None, 200)


dpvs_get_parser = reqparse.RequestParser()
dpvs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
dpvs_get_parser.add_argument(
    'limit',
    type=int,
    default=conf.dpv_list_limit,
    location='args',
)
dpvs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'dpv_name',
        'total_size',
        'free_size',
        'lock_timestamp',
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
    'locked',
    type=str,
    choices=('true', 'false'),
    location='args',
)
dpvs_get_parser.add_argument(
    'dvg_name',
    type=str,
    location='args',
)

dpvs_get_fields = OrderedDict()
dpvs_get_fields['request_id'] = fields.String
dpvs_get_fields['message'] = fields.String
dpvs_get_fields['body'] = fields.List(fields.Nested(dpv_fields))


dpvs_post_parser = reqparse.RequestParser()
dpvs_post_parser.add_argument(
    'dpv_name',
    type=str,
    required=True,
    location='json'
)

dpvs_post_fields = OrderedDict()
dpvs_post_fields['request_id'] = fields.String
dpvs_post_fields['message'] = fields.String


class Dpvs(Resource):

    @marshal_with(dpvs_get_fields)
    def get(self):
        return handle_dlvm_request(
            handle_dpvs_get, dpvs_get_parser, None, 200)

    @marshal_with(dpvs_post_fields)
    def post(self):
        return handle_dlvm_request(
            handle_dpvs_post, dpvs_post_parser, None, 200)


def create_app():
    loginit()
    app = Flask(__name__)
    api = Api(app)
    app.config['SQLALCHEMY_DATABASE_URI'] = conf.db_uri
    app.config.setdefault('SQLALCHEMY_TRACK_MODIFICATIONS', False)
    db.init_app(app)
    api.add_resource(Root, '/')
    api.add_resource(Dpvs, '/dpvs')
    return app


def init_db():
    app = create_app()
    with app.app_context():
        db.create_all()
