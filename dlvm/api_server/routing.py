#!/usr/bin/env python

from collections import OrderedDict
import copy
from flask import Flask
from flask_restful import Api, Resource, reqparse, fields
from dlvm.utils.configure import conf

from dlvm.utils.loginit import loginit
from dlvm.utils.modules import db
from dlvm.api_server.handler import handle_dlvm_request
from dlvm.api_server.dpv import handle_dpvs_get, handle_dpvs_post, \
    handle_dpv_get, handle_dpv_delete, handle_dpv_resize
from dlvm.api_server.dvg import handle_dvgs_get, handle_dvgs_post, \
    handle_dvg_get, handle_dvg_delete, \
    handle_dvg_extend, handle_dvg_reduce


dpv_fields = OrderedDict()
dpv_fields['dpv_name'] = fields.String
dpv_fields['total_size'] = fields.Integer
dpv_fields['free_size'] = fields.Integer
dpv_fields['status'] = fields.String
dpv_fields['dvg_name'] = fields.String
dpv_fields['lock_id'] = fields.String
dpv_fields['lock_timestamp'] = fields.Integer

dvg_fields = OrderedDict()
dvg_fields['dvg_name'] = fields.String
dvg_fields['total_size'] = fields.Integer
dvg_fields['free_size'] = fields.Integer

group_fields = OrderedDict()
group_fields['group_id'] = fields.String
group_fields['idx'] = fields.Integer
group_fields['group_size'] = fields.Integer
group_fields['dlv_name'] = fields.String

leg_fields = OrderedDict()
leg_fields['leg_id'] = fields.String
leg_fields['idx'] = fields.Integer
leg_fields['leg_size'] = fields.Integer
leg_fields['dpv_name'] = fields.String


def handle_root_get(request_id, params, args):
    return ['dpvs', 'dvgs', 'dlvs', 'fjs', 'ejs', 'cjs', 'rjs', 'mjs']


class Root(Resource):

    def get(self):
        return handle_dlvm_request(
            handle_root_get, None, None, 200, None)


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

dpv_summary_fields = copy.deepcopy(dpv_fields)

dpvs_post_parser = reqparse.RequestParser()
dpvs_post_parser.add_argument(
    'dpv_name',
    type=str,
    required=True,
    location='json'
)


class Dpvs(Resource):

    def get(self):
        return handle_dlvm_request(
            handle_dpvs_get, dpvs_get_parser, None, 200, dpv_summary_fields)

    def post(self):
        return handle_dlvm_request(
            handle_dpvs_post, dpvs_post_parser, None, 200, None)


dpv_leg_group_fields = copy.deepcopy(group_fields)
dpv_leg_fields = copy.deepcopy(leg_fields)
dpv_leg_fields['group'] = fields.Nested(dpv_leg_group_fields)
dpv_detail_fields = copy.deepcopy(dpv_fields)
dpv_detail_fields['legs'] = fields.List(fields.Nested(dpv_leg_fields))


class Dpv(Resource):

    def get(self, dpv_name):
        return handle_dlvm_request(
            handle_dpv_get, None, [dpv_name], 200, dpv_detail_fields)

    def delete(self, dpv_name):
        return handle_dlvm_request(
            handle_dpv_delete, None, [dpv_name], 200, None)


class DpvResize(Resource):

    def put(self, dpv_name):
        return handle_dlvm_request(
            handle_dpv_resize, None, [dpv_name], 200, None)


dvgs_get_parser = reqparse.RequestParser()
dvgs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
dvgs_get_parser.add_argument(
    'limit',
    type=int,
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

dvg_summary_fields = copy.deepcopy(dvg_fields)

dvgs_post_parser = reqparse.RequestParser()
dvgs_post_parser.add_argument(
    'dvg_name',
    type=str,
    required=True,
    location='json',
)


class Dvgs(Resource):

    def get(self):
        return handle_dlvm_request(
            handle_dvgs_get, dvgs_get_parser, None, 200, dvg_summary_fields)

    def post(self):
        return handle_dlvm_request(
            handle_dvgs_post, dvgs_post_parser, None, 200, None)


dvg_detail_fields = copy.deepcopy(dvg_fields)


class Dvg(Resource):

    def get(self, dvg_name):
        return handle_dlvm_request(
            handle_dvg_get, None, [dvg_name], 200, dvg_detail_fields)

    def delete(self, dvg_name):
        return handle_dlvm_request(
            handle_dvg_delete, None, [dvg_name], 200, None)


dvg_extend_parser = reqparse.RequestParser()
dvg_extend_parser.add_argument(
    'dpv_name',
    type=str,
    required=True,
    location='json',
)


class DvgExtend(Resource):

    def put(self, dvg_name):
        return handle_dlvm_request(
            handle_dvg_extend, dvg_extend_parser, [dvg_name], 200, None)


dvg_reduce_parser = reqparse.RequestParser()
dvg_reduce_parser.add_argument(
    'dpv_name',
    type=str,
    required=True,
    location='json',
)


class DvgReduce(Resource):

    def put(self, dvg_name):
        return handle_dlvm_request(
            handle_dvg_reduce, dvg_reduce_parser, [dvg_name], 200, None)


def create_app():
    loginit()
    app = Flask(__name__)
    api = Api(app)
    app.config['SQLALCHEMY_DATABASE_URI'] = conf.db_uri
    app.config.setdefault('SQLALCHEMY_TRACK_MODIFICATIONS', False)
    db.init_app(app)
    api.add_resource(Root, '/')
    api.add_resource(Dpvs, '/dpvs')
    api.add_resource(Dpv, '/dpvs/<string:dpv_name>')
    api.add_resource(DpvResize, '/dpvs/<string:dpv_name>/resize')
    api.add_resource(Dvgs, '/dvgs')
    api.add_resource(Dvg, '/dvgs/<string:dvg_name>')
    api.add_resource(DvgExtend, '/dvgs/<string:dvg_name>/extend')
    api.add_resource(DvgReduce, '/dvgs/<string:dvg_name>/reduce')
    return app


def init_db():
    app = create_app()
    with app.app_context():
        db.create_all()
