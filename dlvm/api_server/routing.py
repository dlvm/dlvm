#!/usr/bin/env python

from collections import OrderedDict
from flask import Flask
from flask_restful import Api, Resource, reqparse, fields, marshal_with
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from modules import db
from handler import handle_dlvm_request
# from dpv import handle_dpvs_get, handle_dpvs_post


def handle_root_get(params, args):
    body = ['dpvs', 'dvgs', 'dlvs', 'fjs', 'ejs', 'cjs', 'rjs', 'mjs']
    message = 'SUCCESS'
    return_code = 200
    return body, message, return_code


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
            handle_root_get, None, None)


def create_app():
    loginit()
    app = Flask(__name__)
    api = Api(app)
    app.config['SQLALCHEMY_DATABASE_URI'] = conf.db_uri
    app.config.setdefault('SQLALCHEMY_TRACK_MODIFICATIONS', False)
    db.init_app(app)
    api.add_resource(Root, '/')
    return app


def init_db():
    app = create_app()
    with app.app_context():
        db.create_all()
