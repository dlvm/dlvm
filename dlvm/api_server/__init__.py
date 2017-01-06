#!/usr/bin/env python

from collections import OrderedDict
from flask import Flask
from flask_restful import Api, Resource, fields, marshal
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from modules import db
from handler import handle_dlvm_request
from dpv import Dpvs, Dpv
from dvg import Dvgs, Dvg
from dlv import Dlvs, Dlv
from obt import Obts, Obt

root_get_fields = OrderedDict()
root_get_fields['endpoints'] = fields.List(fields.String)


def handle_root_get(params, args):
    body = marshal(
        {'endpoints': ['dpvs', 'dvgs', 'dlvs', 'thosts', 'mjs']},
        root_get_fields,
    )
    return body['endpoints'], 200


class Root(Resource):

    def get(self):
        return handle_dlvm_request(None, None, handle_root_get)


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
    api.add_resource(Dvgs, '/dvgs')
    api.add_resource(Dvg, '/dvgs/<string:dvg_name>')
    api.add_resource(Dlvs, '/dlvs')
    api.add_resource(Dlv, '/dlvs/<string:dlv_name>')
    api.add_resource(Obts, '/obts')
    api.add_resource(Obt, '/obts/<string:t_id>')
    return app

app = create_app()


def init_db():
    app = create_app()
    with app.app_context():
        db.create_all()
