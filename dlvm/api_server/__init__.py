#!/usr/bin/env python

from collections import OrderedDict
from flask import Flask
from flask_restful import Api, Resource, fields, marshal
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from modules import db
from handler import handle_dlvm_request

root_get_fields = OrderedDict()
root_get_fields['endpoints'] = fields.List(fields.String)


def handle_root_get(params, args):
    body = marshal(
        {'endpoints': ['dpvs', 'dvgs', 'dlvs', 'hosts', 'mjs']},
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
    return app

app = create_app()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=conf.api_port, debug=True)
