#!/usr/bin/env python

from collections import OrderedDict
import uuid
import logging
from flask import Flask
from flask_restful import Api, Resource, fields, marshal
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from dlvm.utils.error import TransactionConflictError, TransactionMissError
from modules import db


logger = logging.getLogger('dlvm_api')


def handle_dlvm_request(params, parser, handler):
    request_id = uuid.uuid4().hex
    response = OrderedDict()
    response['request_id'] = request_id
    if parser:
        args = parser.parse_args()
    else:
        args = None
    logger.info('request_id=%s, params=%s, args=%s, handler=%s',
                request_id, params, args, handler.__name__)
    try:
        body, return_code = handler(params, args)
    except TransactionConflictError:
        db.session.rollback()
        logger.warning('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'transaction_conflict',
        }
        return_code = 400
        response['body'] = body
    except TransactionMissError:
        db.session.rollback()
        logger.warning('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'transaction_miss',
        }
        return_code = 400
        response['body'] = body
    except:
        db.session.rollback()
        logger.error('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'internal_error',
        }
        return_code = 500
        response['body'] = body
    finally:
        db.session.close()
        logger.info('request_id=%s\nbody=%s\nreturn_code=%d',
                    request_id, body, return_code)
        response['body'] = body
        return response, return_code


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
