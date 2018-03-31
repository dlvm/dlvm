import unittest
from collections import OrderedDict
import json

from flask import Flask
from flask_restful import Api, reqparse, Resource, fields

from dlvm.hook.api_wrapper import handle_dlvm_api


fake_get_parser = reqparse.RequestParser()
fake_get_parser.add_argument(
    'arg1',
    type=str,
    location='args',
)
fake_get_parser.add_argument(
    'arg2',
    type=int,
    location='args',
)

fake_get_params_fields = OrderedDict()
fake_get_params_fields['arg1'] = fields.String
fake_get_params_fields['arg2'] = fields.Integer
fake_get_kwargs_fields = OrderedDict()
fake_get_kwargs_fields['name1'] = fields.String
fake_get_kwargs_fields['name2'] = fields.String
fake_get_fields = OrderedDict()
fake_get_fields['params'] = fields.Nested(fake_get_params_fields)
fake_get_fields['kwargs'] = fields.Nested(fake_get_kwargs_fields)


def fake_get(req_ctx, work_ctx, params, kwargs):
    return {
        'params': params,
        'kwargs': kwargs,
    }


class FakeResource(Resource):

    def get(self, *args, **kwargs):
        return handle_dlvm_api(
            fake_get, 200, fake_get_parser, fake_get_fields, args, kwargs)


class ApiWrapperTest(unittest.TestCase):

    def setUp(self):
        app = Flask(__name__)
        api = Api(app)
        api.add_resource(
            FakeResource, '/path/<string:name1>/and/<string:name2>')
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_get(self):
        resp = self.client.get('/path/value1/and/value2')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['body']['kwargs']['name1'], 'value1')
