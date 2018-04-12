import sys
import uuid
import enum
from collections import namedtuple
import logging

from flask import request, g, make_response
from marshmallow import Schema, fields, ValidationError

from dlvm.common.utils import RequestContext, WorkContext, \
    namedtuple_with_default, HttpStatus
from dlvm.common.error import DlvmError
from dlvm.common.database import Session
from dlvm.hook.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook, ExcInfo


ApiContext = namedtuple('ApiContext', [
    'req_ctx', 'work_ctx', 'func_name',
    'arg_dict', 'path_args', 'path_kwargs'])


class ApiResponseSchemaError(Exception):

    def __init__(self, message):
        self.message = message
        super(ApiResponseSchemaError, self).__init__(message)


class ApiResponseSchema(Schema):
    req_id = fields.UUID()
    message = fields.String()
    data = fields.Method('dump_data')

    def dump_data(self, obj):
        schema = self.context['data_schema']
        try:
            if obj['data'] is None:
                return None
            elif schema is None:
                return obj['data']
            else:
                schema = self.context['data_schema']
                return schema.dump(obj['data'])
        except ValidationError as e:
            raise ApiResponseSchemaError(str(e.messages))


class ArgLocation(enum.Enum):
    args = 'args'
    json = 'json'


ArgInfo = namedtuple('ArgInfo', [
        'arg_schema_cls', 'location'])


empty_arg_info = ArgInfo(Schema, ArgLocation.args)


ApiMethod = namedtuple_with_default(
    'ApiMethod',
    ['func', 'status_code', 'arg_info'],
    (empty_arg_info,))

ApiResource = namedtuple_with_default(
    'ApiResource',
    ['path', 'get', 'post', 'put', 'delete'],
    (None, None, None, None))


ResInfo = namedtuple('ResInfo', [
        'res', 'method_dict'])


ApiRet = namedtuple('ApiRet', ['data', 'schema'])


api_hook_list = build_hook_list('api_hook')
ori_logger = logging.getLogger('dlvm_api')
api_headers = {'Content-Type': 'application/json'}


class Api():

    def __init__(self, app):
        self.app = app
        self.res_info_dict = {}

    def handler(self, *path_args, **path_kwargs):
        res_info = self.res_info_dict[request.endpoint]
        method = res_info.method_dict[request.method]
        if method.arg_info.location == ArgLocation.args:
            arg_dict = request.args
        else:
            arg_dict = request.get_json()
        raw_response = {}
        req_id = uuid.uuid4()
        raw_response['req_id'] = req_id
        logger = logging.LoggerAdapter(ori_logger, {'req_id': req_id})
        req_ctx = RequestContext(req_id, logger)
        session = Session()
        work_ctx = WorkContext(session, set())
        hook_ctx = ApiContext(
            req_ctx, work_ctx, method.func.__name__,
            arg_dict, path_args, path_kwargs)

        hook_ret_dict = run_pre_hook('api', api_hook_list, hook_ctx)
        try:
            args = method.arg_info.arg_schema_cls().load(arg_dict)
            g.args = args
            g.req_ctx = req_ctx
            g.work_ctx = work_ctx
            api_ret = method.func(*path_args, **path_kwargs)
            raw_response['message'] = 'succeed'
            raw_response['data'] = api_ret.data
            api_response_schema = ApiResponseSchema()
            api_response_schema.context['data_schema'] = api_ret.schema
            response = api_response_schema.dumps(raw_response)
            full_response = make_response(
                response, method.status_code, api_headers)
        except Exception as e:
            etype, value, tb = sys.exc_info()
            exc_info = ExcInfo(etype, value, tb)
            run_error_hook(
                'api', api_hook_list, hook_ctx, hook_ret_dict, exc_info)
            session.rollback()
            if isinstance(e, ValidationError):
                message = str(e.messages)
                status_code = HttpStatus.BadRequest
            elif isinstance(e, DlvmError):
                message = e.message
                status_code = e.status_code
            elif isinstance(e, ApiResponseSchemaError):
                message = e.message
                status_code = HttpStatus.InternalServerError
            else:
                message = 'internal_error'
                status_code = HttpStatus.InternalServerError
            raw_response['message'] = message
            raw_response['data'] = None
            api_response_schema = ApiResponseSchema()
            response = api_response_schema.dumps(raw_response)
            full_response = make_response(
                response, status_code, api_headers)
        else:
            run_post_hook(
                'api', api_hook_list, hook_ctx, hook_ret_dict, full_response)
        finally:
            session.close()
            return full_response

    def add_resource(self, res):
        methods = []
        method_dict = {}
        if res.get is not None:
            methods.append('GET')
            method_dict['GET'] = res.get
        if res.post is not None:
            methods.append('POST')
            method_dict['POST'] = res.post
        if res.put is not None:
            methods.append('PUT')
            method_dict['PUT'] = res.put
        if res.delete is not None:
            methods.append('DELETE')
            method_dict['DELETE'] = res.delete
        res_info = ResInfo(res, method_dict)
        self.res_info_dict[res.path] = res_info
        self.app.add_url_rule(
            res.path, res.path, self.handler, methods=methods)
