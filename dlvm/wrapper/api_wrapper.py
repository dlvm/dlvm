from typing import NamedTuple, Mapping, Sequence, Callable, Optional, Type
import sys
import uuid
import enum
from logging import getLogger, LoggerAdapter

from flask import request, make_response
from marshmallow import Schema, fields, ValidationError

from dlvm.common.utils import RequestContext, HttpStatus, ExcInfo
from dlvm.common.loginit import loginit
from dlvm.common.error import DlvmError
from dlvm.common.database import Session
from dlvm.wrapper.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook
from dlvm.wrapper.local_ctx import frontend_local, Direction


class ApiContext(NamedTuple):
    req_ctx: RequestContext
    func_name: str
    arg_dict: Mapping
    path_args: Sequence
    path_kwargs: Mapping


class ApiResponseSchemaError(Exception):

    def __init__(self, message, exc_info):
        self.message = message
        self.exc_info = exc_info
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
            etype, value, tb = sys.exc_info()
            exc_info = ExcInfo(etype, value, tb)
            raise ApiResponseSchemaError(str(e.messages), exc_info)


class ArgLocation(enum.Enum):
    args = 'args'
    json = 'json'


class ArgInfo(NamedTuple):
    arg_schema_cls: Type[Schema]
    location: ArgLocation


empty_arg_info = ArgInfo(Schema, ArgLocation.args)


class ApiMethod(NamedTuple):
    func: Callable
    status_code: HttpStatus
    arg_info: ArgInfo = empty_arg_info


class ApiResource(NamedTuple):
    path: str
    get: Optional[ApiMethod] = None
    post: Optional[ApiMethod] = None
    put: Optional[ApiMethod] = None
    delete: Optional[ApiMethod] = None


class ResInfo(NamedTuple):
    res: ApiResource
    method_dict: Mapping[str, ApiMethod]


class ApiRet(NamedTuple):
    data: object
    schema: Schema


api_hook_list = build_hook_list('api_hook')
ori_logger = getLogger('dlvm_api')
api_headers = {'Content-Type': 'application/json'}


class Api():

    def __init__(self, app):
        self.app = app
        self.res_info_dict = {}
        loginit()

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
        logger = LoggerAdapter(ori_logger, {'req_id': req_id})
        req_ctx = RequestContext(req_id, logger)
        session = Session()
        hook_ctx = ApiContext(
            req_ctx, method.func.__name__,
            arg_dict, path_args, path_kwargs)

        hook_ret_dict = run_pre_hook('api', api_hook_list, hook_ctx)
        try:
            args = method.arg_info.arg_schema_cls().load(arg_dict)
            frontend_local.args = args
            frontend_local.req_ctx = req_ctx
            frontend_local.session = session
            frontend_local.force = False
            frontend_local.worklog = set()
            frontend_local.direction = Direction.forward
            api_ret = method.func(*path_args, **path_kwargs)
            raw_response['message'] = 'succeed'
            if api_ret is None:
                raw_response['data'] = None
                api_response_schema = ApiResponseSchema()
                api_response_schema.context['data_schema'] = None
            else:
                raw_response['data'] = api_ret.data
                api_response_schema = ApiResponseSchema()
                api_response_schema.context['data_schema'] = api_ret.schema
            response = api_response_schema.dumps(raw_response)
            full_response = make_response(
                response, method.status_code, api_headers)
        except Exception as e:
            if isinstance(e, ValidationError):
                message = str(e.messages)
                status_code = HttpStatus.BadRequest
                etype, value, tb = sys.exc_info()
                exc_info = ExcInfo(etype, value, tb)
            elif isinstance(e, DlvmError):
                message = e.message
                status_code = e.status_code
                if e.exc_info is not None:
                    exc_info = e.exc_info
                else:
                    etype, value, tb = sys.exc_info()
                    exc_info = ExcInfo(etype, value, tb)
            elif isinstance(e, ApiResponseSchemaError):
                message = e.message
                status_code = HttpStatus.InternalServerError
                exc_info = e.exc_info
            else:
                message = 'internal_error'
                status_code = HttpStatus.InternalServerError
                etype, value, tb = sys.exc_info()
                exc_info = ExcInfo(etype, value, tb)

            session.rollback()
            run_error_hook(
                'api', api_hook_list, hook_ctx, hook_ret_dict, exc_info)
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
