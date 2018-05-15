from typing import NamedTuple, Mapping, Sequence, Callable, Optional, Type
import sys
import uuid
import enum
from logging import getLogger, LoggerAdapter

from flask import request, make_response
from marshmallow import Schema, fields, ValidationError

from dlvm.common.constant import API_LOGGER_NAME
from dlvm.common.utils import RequestContext, HttpStatus, ExcInfo
from dlvm.common.loginit import loginit
from dlvm.common.error import DlvmError
from dlvm.common.database import Session
from dlvm.common.marshmallow_ext import NtSchema
from dlvm.wrapper.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook
from dlvm.wrapper.local_ctx import frontend_local, get_empty_worker_ctx


class ApiContext(NamedTuple):
    req_ctx: RequestContext
    func_name: str
    arg_dict: Mapping
    path_args: Sequence
    path_kwargs: Mapping


class ApiResponseSchema(Schema):
    req_id = fields.UUID()
    message = fields.String()
    data = fields.Raw()


class ArgLocation(enum.Enum):
    query = 'query'
    body = 'body'


class ArgInfo(NamedTuple):
    arg_schema: Type[NtSchema]
    location: ArgLocation


empty_arg_info = ArgInfo(NtSchema, ArgLocation.query)


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


api_hook_list = build_hook_list('api_hook')
ori_logger = getLogger(API_LOGGER_NAME)
api_headers = {'Content-Type': 'application/json'}


def common_handler(method, *path_args, **path_kwargs):
    if method.arg_info.location == ArgLocation.query:
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
        arg = method.arg_info.arg_schema().load(arg_dict)
        frontend_local.arg = arg
        frontend_local.req_ctx = req_ctx
        frontend_local.session = session
        frontend_local.worker_ctx = get_empty_worker_ctx()
        frontend_local.async = True
        data = method.func(*path_args, **path_kwargs)
        raw_response['data'] = data
        raw_response['message'] = 'succeed'
        response = ApiResponseSchema().dumps(raw_response)
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
        response = ApiResponseSchema().dumps(raw_response)
        full_response = make_response(
            response, status_code, api_headers)
    else:
        run_post_hook(
            'api', api_hook_list, hook_ctx, hook_ret_dict, full_response)
    finally:
        session.close()
        return full_response


class Api():

    def __init__(self, app):
        self.app = app
        self.res_info_dict = {}
        loginit()

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

        def handler(*path_args, **path_kwargs):
            method = method_dict[request.method]
            return common_handler(method, *path_args, **path_kwargs)

        self.app.add_url_rule(
            res.path, res.path, handler, methods=methods)
