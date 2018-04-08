from typing import NamedTuple, TypeVar, Generic, Type, Optional, Callable, \
    Sequence, Mapping, MutableMapping, List

from enum import Enum
import uuid
import logging
import traceback

from flask import Flask, request
from marshmallow import Schema, fields, ValidationError

from dlvm.common.utils import RequestContext, WorkContext, HttpStatus, ReqId
from dlvm.common.database import Session
from dlvm.common.error import DlvmError
from dlvm.hook.hook import hook_builder, HookRet, ApiHook, \
    ApiHookConcrete, ApiContext


api_hook_list: List[ApiHookConcrete] = hook_builder(ApiHook.hook_name)
ori_logger = logging.getLogger('dlvm_api')


class ApiResponseError(Exception):

    def __init__(self, message: str)-> None:
        self.message = message
        super(ApiResponseError, self).__init__(message)


class ApiSchema(Schema):
    req_id: ReqId = fields.String()
    message: str = fields.String()
    body: Mapping = fields.Method('dump_body')

    def dump_body(self, obj: Mapping)-> Mapping:
        try:
            schema = self.context['body_schema']
            return schema.dump(obj['body'])
        except ValidationError as e:
            raise ApiResponseError(str(e.messages))


class ApiResponse(NamedTuple):
    response: str
    status: HttpStatus
    headers: Mapping = {'Content-Type': 'application/json'}


class ArgLocation(Enum):
    args = 'args'
    json = 'json'


PathType = TypeVar('PathType')
ArgSchemaType = TypeVar('ArgSchemaType', bound=Schema)
ArgType = TypeVar('ArgType')


class ArgInfo(NamedTuple):
    ArgSchema: Type[Schema]
    location: ArgLocation


class EmptyPathType(NamedTuple):
    pass


class EmptyArgType(NamedTuple):
    pass


empty_arg_info = ArgInfo(Schema, ArgLocation.args)


class ApiMethod(Generic[ArgType, PathType]):

    def __init__(
            self,
            func: Callable[
                [RequestContext, WorkContext, ArgType, PathType], object],
            status_code: HttpStatus,
            BodySchema: Type[Schema],
            arg_info: ArgInfo = empty_arg_info)-> None:
        self.func = func
        self.status_code = status_code
        self.BodySchema = BodySchema
        self.arg_info = arg_info


class ApiResource(Generic[ArgType, PathType]):

    def __init__(
            self,
            path_template: str,
            path_type: Type[PathType],
            get: Optional[ApiMethod[ArgType, PathType]] = None,
            post: Optional[ApiMethod[ArgType, PathType]] = None,
            put: Optional[ApiMethod[ArgType, PathType]] = None,
            delete: Optional[ApiMethod[ArgType, PathType]] = None)-> None:
        self.path_template = path_template
        self.path_type = path_type
        real_path = self.build_path(path_template, path_type)
        self.real_path = real_path
        method_dict: MutableMapping[str, ApiMethod[ArgType, PathType]] = {}
        if get is not None:
            method_dict['GET'] = get
        if post is not None:
            method_dict['POST'] = post
        if put is not None:
            method_dict['PUT'] = put
        if delete is not None:
            method_dict['DELETE'] = delete
        self.method_dict = method_dict

    def build_path(self, path_template: str, path_type: Type[PathType])-> str:
        d: Mapping[str, type] = path_type.__annotations__
        fmt: MutableMapping[str, str] = {}
        for key in d:
            if d[key] is int:
                val = '<int:{0}>'.format(key)
            elif d[key] is float:
                val = '<float:{0}>'.format(key)
            else:
                val = '<{0}>'.format(key)
            fmt[key] = val
        return path_template.format(**fmt)


class Api():

    def __init__(self, app: Flask)-> None:
        self.app = app
        self.endpoint_dict: MutableMapping[str, ApiResource] = {}

    def handler(self, *args: Sequence, **kwargs: Mapping)-> ApiResponse:
        res = self.endpoint_dict[request.endpoint]
        method = res.method_dict[request.method]
        if method.arg_info.location == ArgLocation.args:
            arg_dict = request.args
        else:
            arg_dict = request.get_json()
        path = res.path_type(*args, **kwargs)

        raw_response: MutableMapping[str, object] = {}
        req_id = ReqId(uuid.uuid4().hex)
        raw_response['req_id'] = req_id
        logger = logging.LoggerAdapter(ori_logger, {'req_id': req_id})
        req_ctx = RequestContext(req_id, logger)
        session = Session()
        work_ctx = WorkContext(session, set())
        api_ctx = ApiContext(
            req_ctx, work_ctx, method.func.__name__, arg_dict, path)
        hook_ret_dict: MutableMapping[ApiHook, HookRet] = {}
        for hook in api_hook_list:
            try:
                hook_ret = hook.pre_hook(api_ctx)
            except Exception:
                logger.error(
                    'api rep_hook failed: %s %s',
                    hook, api_ctx, exc_info=True)
                hook_ret_dict[hook] = HookRet(None)
            else:
                hook_ret_dict[hook] = hook_ret
        try:
            args = method.arg_info.ArgSchema().load(arg_dict)
            body = method.func(req_ctx, work_ctx, args, path)
            raw_response['message'] = 'succeed'
            raw_response['body'] = body
            api_schema = ApiSchema()
            api_schema.context['body_schema'] = method.BodySchema()
            response = api_schema.dumps(raw_response)
            api_response = ApiResponse(response, method.status_code)
        except Exception as e:
            session.rollback()
            calltrace = traceback.format_exc()
            for hook in api_hook_list:
                hook_ret = hook_ret_dict[hook]
                try:
                    hook.error_hook(
                        api_ctx, hook_ret, e, calltrace)
                except Exception:
                    logger.error(
                        'api error_hook failed: %s %s %s %s %s',
                        hook, api_ctx, hook_ret, e, calltrace,
                        exc_info=True)
                if isinstance(e, ValidationError):
                    message = str(e.messages)
                    status_code = HttpStatus.BadRequest
                elif isinstance(e, DlvmError):
                    message = e.message
                    status_code = e.status_code
                elif isinstance(e, ApiResponseError):
                    message = e.message
                    status_code = HttpStatus.InternalServerError
                else:
                    message = 'internal_error'
                    status_code = HttpStatus.InternalServerError
                raw_response['message'] = message
                raw_response['body'] = None
                api_schema = ApiSchema()
                api_schema.context['body_schema'] = method.BodySchema()
                response = api_schema.dumps(raw_response)
                api_response = ApiResponse(response, status_code)
        else:
            for hook in api_hook_list:
                hook_ret = hook_ret_dict[hook]
                try:
                    hook.post_hook(
                        api_ctx, hook_ret, body)
                except Exception:
                    logger.error(
                        'api post_hook failed: %s %s %s %s',
                        hook, api_ctx, hook_ret, body)
        finally:
            session.close()
            return api_response

    def add_resource(self, res: ApiResource)-> None:
        methods: Sequence[str] = [key for key in res.method_dict.keys()]
        self.endpoint_dict[res.real_path] = res
        self.app.add_url_rule(
            res.real_path, res.real_path, self.handler, methods=methods)
