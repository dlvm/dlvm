from typing import NamedTuple

from marshmallow import Schema, fields

from dlvm.common.utils import RequestContext, WorkContext, HttpStatus
from dlvm.hook.hook import ApiRet
from dlvm.hook.api_wrapper import EmptyPath, EmptyArgs, empty_arg_info, \
    ApiMethod, ApiResource


class RootGetBody(NamedTuple):
    api_version: str


class RootGetBodySchema(Schema):
    api_version = fields.String()


def api_root_get(
        req_ctx: RequestContext, work_ctx: WorkContext,
        args: EmptyArgs, path: EmptyPath)-> ApiRet:
    return ApiRet(RootGetBody('v1'), RootGetBodySchema())


root_get_method = ApiMethod[EmptyArgs, EmptyPath](
    api_root_get, HttpStatus.OK, empty_arg_info)

root_path_template = '/'
root_path_type = EmptyPath

root_res = ApiResource[EmptyPath](
    root_path_template, root_path_type, get=root_get_method)
