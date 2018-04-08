from typing import NamedTuple

from flask import Flask
from marshmallow import Schema, fields

from dlvm.common.utils import RequestContext, WorkContext, HttpStatus
from dlvm.hook.api_wrapper import EmptyPathType, EmptyArgType, \
    ApiMethod, ApiResource, Api


app = Flask(__name__)
api = Api(app)


class RootGetBody(NamedTuple):
    api_version: str


class RootGetBodySchema(Schema):
    api_version = fields.String()


def api_root_get(
        req_ctx: RequestContext, work_ctx: WorkContext,
        args: EmptyArgType, path: EmptyPathType)-> RootGetBody:
    return RootGetBody('v1')


root_get_method = ApiMethod[EmptyArgType, EmptyPathType](
    api_root_get, HttpStatus.OK, RootGetBodySchema)

root_path_template = '/'
root_path_type = EmptyPathType

root_res = ApiResource[EmptyArgType, EmptyPathType](
    root_path_template, root_path_type, get=root_get_method)

api.add_resource(root_res)
