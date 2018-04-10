from typing import NamedTuple, Mapping, Optional

import enum

from flask import Flask
from marshmallow import Schema, fields, post_load
from marshmallow.validate import Range
from marshmallow_enum import EnumField

from dlvm.common.configure import cfg
from dlvm.common.utils import RequestContext, WorkContext, HttpStatus
from dlvm.hook.hook import ApiRet
from dlvm.hook.api_wrapper import EmptyPath, EmptyArgs, \
    ApiMethod, ApiResource, Api, ArgInfo, ArgLocation
from dlvm.core.modules import DpvStatus, DistributePhysicalVolume
from dlvm.core.schema import DpvApiSchema
from dlvm.core.helper import GeneralQuery

app = Flask(__name__)
api = Api(app)


class RootGetBody(NamedTuple):
    api_version: str


class RootGetBodySchema(Schema):
    api_version = fields.String()


def api_root_get(
        req_ctx: RequestContext, work_ctx: WorkContext,
        args: EmptyArgs, path: EmptyPath)-> ApiRet:
    return ApiRet(RootGetBody('v1'), RootGetBodySchema())


root_get_method = ApiMethod[EmptyArgs, EmptyPath](
    api_root_get, HttpStatus.OK)

root_path_template = '/'
root_path_type = EmptyPath

root_res = ApiResource[EmptyPath](
    root_path_template, root_path_type, get=root_get_method)

api.add_resource(root_res)


class DpvOrderFields(enum.Enum):
    dpv_name = 'dpv_name'
    total_size = 'total_size'
    free_size = 'free_size'


class DpvsGetArgs(NamedTuple):
    order_by: DpvOrderFields = DpvOrderFields.dpv_name
    reverse: bool = False
    offset: int = 0
    limit: int = cfg.api.list_limit
    status: Optional[DpvStatus] = None
    locked: Optional[bool] = None
    dvg_name: Optional[str] = None


class DpvsGetArgsSchema(Schema):
    order_by = EnumField(DpvOrderFields)
    reverse = fields.Boolean()
    offset = fields.Integer(validate=Range(0))
    limit = fields.Integer(validate=Range(0, cfg.api.list_limit))
    status = EnumField(DpvStatus)
    locked = fields.Boolean()
    dvg_name = fields.String()

    @post_load
    def make_dpvs_get_args(self, data: Mapping)-> DpvsGetArgs:
        return DpvsGetArgs(**data)


dpvs_get_args_info = ArgInfo(DpvsGetArgsSchema, ArgLocation.args)


def api_dpvs_get(
        req_ctx: RequestContext, work_ctx: WorkContext,
        args: DpvsGetArgs, path: EmptyPath)-> ApiRet:
    query = GeneralQuery(work_ctx.session, DistributePhysicalVolume)
    query.add_order_field(args.order_by.value, args.reverse)
    query.set_offset(args.offset)
    query.set_limit(args.limit)
    if args.status is not None:
        query.add_is_field('status', args.status)
    if args.locked is not None:
        if args.locked is True:
            query.add_isnot_field('lock_id', None)
        else:
            query.add_is_field('lock_id', None)
    if args.dvg_name is not None:
        query.add_is_field('dvg_name', args.dvg_name)
    dpvs = query.query()
    summary_fields = (
        'dpv_name',
        'total_size',
        'free_size',
        'status',
        'dvg_name',
        'lock_id',
        'lock_timestamp',
    )
    schema = DpvApiSchema(only=summary_fields, many=True)
    return ApiRet(dpvs, schema)


dpvs_get_method = ApiMethod[DpvsGetArgs, EmptyPath](
    api_dpvs_get, HttpStatus.OK, arg_info=dpvs_get_args_info)

dpvs_path_template = '/dpvs'
dpvs_path_type = EmptyPath

dpvs_res = ApiResource[EmptyPath](
    dpvs_path_template, dpvs_path_type,
    get=dpvs_get_method,
)

api.add_resource(dpvs_res)
