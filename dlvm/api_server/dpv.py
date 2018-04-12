from flask import g
from marshmallow import Schema, fields
from marshmallow.validate import Range, OneOf
from marshmallow_enum import EnumField

from dlvm.common.configure import cfg
from dlvm.common.utils import HttpStatus
from dlvm.hook.api_wrapper import ArgLocation, ArgInfo, ApiRet, \
    ApiMethod, ApiResource
from dlvm.core.modules import DistributePhysicalVolume, DpvStatus
from dlvm.core.schema import DpvApiSchema
from dlvm.core.helper import GeneralQuery


DPV_SUMMARY_FIELDS = (
    'dpv_name',
    'total_size',
    'free_size',
    'status',
    'dvg_name',
    'lock_id',
    'lock_timestamp',
)

DPV_ORDER_FIELDS = ('dpv_name', 'total_size', 'free_size')
DPV_LIST_LIMIT = cfg.getint('api', 'list_limit')


class DpvsGetArgSchema(Schema):
    order_by = fields.String(
        missing=DPV_ORDER_FIELDS[0], validate=OneOf(DPV_ORDER_FIELDS))
    reverse = fields.Boolean(missing=False)
    offset = fields.Integer(missing=0, validate=Range(0))
    limit = fields.Integer(
        missing=DPV_LIST_LIMIT, validate=Range(0, DPV_LIST_LIMIT))
    status = EnumField(DpvStatus, missing=None)
    locked = fields.Boolean(missing=None)
    dvg_name = fields.String(missing=None)


dpvs_get_args_info = ArgInfo(DpvsGetArgSchema, ArgLocation.args)


def dpvs_get():
    work_ctx = g.work_ctx
    args = g.args
    query = GeneralQuery(work_ctx.session, DistributePhysicalVolume)
    query.add_order_field(args['order_by'], args['reverse'])
    query.set_offset(args['offset'])
    query.set_limit(args['limit'])
    if args['status'] is not None:
        query.add_is_field('status', args['status'])
    if args['locked'] is not None:
        if args['locked'] is True:
            query.add_isnot_field('lock_id', None)
        else:
            query.add_is_field('lock_id', None)
    if args['dvg_name'] is not None:
        query.add_is_field('dvg_name', args['dvg_name'])
    dpvs = query.query()
    schema = DpvApiSchema(only=DPV_SUMMARY_FIELDS, many=True)
    return ApiRet(dpvs, schema)


dpvs_get_method = ApiMethod(dpvs_get, HttpStatus.OK, dpvs_get_args_info)

dpvs_res = ApiResource('/dpvs', get=dpvs_get_method)
