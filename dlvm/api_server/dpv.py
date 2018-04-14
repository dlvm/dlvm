import sys

from sqlalchemy.exc import IntegrityError
from marshmallow import Schema, fields
from marshmallow.validate import Range, OneOf
from marshmallow_enum import EnumField

from dlvm.common.configure import cfg
from dlvm.common.utils import HttpStatus, ExcInfo
import dlvm.common.error as error
from dlvm.hook.api_wrapper import ArgLocation, ArgInfo, ApiRet, \
    ApiMethod, ApiResource
from dlvm.hook.local_ctx import frontend_local
from dlvm.hook.rpc_wrapper import DpvClient
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
    session = frontend_local.session
    args = frontend_local.args
    query = GeneralQuery(session, DistributePhysicalVolume)
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


class DpvsPostArgSchema(Schema):
    dpv_name = fields.String(required=True)


dpvs_post_args_info = ArgInfo(DpvsPostArgSchema, ArgLocation.json)


def dpvs_post():
    session = frontend_local.session
    args = frontend_local.args
    dpv_name = args['dpv_name']
    client = DpvClient(dpv_name)
    try:
        dpv_info = client.dpv_get_info()
        total_size = dpv_info['total_size']
        free_size = dpv_info['free_size']
        assert(total_size == free_size)
    except Exception:
        raise error.DpvError(dpv_name)
    dpv = DistributePhysicalVolume(
        dpv_name=dpv_name,
        total_size=total_size,
        free_size=free_size,
        status=DpvStatus.available,
    )
    session.add(dpv)
    try:
        session.commit()
    except IntegrityError:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        raise error.ResourceDuplicateError('dpv', dpv_name, exc_info)
    return None


dpvs_post_method = ApiMethod(dpvs_post, HttpStatus.OK, dpvs_post_args_info)


dpvs_res = ApiResource(
    '/dpvs',
    get=dpvs_get_method, post=dpvs_post_method)


class DpvGetArgSchema(Schema):
    detail = fields.Boolean(missing=False)


dpv_get_args_info = ArgInfo(DpvGetArgSchema, ArgLocation.args)


def dpv_get(dpv_name):
    session = frontend_local.session
    args = frontend_local.args
    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(dpv_name=dpv_name) \
        .one_or_none()
    if dpv is None:
        raise error.ResourceNotFoundError(
            'dpv', dpv_name)
    if args['detail'] is True:
        schema = DpvApiSchema(only=DPV_SUMMARY_FIELDS, many=False)
    else:
        schema = DpvApiSchema(many=False)
    return ApiRet(dpv, schema)


dpv_get_method = ApiMethod(dpv_get, HttpStatus.OK, dpv_get_args_info)


def dpv_delete(dpv_name):
    session = frontend_local.session
    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(dpv_name=dpv_name) \
        .with_lockmode('update') \
        .one_or_none()
    if dpv is None:
        return None
    if dpv.dvg_name is not None:
        raise error.ResourceBusyError(
            'dpv', dpv_name, 'dvg', dpv.dvg_name)
    session.delete(dpv)
    session.commit()
    return None


dpv_delete_method = ApiMethod(dpv_delete, HttpStatus.OK)


dpv_res = ApiResource(
    '/dpvs/<dpv_name>',
    get=dpv_get_method,
    delete=dpv_delete_method)
