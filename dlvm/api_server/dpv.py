import sys

from sqlalchemy.exc import IntegrityError
from marshmallow import fields
from marshmallow.validate import Range, OneOf

from dlvm.common.configure import cfg
from dlvm.common.utils import HttpStatus, ExcInfo
from dlvm.common.marshmallow_ext import NtSchema, EnumField
import dlvm.common.error as error
from dlvm.common.modules import DistributePhysicalVolume, \
    DpvStatus, DistributeVolumeGroup
from dlvm.common.db_schema import DpvSummarySchema, DpvSchema
from dlvm.common.database import GeneralQuery
from dlvm.wrapper.api_wrapper import ArgLocation, ArgInfo, \
    ApiMethod, ApiResource
from dlvm.wrapper.local_ctx import frontend_local
from dlvm.dpv_agent import dpv_rpc


DPV_ORDER_FIELDS = ('dpv_name', 'total_size', 'free_size')
DPV_LIST_LIMIT = cfg.getint('api', 'list_limit')


class DpvsGetArgSchema(NtSchema):
    order_by = fields.String(
        missing=DPV_ORDER_FIELDS[0], validate=OneOf(DPV_ORDER_FIELDS))
    reverse = fields.Boolean(missing=False)
    offset = fields.Integer(missing=0, validate=Range(0))
    limit = fields.Integer(
        missing=DPV_LIST_LIMIT, validate=Range(0, DPV_LIST_LIMIT))
    dpv_status = EnumField(DpvStatus, missing=None)
    locked = fields.Boolean(missing=None)
    dvg_name = fields.String(missing=None)


dpvs_get_arg_info = ArgInfo(DpvsGetArgSchema, ArgLocation.query)


def dpvs_get():
    session = frontend_local.session
    arg = frontend_local.arg
    query = GeneralQuery(session, DistributePhysicalVolume)
    query.add_order_field(arg.order_by, arg.reverse)
    query.set_offset(arg.offset)
    query.set_limit(arg.limit)
    if arg.dpv_status is not None:
        query.add_is_field('dpv_status', arg.service_status)
    if arg.locked is not None:
        if arg.locked is True:
            query.add_isnot_field('lock_id', None)
        else:
            query.add_is_field('lock_id', None)
    if arg.dvg_name is not None:
        query.add_is_field('dvg_name', arg.dvg_name)
    dpvs = query.query()
    return DpvSummarySchema(many=True).dump(dpvs)


dpvs_get_method = ApiMethod(dpvs_get, HttpStatus.OK, dpvs_get_arg_info)


class DpvsPostArgSchema(NtSchema):
    dpv_name = fields.String(required=True)
    location = fields.String(missing=None)


dpvs_post_arg_info = ArgInfo(DpvsPostArgSchema, ArgLocation.body)


def dpvs_post():
    session = frontend_local.session
    arg = frontend_local.arg
    dpv_name = arg.dpv_name
    location = arg.location
    client = dpv_rpc.sync_client(dpv_name)
    try:
        dpv_info = client.dpv_get_info()
        total_size = dpv_info.total_size
        free_size = dpv_info.free_size
        assert(total_size == free_size)
    except error.RpcError:
        raise error.DpvError(dpv_name)
    dpv = DistributePhysicalVolume(
        dpv_name=dpv_name,
        total_size=total_size,
        free_size=free_size,
        dpv_status=DpvStatus.available,
        location=location,
    )
    session.add(dpv)
    try:
        session.commit()
    except IntegrityError:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        raise error.ResourceDuplicateError('dpv', dpv_name, exc_info)
    return None


dpvs_post_method = ApiMethod(dpvs_post, HttpStatus.OK, dpvs_post_arg_info)


dpvs_res = ApiResource(
    '/dpvs',
    get=dpvs_get_method, post=dpvs_post_method)


def dpv_get(dpv_name):
    session = frontend_local.session
    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(dpv_name=dpv_name) \
        .one_or_none()
    if dpv is None:
        raise error.ResourceNotFoundError(
            'dpv', dpv_name)
    return DpvSchema(many=False).dump(dpv)


dpv_get_method = ApiMethod(dpv_get, HttpStatus.OK)


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


def dpv_update(dpv_name):
    session = frontend_local.session
    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(dpv_name=dpv_name) \
        .with_lockmode('update') \
        .one_or_none()
    if dpv is None:
        raise error.ResourceNotFoundError(
            'dpv', dpv_name)

    client = dpv_rpc.sync_client(dpv_name)
    try:
        dpv_info = client.dpv_get_info()
        total_size = dpv_info.total_size
        free_size = dpv_info.free_size
        total_delta = total_size - dpv.total_size
        free_delta = free_size - dpv.free_size
        assert(total_delta == free_delta)
    except error.RpcError:
        raise error.DpvError(dpv_name)

    dpv.total_size = total_size
    dpv.free_size = free_size
    session.add(dpv)

    if dpv.dvg_name is not None:
        dvg = session.query(DistributeVolumeGroup) \
              .with_lockmode('update') \
              .filter_by(dvg_name=dpv.dvg_name) \
              .one()
        dvg.total_size += total_delta
        dvg.free_size += free_delta
        session.add(dvg)

    session.commit()
    return None


dpv_update_method = ApiMethod(dpv_update, HttpStatus.OK)
dpv_update_res = ApiResource(
    '/dpvs/<dpv_name>/update', put=dpv_update_method)
