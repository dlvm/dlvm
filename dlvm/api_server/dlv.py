import sys
from sqlalchemy.exc import IntegrityError
from marshmallow import fields
from marshmallow.validate import Length, Regexp, OneOf, Range

from dlvm.common.constant import RES_NAME_REGEX, RES_NAME_LENGTH
from dlvm.common.configure import cfg
from dlvm.common.utils import HttpStatus, ExcInfo
from dlvm.common.marshmallow_ext import NtSchema, EnumField
import dlvm.common.error as error
from dlvm.common.modules import DistributePhysicalVolume, \
    DistributeVolumeGroup, DistributeLogicalVolume, DlvStatus
from dlvm.common.db_schema import DlvSummarySchema, DlvSchema
from dlvm.common.database import GeneralQuery
from dlvm.wrapper.api_wrapper import ArgLocation, ArgInfo, \
    ApiMethod, ApiResource
from dlvm.wrapper.local_ctx import frontend_local


DLV_ORDER_FIELDS = ('dlv_name', 'dlv_size', 'data_size')
DLV_LIST_LIMIT = cfg.getint('api', 'list_limit')


class DlvsGetArgSchema(NtSchema):
    order_by = fields.String(
        missing=DLV_ORDER_FIELDS[0], validate=OneOf(DLV_ORDER_FIELDS))
    reverse = fields.Boolean(missing=False)
    offset = fields.Integer(missing=0, validate=Range(0))
    limit = fields.Integer(
        missing=DLV_LIST_LIMIT, validate=Range(0, DLV_LIST_LIMIT))
    status = EnumField(DlvStatus, missing=None)
    locked = fields.Boolean(missing=None)
    dvg_name = fields.String(missing=None)


dlvs_get_arg_info = ArgInfo(DlvsGetArgSchema, ArgLocation.query)


def dlvs_get():
    session = frontend_local.session
    arg = frontend_local.arg
    query = GeneralQuery(session, DistributeLogicalVolume)
    query.add_order_field(arg.order_by, arg.reverse)
    query.set_offset(arg.offset)
    query.set_limit(arg.limit)
    if arg.status is not None:
        query.add_is_field('status', arg.status)
    if arg.locked is not None:
        if arg.locked is True:
            query.add_isnot_field('lock_id', None)
        else:
            query.add_is_field('lock_id', None)
    if arg.dvg_name is not None:
        query.add_is_field('dvg_name', arg.dvg_name)
    dlvs = query.query()
    return DlvSummarySchema(many=True).dump(dlvs)


dlvs_get_method = ApiMethod(dlvs_get, HttpStatus.OK, dlvs_get_arg_info)


def dlv_size_validator(dlv_size):
    pass


def stripe_number_validator(stripe_number):
    pass


def init_size_validator(init_size):
    pass


class DlvsPostArgSchema(NtSchema):
    dlv_name = fields.String(
        required=True, validate=[
            Length(1, RES_NAME_LENGTH), Regexp(RES_NAME_REGEX)])
    dlv_size = fields.Integer(
        required=True, validate=dlv_size_validator)
    stripe_number = fields.Integer(
        required=True, validate=stripe_number_validator)
    init_size = fields.Integer(
        required=True, validate=init_size_validator)
    dvg_name = fields.String(required=True)


dlvs_post_arg_info = ArgInfo(DlvsPostArgSchema, ArgLocation.body)


def dlvs_post():
    pass


dlvs_post_method = ApiMethod(dlvs_post, HttpStatus.Created, dlvs_post_arg_info)


dlvs_res = ApiResource(
    '/dlvs',
    get=dlvs_get_method, post=dlvs_post_method)
