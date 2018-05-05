import sys
import uuid
from datetime import datetime
import zlib

from sqlalchemy.exc import IntegrityError
from marshmallow import fields
from marshmallow.validate import Length, Regexp, OneOf, Range

from dlvm.common.constant import RES_NAME_REGEX, RES_NAME_LENGTH, \
    DEFAULT_SNAP_NAME
from dlvm.common.configure import cfg
from dlvm.common.utils import HttpStatus, ExcInfo, get_empty_thin_mapping, \
    div_round_up
from dlvm.common.marshmallow_ext import NtSchema, EnumField
import dlvm.common.error as error
from dlvm.common.modules import DistributeLogicalVolume, Snapshot, \
    DlvStatus, SnapStatus, Lock, LockType, Group, Leg
from dlvm.common.db_schema import DlvSummarySchema, DlvSchema
from dlvm.common.database import GeneralQuery
from dlvm.wrapper.api_wrapper import ArgLocation, ArgInfo, \
    ApiMethod, ApiResource
from dlvm.wrapper.local_ctx import frontend_local
from dlvm.wrapper.action_check import Action, run_checker, add_checker
from dlvm.worker.dlv import DlvCreate, DlvDelete


thin_meta_factor = cfg.getint('device_mapper', 'thin_meta_factor')
thin_block_size = cfg.getsize('device_mapper', 'thin_block_size')
lvm_unit = cfg.getsize('device_mapper', 'lvm_unit')
thin_meta_min = cfg.getsize('device_mapper', 'thin_meta_min')
thin_block_size = cfg.getsize('device_mapper', 'thin_block_size')
mirror_meta_blocks = cfg.getint('device_mapper', 'mirror_meta_blocks')
mirror_meta_size = thin_block_size * mirror_meta_blocks


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
    bm_ignore = fields.Boolean(missing=False)
    dvg_name = fields.String(required=True)


dlvs_post_arg_info = ArgInfo(DlvsPostArgSchema, ArgLocation.body)


def dlvs_post():
    session = frontend_local.session
    arg = frontend_local.arg

    lock_owner = uuid.uuid4().hex
    lock_dt = datetime.utcnow()
    lock = Lock(
        lock_owner=lock_owner,
        lock_type=LockType.dlv,
        lock_dt=lock_dt,
    )
    session.add(lock)

    dlv = DistributeLogicalVolume(
        dlv_name=arg.dlv_name,
        dlv_size=arg.dlv_size,
        data_size=arg.init_size,
        stripe_number=arg.stripe_number,
        status=DlvStatus.creating,
        bm_dirty=False,
        bm_ignore=arg.bm_ignore,
        dvg_name=arg.dvg_name,
        active_snap_name=DEFAULT_SNAP_NAME,
        lock=lock,
    )
    session.add(dlv)

    snap_id = '{0}/{1}'.format(arg.dlv_name, DEFAULT_SNAP_NAME)
    thin_mapping_str = get_empty_thin_mapping(
        thin_block_size, arg.init_size//thin_block_size)
    thin_mapping = zlib.compress(thin_mapping_str.encode('utf-8'))
    snap = Snapshot(
        snap_id=snap_id,
        snap_name=DEFAULT_SNAP_NAME,
        thin_id=0,
        ori_thin_id=0,
        status=SnapStatus.available,
        thin_mapping=thin_mapping,
        dlv_name=arg.dlv_name,
    )
    session.add(snap)

    thin_meta_size = thin_meta_factor * div_round_up(
        arg.dlv_size, thin_block_size)
    if thin_meta_size < thin_meta_min:
        thin_meta_size = thin_meta_min
    group = Group(
        group_idx=0,
        group_size=thin_meta_size,
        dlv_name=arg.dlv_name,
        bitmap=bytes(),
    )
    session.add(group)
    leg_size = thin_meta_size + mirror_meta_size
    leg_size = div_round_up(leg_size, lvm_unit) * lvm_unit
    for i in range(2):
        leg = Leg(
            leg_idx=i,
            group=group,
            leg_size=leg_size,
        )
        session.add(leg)

    group_size = arg.init_size
    bm_len_8 = div_round_up(group_size, thin_block_size)
    bm_len = div_round_up(bm_len_8, 8)
    bitmap = bytes([0x0 for i in range(bm_len)])
    group = Group(
        group_idx=1,
        group_size=group_size,
        dlv_name=arg.dlv_name,
        bitmap=bitmap,
    )
    session.add(group)
    leg_size = div_round_up(
        group_size, arg.stripe_number) + mirror_meta_size
    leg_size = div_round_up(leg_size, lvm_unit) * lvm_unit
    legs_per_group = 2 * arg.stripe_number
    for i in range(legs_per_group):
        leg = Leg(
            leg_idx=i,
            group=group,
            leg_size=leg_size,
        )
        session.add(leg)

    try:
        session.commit()
    except IntegrityError:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        raise error.ResourceDuplicateError('dlv', arg.dlv_name, exc_info)

    sm = DlvCreate(arg.dlv_name)
    sm.start(lock)


dlvs_post_method = ApiMethod(dlvs_post, HttpStatus.Created, dlvs_post_arg_info)


dlvs_res = ApiResource(
    '/dlvs',
    get=dlvs_get_method, post=dlvs_post_method)


def dlv_get(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .one_or_none()
    if dlv is None:
        raise error.ResourceNotFoundError(
            'dlv', dlv_name)
    return DlvSchema(many=False).dump(dlv)


dlv_get_method = ApiMethod(dlv_get, HttpStatus.OK)


def dlv_delete(dlv_name):
    session = frontend_local.session
    dlv = session.query(DistributeLogicalVolume) \
        .filter_by(dlv_name=dlv_name) \
        .with_lockmode('update') \
        .one_or_none()
    if dlv is None:
        return None

    run_checker(Action.dlv_delete, dlv)

    lock_owner = uuid.uuid4().hex
    lock_dt = datetime.utcnow()
    lock = Lock(
        lock_owner=lock_owner,
        lock_type=LockType.dlv,
        lock_dt=lock_dt,
    )
    session.add(lock)

    dlv.status = DlvStatus.deleting
    dlv.lock = lock
    session.add(dlv)
    session.commit()

    sm = DlvDelete(dlv.dlv_name)
    sm.start(lock)


dlv_delete_method = ApiMethod(dlv_delete, HttpStatus.Created)

dlv_res = ApiResource(
    '/dlvs/<dlv_name>',
    get=dlv_get_method,
    delete=dlv_delete_method)


@add_checker(Action.dlv_delete)
def normal_checker(dlv):
    if dlv.status != DlvStatus.available:
        reason = 'status is {0}'.format(dlv.status)
        raise error.CheckerError('dlv', dlv.dlv_name, reason)
    if dlv.lock_id is not None:
        reason = 'has lock {0}'.format(dlv.lock_id)
        raise error.CheckerError('dlv', dlv.dlv_name, reason)
