import sys
from sqlalchemy.exc import IntegrityError
from marshmallow import fields
from marshmallow.validate import Length, Regexp

from dlvm.common.constant import RES_NAME_REGEX, RES_NAME_LENGTH
from dlvm.common.utils import HttpStatus, ExcInfo
from dlvm.common.marshmallow_ext import NtSchema
import dlvm.common.error as error
from dlvm.common.modules import DistributePhysicalVolume, \
    DistributeVolumeGroup, DistributeLogicalVolume
from dlvm.common.schema import DvgSchema
from dlvm.common.database import GeneralQuery
from dlvm.wrapper.api_wrapper import ArgLocation, ArgInfo, \
    ApiMethod, ApiResource
from dlvm.wrapper.local_ctx import frontend_local


def dvgs_get():
    session = frontend_local.session
    query = GeneralQuery(session, DistributeVolumeGroup)
    dvgs = query.query()
    return DvgSchema(many=True).dump(dvgs)


dvgs_get_method = ApiMethod(dvgs_get, HttpStatus.OK)


class DvgsPostArgSchema(NtSchema):
    dvg_name = fields.String(
        required=True, validate=[
            Length(1, RES_NAME_LENGTH), Regexp(RES_NAME_REGEX)])


dvgs_post_arg_info = ArgInfo(DvgsPostArgSchema, ArgLocation.body)


def dvgs_post():
    session = frontend_local.session
    arg = frontend_local.arg
    dvg_name = arg.dvg_name
    dvg = DistributeVolumeGroup(
        dvg_name=dvg_name,
        total_size=0,
        free_size=0,
    )
    session.add(dvg)
    try:
        session.commit()
    except IntegrityError:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        raise error.ResourceDuplicateError('dvg', dvg_name, exc_info)
    return None


dvgs_post_method = ApiMethod(dvgs_post, HttpStatus.OK, dvgs_post_arg_info)


dvgs_res = ApiResource(
    '/dvgs',
    get=dvgs_get_method, post=dvgs_post_method)


def dvg_get(dvg_name):
    session = frontend_local.session
    dvg = session.query(DistributeVolumeGroup) \
        .filter_by(dvg_name=dvg_name) \
        .one_or_none()
    if dvg is None:
        raise error.ResourceNotFoundError(
            'dvg', dvg_name)
    return DvgSchema(many=False).dump(dvg)


dvg_get_method = ApiMethod(dvg_get, HttpStatus.OK)


def dvg_delete(dvg_name):
    session = frontend_local.session
    dvg = session.query(DistributeVolumeGroup) \
        .filter_by(dvg_name=dvg_name) \
        .with_lockmode('update') \
        .one_or_none()
    if dvg is None:
        return None
    dpv = dvg \
        .dpvs \
        .with_entities(DistributePhysicalVolume.dpv_name) \
        .limit(1) \
        .one_or_none()
    if dpv is not None:
        raise error.ResourceBusyError(
            'dvg', dvg_name, 'dpv', dpv.dpv_name)

    dlv = dvg \
        .dlvs \
        .with_entities(DistributeLogicalVolume.dlv_name) \
        .limit(1) \
        .one_or_none()
    if dlv is not None:
        raise error.ResourceBusyError(
            'dvg', dvg_name, 'dlv', dlv.dlv_name)

    assert(dvg.total_size == 0)
    assert(dvg.free_size == 0)
    session.delete(dvg)
    session.commit()


dvg_delete_method = ApiMethod(dvg_delete, HttpStatus.OK)


dvg_res = ApiResource(
    '/dvgs/<dvg_name>',
    get=dvg_get_method,
    delete=dvg_delete_method)


class DvgExtendArgSchema(NtSchema):
    dpv_name = fields.String(required=True)


dvg_extend_arg_info = ArgInfo(DvgExtendArgSchema, ArgLocation.body)


def dvg_extend(dvg_name):
    session = frontend_local.session
    arg = frontend_local.arg
    dpv_name = arg.dpv_name
    dvg = session.query(DistributeVolumeGroup) \
        .filter_by(dvg_name=dvg_name) \
        .with_lockmode('update') \
        .one_or_none()
    if dvg is None:
        raise error.ResourceNotFoundError(
            'dvg', dvg_name)

    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(dpv_name=dpv_name) \
        .with_lockmode('update') \
        .one_or_none()
    if dpv is None:
        raise error.ResourceNotFoundError(
            'dpv', dpv_name)

    if dpv.dvg_name is not None:
        raise error.ResourceBusyError(
            'dpv', dpv_name, 'dvg', dvg.dvg_name)

    if len(dpv.legs) != 0:
        raise error.ResourceBusyError(
            'dpv', dpv_name, 'leg', dpv.legs[0].leg_id)

    assert(dpv.total_size == dpv.free_size)

    dpv.dvg_name = dvg_name
    dvg.total_size += dpv.total_size
    dvg.free_size += dpv.free_size
    session.add(dpv)
    session.add(dvg)
    session.commit()
    return None


dvg_extend_method = ApiMethod(dvg_extend, HttpStatus.OK, dvg_extend_arg_info)
dvg_extend_res = ApiResource(
    '/dvgs/<dvg_name>/extend', put=dvg_extend_method)


class DvgReduceArgSchema(NtSchema):
    dpv_name = fields.String(required=True)


dvg_reduce_arg_info = ArgInfo(DvgReduceArgSchema, ArgLocation.body)


def dvg_reduce(dvg_name):
    session = frontend_local.session
    arg = frontend_local.arg
    dpv_name = arg.dpv_name
    dvg = session.query(DistributeVolumeGroup) \
        .filter_by(dvg_name=dvg_name) \
        .with_lockmode('update') \
        .one_or_none()
    if dvg is None:
        raise error.ResourceNotFoundError(
            'dvg', dvg_name)

    dpv = session.query(DistributePhysicalVolume) \
        .filter_by(dpv_name=dpv_name) \
        .with_lockmode('update') \
        .one_or_none()
    if dpv is None:
        raise error.ResourceNotFoundError(
            'dpv', dpv_name)

    if dpv.dvg_name is None:
        return None
    if dpv.dvg_name != dvg_name:
        raise error.ResourceInvalidError(
            'dpv', dpv_name, 'dvg', dpv.dvg_name)

    if len(dpv.legs) != 0:
        raise error.ResourceBusyError(
            'dpv', dpv_name, 'leg', dpv.legs[0].leg_id)

    assert(dpv.total_size == dpv.free_size)
    dpv.dvg_name = None
    dvg.total_size -= dpv.total_size
    dvg.free_size -= dpv.free_size
    session.add(dpv)
    session.add(dvg)
    session.commit()
    return None


dvg_reduce_method = ApiMethod(dvg_reduce, HttpStatus.OK, dvg_reduce_arg_info)
dvg_reduce_res = ApiResource(
    '/dvgs/<dvg_name>/reduce', put=dvg_reduce_method)
