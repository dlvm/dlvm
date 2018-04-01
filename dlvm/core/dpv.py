from typing import Optional, List
import traceback

from sqlalchemy.exc import IntegrityError

from dlvm.common.utils import RequestContext, WorkContext
from dlvm.common.error import DpvError, ResourceDuplicateError, \
    ResourceNotFoundError, ResourceBusyError
from dlvm.core.helper import GeneralQuery, DpvClient
from dlvm.core.modules import DistributePhysicalVolume, DistributeVolumeGroup


def dpv_list(
        req_ctx: RequestContext,
        work_ctx: WorkContext,
        order_by: str,
        reverse: bool,
        offset: int,
        limit: int,
        status: Optional[str],
        locked: Optional[bool],
        dvg_name: Optional[str],
)-> List[DistributePhysicalVolume]:
    query = GeneralQuery(work_ctx.session, DistributePhysicalVolume)
    query.add_order_field(order_by, reverse)
    query.set_offset(offset)
    query.set_limit(limit)
    if status is not None:
        query.add_is_field('status', status)
    if locked is not None:
        if locked is True:
            query.add_isnot_field('lock_id', None)
        else:
            query.add_is_field('lock_id', None)
    if dvg_name is not None:
        query.add_is_field('dvg_name', dvg_name)
    return query.query()


def dpv_create(
        req_ctx: RequestContext,
        work_ctx: WorkContext,
        dpv_name: str)-> None:
    client = DpvClient(req_ctx, dpv_name, 0)
    try:
        ret = client.get_size()
        dpv_info, = ret.get_value()
        total_size = dpv_info['total_size']
        free_size = dpv_info['free_size']
        assert(total_size == free_size)
    except Exception:
        raise DpvError(dpv_name)
    dpv = DistributePhysicalVolume(
        dpv_name=dpv_name,
        total_size=total_size,
        free_size=free_size,
        status='available',
    )
    work_ctx.session.add(dpv)
    try:
        work_ctx.session.commit()
    except IntegrityError:
        raise ResourceDuplicateError('dpv', dpv_name, traceback.format_exc())
    return None


def dpv_show(
        req_ctx: RequestContext,
        work_ctx: WorkContext,
        dpv_name: str)-> DistributePhysicalVolume:
    dpv = work_ctx.session.query(DistributePhysicalVolume) \
          .filter_by(dpv_name=dpv_name) \
          .one_or_none()
    if dpv is None:
        raise ResourceNotFoundError(
            'dpv', dpv_name)
    return dpv


def dpv_delete(
        req_ctx: RequestContext,
        work_ctx: WorkContext,
        dpv_name: str)-> None:
    dpv = work_ctx.session.query(DistributePhysicalVolume) \
          .filter_by(dpv_name=dpv_name) \
          .one_or_none()
    if dpv is None:
        return None
    if dpv.dvg_name is not None:
        raise ResourceBusyError(
            'dpv', dpv_name, 'dvg', dpv.dvg_name)
    work_ctx.session.delete(dpv)
    work_ctx.session.commit()
    return None


def dpv_resize(
        req_ctx: RequestContext,
        work_ctx: WorkContext,
        dpv_name: str)-> None:
    dpv = work_ctx.session.query(DistributePhysicalVolume) \
          .filter_by(dpv_name=dpv_name) \
          .one_or_none()
    if dpv is None:
        raise ResourceNotFoundError(
            'dpv', dpv_name)

    client = DpvClient(req_ctx, dpv_name, 0)
    try:
        ret = client.get_size()
        dpv_info, = ret.get_value()
        total_size = dpv_info['total_size']
        free_size = dpv_info['free_size']
        total_delta = total_size - dpv.total_size
        free_delta = free_size - dpv.free_size
        assert(total_delta == free_delta)
    except Exception:
        raise DpvError(dpv_name)

    dpv.total_size = total_size
    dpv.free_size = free_size
    work_ctx.session.add(dpv)

    if dpv.dvg_name is not None:
        dvg = work_ctx.session.query(DistributeVolumeGroup) \
              .with_lockmode('update') \
              .filter_by(dvg_name=dpv.dvg_name) \
              .one()
        dvg.total_size += total_delta
        dvg.free_size += free_delta
        work_ctx.session.add(dvg)

    work_ctx.session.commit()
    return None
