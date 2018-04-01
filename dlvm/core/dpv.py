from typing import Optional, List
import traceback

from sqlalchemy.exc import IntegrityError

from dlvm.common.utils import RequestContext, WorkContext
from dlvm.common.error import DpvError, ResourceDuplicateError
from dlvm.core.helper import GeneralQuery, DpvClient
from dlvm.core.modules import DistributePhysicalVolume


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
    query.add_offset(offset)
    query.add_limit(limit)
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
