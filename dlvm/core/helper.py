from typing import Type, Optional, MutableSequence, MutableMapping, \
    List, Tuple

from sqlalchemy.orm.session import Session

from dlvm.common.utils import RequestContext
from dlvm.common.configure import cfg
from dlvm.common.database import engine
from dlvm.hook.rpc_wrapper import RpcClient
from dlvm.core.modules import Base, FieldType


class GeneralQuery():

    def __init__(self, session: Session, obj_cls: Type[Base])-> None:
        self.session = session
        self.obj_cls = obj_cls
        self.order_fields: MutableSequence[Tuple[str, bool]] = []
        self.is_fields: MutableMapping[str, FieldType] = {}
        self.isnot_fields: MutableMapping[str, FieldType] = {}
        self.offset: Optional[int] = None
        self.limit: Optional[int] = None

    def set_offset(self, offset: int)-> None:
        self.offset = offset

    def set_limit(self, limit: int)-> None:
        self.limit = limit

    def add_order_field(self, order_name: str, reverse: bool)-> None:
        self.order_fields.append((order_name, reverse))

    def add_is_field(self, field_name: str, value: FieldType)-> None:
        self.is_fields[field_name] = value

    def add_isnot_field(self, field_name: str, value: FieldType)-> None:
        self.isnot_fields[field_name] = value

    def query(self)-> List[Base]:
        query = self.session.query(self.obj_cls)
        filter_list = []
        for order_name, reverse in self.order_fields:
            order_field = getattr(self.obj_cls, order_name)
            if reverse is True:
                order_attr = order_field.desc()
            else:
                order_attr = order_field.asc()
            filter_list.append(order_attr)
        query = query.order_by(*filter_list)
        for field_name in self.is_fields:
            field = getattr(self.obj_cls, field_name)
            value = self.is_fields[field_name]
            query = query.filter(field.is_(value))
        for field_name in self.isnot_fields:
            field = getattr(self.obj_cls, field_name)
            value = self.isnot_fields[field_name]
            query = query.filter(field.isnot(value))
        if self.offset is not None:
            query = query.offset(self.offset)
        if self.limit is not None:
            query = query.limit(self.limit)
        return query.all()


class DpvClient(RpcClient):

    def __init__(
            self, req_ctx: RequestContext, dpv_name: str,
            expire_time: int)-> None:
        super(DpvClient, self).__init__(
            req_ctx, dpv_name, cfg.dpv_port,
            expire_time, cfg.dpv_timeout)


class IhostClient(RpcClient):

    def __init__(
            self, req_ctx: RequestContext, ihost_name: str,
            expire_time: int)-> None:
        super(IhostClient, self).__init__(
            req_ctx, ihost_name, cfg.ihost_port,
            expire_time, cfg.ihost_timeout)


def create_all()-> None:
    Base.metadata.create_all(engine)


def drop_all()-> None:
    Base.metadata.drop_all(engine)
