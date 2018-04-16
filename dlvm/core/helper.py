from dlvm.common.database import engine
from dlvm.core.modules import Base


class GeneralQuery():

    def __init__(self, session, obj_cls):
        self.session = session
        self.obj_cls = obj_cls
        self.order_fields = []
        self.is_fields = {}
        self.isnot_fields = {}
        self.offset = None
        self.limit = None

    def set_offset(self, offset):
        self.offset = offset

    def set_limit(self, limit):
        self.limit = limit

    def add_order_field(self, order_name, reverse):
        self.order_fields.append((order_name, reverse))

    def add_is_field(self, field_name, value):
        self.is_fields[field_name] = value

    def add_isnot_field(self, field_name, value):
        self.isnot_fields[field_name] = value

    def query(self):
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


def create_all():
    Base.metadata.create_all(engine)


def drop_all():
    Base.metadata.drop_all(engine)
