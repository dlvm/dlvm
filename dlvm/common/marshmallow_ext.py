from collections import namedtuple

from marshmallow import Schema, fields, post_load, ValidationError
from marshmallow.schema import SchemaMeta


class SetField(fields.List):

    def _deserialize(self, value, attr, data):
        ret = super(SetField, self)._deserialize(value, attr, data)
        return set(ret)


class EnumField(fields.Field):

    def __init__(self, enum_cls, *args, **kwargs):
        self.enum_cls = enum_cls
        super(EnumField, self).__init__(*args, **kwargs)

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        else:
            return value.value

    def _deserialize(self, value, attr, data):
        if value is None:
            return None
        else:
            try:
                return self.enum_cls(value)
            except ValueError as e:
                raise ValidationError(str(e))


class NtSchemaMeta(SchemaMeta):

    def __new__(mcs, name, bases, attrs):
        cls = super(NtSchemaMeta, mcs).__new__(mcs, name, bases, attrs)
        attr_list = []
        for key in cls._declared_fields:
            attr_list.append(key)
        if name.endswith('Schema'):
            nt_name = name[:-6]
        else:
            nt_name = name
        cls.nt = namedtuple(nt_name, attr_list)
        cls.nt.__new__.__defaults__ = (None,) * len(attr_list)

        @post_load
        def load_nt(self, data):
            return self.nt(**data)
        cls.load_nt = load_nt

        return cls


class NtSchema(Schema, metaclass=NtSchemaMeta):
    pass
