from marshmallow import Schema, fields, post_load, post_dump
from marshmallow_enum import EnumField

from dlvm.common.modules import ServiceStatus, DiskStatus, \
    DlvStatus, SnapStatus


class GroupSummarySchema(Schema):
    group_id = fields.UUID()
    dlv_name = fields.String()


class LegApiSchema(Schema):
    leg_id = fields.UUID()
    leg_idx = fields.Integer()
    leg_size = fields.Integer()
    group_id = fields.String()
    dpv_name = fields.String()
    group_summary = fields.Nested(GroupSummarySchema, many=False)


class LegRpcSchema(Schema):
    leg_id = fields.UUID()
    leg_idx = fields.Integer()
    leg_size = fields.Integer()

    @post_dump
    def dump_leg(self, data):
        data['leg_size'] = str(data['leg_size'])

    @post_load
    def make_leg(self, data):
        data['leg_size'] = int(data['leg_size'])
        return data


class GroupApiSchema(Schema):
    group_id = fields.UUID()
    group_idx = fields.Integer()
    group_size = fields.Integer()
    dlv_name = fields.String()
    legs = fields.Nested(LegApiSchema, many=True)


class GroupRpcSchema(Schema):
    group_id = fields.UUID()
    group_idx = fields.Integer()
    group_size = fields.Integer()
    legs = fields.Nested(LegRpcSchema, many=True)

    @post_dump
    def dump_group(self, data):
        data['group_size'] = str(data['group_size'])

    @post_load
    def make_group(self, data):
        data['group_size'] = int(data['group_size'])
        return data


class SnapApiSchema(Schema):
    snap_name = fields.String()
    thin_id = fields.Integer()
    ori_thin_id = fields.Integer()
    status = EnumField(SnapStatus)
    dlv_name = fields.String()


class SnapRpcSchema(Schema):
    snap_name = fields.String()
    thin_id = fields.Integer()
    ori_thin_id = fields.Integer()
    status = EnumField(SnapStatus)
    dlv_name = fields.String()


class DpvApiSchema(Schema):
    dpv_name = fields.String()
    total_size = fields.Integer()
    free_size = fields.Integer()
    service_status = EnumField(ServiceStatus)
    disk_status = EnumField(DiskStatus)
    dvg_name = fields.String()
    lock_id = fields.String()
    lock_timestamp = fields.Integer()
    legs = fields.Nested(LegApiSchema, many=True)


class DpvInfoSchema(Schema):
    total_size = fields.Integer()
    free_size = fields.Integer()

    @post_dump
    def dump_dpv(self, data):
        data['total_size'] = str(data['total_size'])
        data['free_size'] = str(data['free_size'])

    @post_load
    def make_dpv(self, data):
        data['total_size'] = int(data['total_size'])
        data['free_size'] = int(data['free_size'])
        return data


class DvgApiSchema(Schema):
    dvg_name = fields.String()
    total_size = fields.Integer()
    free_size = fields.Integer()


class DlvApiSchema(Schema):
    dlv_name = fields.String()
    dlv_size = fields.Integer()
    data_size = fields.Integer()
    stripe_number = fields.Integer()
    status = EnumField(DlvStatus)
    dvg_name = fields.String()
    ihost_name = fields.String()
    active_snap_name = fields.String()
    lock_id = fields.String()
    lock_timestamp = fields.Integer()
    snapshots = fields.Nested(SnapApiSchema, many=True)
    groups = fields.Nested(GroupApiSchema, many=True)


class DlvRpcSchema(Schema):
    dlv_name = fields.String()
    dlv_size = fields.Integer()
    data_size = fields.Integer()
    stripe_number = fields.Integer()
    status = EnumField(DlvStatus)
    dvg_name = fields.String()
    active_snap_name = fields.String()
    groups = fields.Nested(GroupRpcSchema, many=True)

    @post_dump
    def dump_dlv(self, data):
        data['dlv_size'] = str(data['dlv_size'])
        data['data_size'] = str(data['data_size'])

    @post_load
    def make_dlv(self, data):
        data['dlv_size'] = int(data['dlv_size'])
        data['data_size'] = int(data['data_size'])
        return data


class IhostApiSchema(Schema):
    ihost_name = fields.String()
    service_status = EnumField(ServiceStatus)
    lock_id = fields.String()
    lock_timestamp = fields.String()
    dlvs = fields.Nested(DlvApiSchema, many=True)
