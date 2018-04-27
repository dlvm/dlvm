from marshmallow import fields

from dlvm.common.marshmallow_ext import NtSchema, EnumField

from dlvm.common.modules import ServiceStatus, DiskStatus, \
    DlvStatus, SnapStatus


class GroupSummarySchema(NtSchema):
    group_id = fields.Integer()
    dlv_name = fields.String()


class LegSchema(NtSchema):
    leg_id = fields.Integer()
    leg_idx = fields.Integer()
    leg_size = fields.Integer()
    group_id = fields.String()
    dpv_name = fields.String()
    group_summary = fields.Nested(GroupSummarySchema, many=False)


class GroupSchema(NtSchema):
    group_id = fields.Integer()
    group_idx = fields.Integer()
    group_size = fields.Integer()
    dlv_name = fields.String()
    legs = fields.Nested(LegSchema, many=True)


class SnapSchema(NtSchema):
    snap_name = fields.String()
    thin_id = fields.Integer()
    ori_thin_id = fields.Integer()
    status = EnumField(SnapStatus)
    dlv_name = fields.String()


class DpvSummarySchema(NtSchema):
    dpv_name = fields.String()
    total_size = fields.Integer()
    free_size = fields.Integer()
    service_status = EnumField(ServiceStatus)
    disk_status = EnumField(DiskStatus)
    dvg_name = fields.String()
    lock_id = fields.Integer()
    lock_timestamp = fields.Integer()


class DpvSchema(DpvSummarySchema):
    legs = fields.Nested(LegSchema, many=True)


class DvgApiSchema(NtSchema):
    dvg_name = fields.String()
    total_size = fields.Integer()
    free_size = fields.Integer()


class DlvSummarySchema(NtSchema):
    dlv_name = fields.String()
    dlv_size = fields.Integer()
    data_size = fields.Integer()
    stripe_number = fields.Integer()
    status = EnumField(DlvStatus)
    dvg_name = fields.String()
    ihost_name = fields.String()
    active_snap_name = fields.String()
    lock_id = fields.Integer()
