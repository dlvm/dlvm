from marshmallow import fields

from dlvm.common.marshmallow_ext import NtSchema, EnumField

from dlvm.common.modules import DpvStatus, \
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
    status = EnumField(DpvStatus)
    dvg_name = fields.String()
    lock_id = fields.Integer()
    lock_timestamp = fields.Integer()


class DpvSchema(DpvSummarySchema):
    legs = fields.Nested(LegSchema, many=True)


class DvgSchema(NtSchema):
    dvg_name = fields.String()
    total_size = fields.Integer()
    free_size = fields.Integer()


class DlvSummarySchema(NtSchema):
    dlv_name = fields.String()
    dlv_size = fields.Integer()
    data_size = fields.Integer()
    stripe_number = fields.Integer()
    status = EnumField(DlvStatus)
    bm_dirty = fields.Boolean()
    dvg_name = fields.String()
    ihost_name = fields.String()
    active_snap_name = fields.String()
    lock_id = fields.Integer()


class DlvSchema(DlvSummarySchema):
    groups = fields.Nested(GroupSchema, many=True)


class DmContextSchema(NtSchema):
    thin_block_size = fields.Integer()
    mirror_meta_blocks = fields.Integer()
    mirror_region_size = fields.Integer()
    stripe_chunk_size = fields.Integer()
    low_water_mark = fields.Integer()


class LegInfoSchema(NtSchema):
    leg_id = fields.Integer()
    leg_idx = fields.Integer()
    leg_size = fields.Integer()
    dpv_name = fields.String()


class GroupInfoSchema(NtSchema):
    group_id = fields.Integer()
    group_idx = fields.Integer()
    group_size = fields.Integer()
    legs = fields.Nested(LegInfoSchema, many=True)


class DlvInfoSchema(NtSchema):
    dlv_size = fields.Integer()
    stripe_number = fields.Integer()
    groups = fields.Nested(GroupInfoSchema, many=True)
