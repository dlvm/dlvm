from typing import MutableMapping, Mapping

from marshmallow import Schema, fields, post_load, post_dump
from marshmallow_enum import EnumField

from dlvm.core.modules import DistributePhysicalVolume, \
    DistributeLogicalVolume, Group, Leg, Snapshot, \
    DpvStatus, IhostStatus, DlvStatus, SnapStatus


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
    def dump_leg(self, data: MutableMapping)-> None:
        data['leg_size'] = str(data['leg_size'])

    @post_load
    def make_leg(self, data: Mapping)-> Leg:
        return Leg(
            leg_id=data['leg_id'],
            leg_idx=data['leg_idx'],
            leg_size=int(data['leg_size']))


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
    def dump_group(self, data: MutableMapping)-> None:
        data['group_size'] = str(data['group_size'])

    @post_load
    def make_group(self, data: Mapping)-> Group:
        return Group(
            group_id=data['group_id'],
            group_idx=data['group_idx'],
            group_size=data['group_size'])


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

    @post_load
    def make_snapshot(self, data: Mapping)-> Snapshot:
        return Snapshot(
            snap_name=data['snap_name'],
            thin_id=data['thin_id'],
            ori_thin_id=data['ori_thin_id'],
            status=data['status'],
            dlv_name=data['dlv_name'],
        )


class DpvApiSchema(Schema):
    dpv_name = fields.String()
    total_size = fields.Integer()
    free_size = fields.Integer()
    status = EnumField(DpvStatus)
    dvg_name = fields.String()
    lock_id = fields.String()
    lock_timestamp = fields.Integer()
    legs = fields.Nested(LegApiSchema, many=True)


class DpvRpcSchema(Schema):
    total_size = fields.Integer()
    free_size = fields.Integer()

    @post_dump
    def dump_dpv(self, data: MutableMapping)-> None:
        data['total_size'] = str(data['total_size'])
        data['free_size'] = str(data['free_size'])

    @post_load
    def make_dpv(self, data: Mapping)-> DistributePhysicalVolume:
        return DistributePhysicalVolume(
            total_size=int(data['total_size']),
            free_size=int(data['free_size']),
        )


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
    def dump_dlv(self, data: MutableMapping)-> None:
        data['dlv_size'] = str(data['dlv_size'])
        data['data_size'] = str(data['data_size'])

    @post_load
    def make_dlv(self, data: Mapping)-> DistributeLogicalVolume:
        return DistributeLogicalVolume(
            dlv_name=data['dlv_name'],
            dlv_size=int(data['dlv_size']),
            data_size=int(data['data_size']),
            stripe_number=data['stripe_number'],
            status=data['status'],
            dvg_name=data['dvg_name'],
            active_snap_name=data['active_snap_name'])


class IhostApiSchema(Schema):
    ihost_name = fields.String()
    status = EnumField(IhostStatus)
    lock_id = fields.String()
    lock_timestamp = fields.String()
    dlvs = fields.Nested(DlvApiSchema, many=True)
