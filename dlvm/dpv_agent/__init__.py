import os
from math import ceil

from marshmallow import fields

from dlvm.common.configure import cfg
from dlvm.common.marshmallow_ext import NtSchema
from dlvm.wrapper.rpc_wrapper import DpvRpc
from dlvm.wrapper import command as cmd
from dlvm.dpv_agent.mirror_meta import generate_mirror_meta


local_vg_name = cfg.get('device_mapper', 'local_vg')
dpv_prefix = cfg.get('device_mapper', 'dpv_prefix')
tmp_dir = cfg.get('general', 'tmp_dir')


def get_layer1_name(leg_id):
    return '{dpv_prefix}-layer1-{leg_id}'.format(
        dpv_prefix=dpv_prefix, leg_id=leg_id)


def get_layer2_name(leg_id):
    return '{dpv_prefix}-layer2-{leg_id}'.format(
        dpv_prefix=dpv_prefix, leg_id=leg_id)


def get_layer2_name_fj(leg_id, fj_name):
    return '{dpv_prefix}-fj-layer2-{leg_id}-{fj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
    )


def get_fj_meta0_name(leg_id, fj_name):
    return '{dpv_prefix}-fj-meta0-{leg_id}-{fj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
    )


def get_fj_meta1_name(leg_id, fj_name):
    return '{dpv_prefix}-fj-meta1-{leg_id}-{fj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
    )


def get_layer2_name_cj(leg_id, cj_name):
    return '{dpv_prefix}-cj-layer2-{leg_id}-{cj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id,
        cj_name=cj_name,
    )


def get_cj_data_name(leg_id, cj_name):
    return '{dpv_prefix}-cj-data-{leg_id}-{cj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id, cj_name=cj_name)


def get_cj_meta_name(leg_id, cj_name):
    return '{dpv_prefix}-cj-meta-{leg_id}-{cj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id, cj_name=cj_name)


def get_cj_pool_name(leg_id, cj_name):
    return '{dpv_prefix}-cj-pool-{leg_id}-{cj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id, cj_name=cj_name)


def get_cj_thin_name(leg_id, cj_name):
    return '{dpv_prefix}-cj-thin-{leg_id}-{cj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id, cj_name=cj_name)


def get_cj_meta0_name(leg_id, cj_name):
    return '{dpv_prefix}-cj-meta0-{leg_id}-{cj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id,
        cj_name=cj_name,
    )


def get_cj_meta1_name(leg_id, cj_name):
    return '{dpv_prefix}-cj-meta1-{leg_id}-{cj_name}'.format(
        dpv_prefix=dpv_prefix,
        leg_id=leg_id,
        cj_name=cj_name,
    )


dpv_rpc = DpvRpc()


class DpvGetInfoRetSchema(NtSchema):
    total_size = fields.Integer()
    free_size = fields.Integer()


@dpv_rpc.register(ret_schema=DpvGetInfoRetSchema)
def dpv_get_info():
    total_size, free_size = cmd.vg_get_size(local_vg_name)
    return DpvGetInfoRetSchema.nt(total_size, free_size)


class DmContextSchema(NtSchema):
    thin_block_size = fields.Integer()
    mirror_meta_blocks = fields.Integer()
    mirror_region_size = fields.Integer()
    stripe_chunk_size = fields.Integer()
    low_water_mark = fields.Integer()


class LegCreateArgSchema(NtSchema):
    leg_id = fields.Integer()
    leg_size = fields.Integer()
    dm_ctx = fields.Nested(DmContextSchema, many=False)


@dpv_rpc.register(
    arg_schema=LegCreateArgSchema,
    lock_method=lambda arg: arg.leg_id)
def leg_create(arg):
    leg_id = arg.leg_id
    leg_size = arg.leg_size
    dm_ctx = arg.dm_ctx
    lv_path = cmd.lv_create(
        str(leg_id), leg_size, local_vg_name)
    leg_sectors = leg_size / 512
    layer1_name = get_layer1_name(leg_id)
    dm = cmd.DmLinear(layer1_name)
    table = [{
        'start': 0,
        'length': leg_sectors,
        'dev_path': lv_path,
        'offset': 0,
    }]
    layer1_path = dm.create(table)

    layer2_name = get_layer2_name(leg_id)
    dm = cmd.DmLinear(layer2_name)
    table = [{
        'start': 0,
        'length': leg_sectors,
        'dev_path': layer1_path,
        'offset': 0,
    }]
    layer2_path = dm.create(table)

    thin_block_size = dm_ctx.thin_block_size
    mirror_meta_blocks = dm_ctx.mirror_meta_blocks
    mirror_meta_size = thin_block_size * mirror_meta_blocks
    mirror_data_size = leg_size - mirror_meta_size
    mirror_region_size = dm_ctx.mirror_region_size
    file_name = 'dlvm-leg-{leg_id}'.format(leg_id=leg_id)
    file_path = os.path.join(tmp_dir, file_name)
    bm_len_8 = ceil(mirror_data_size/mirror_region_size)
    bm_len = ceil(bm_len_8 / 8)
    bm = bytes([0x0 for i in range(bm_len)])
    generate_mirror_meta(
        file_path,
        mirror_meta_size,
        mirror_data_size,
        mirror_region_size,
        bm,
    )
    cmd.dm_dd(
        src=file_path,
        dst=layer2_path,
        bs=mirror_meta_size,
        count=1,
    )
    cmd.dm_dd(
        src='/dev/zero',
        dst=layer2_path,
        bs=thin_block_size,
        count=1,
        seek=mirror_meta_blocks,
    )
    os.remove(file_path)

    target_name = cmd.encode_target_name(leg_id)
    cmd.iscsi_create(target_name, leg_id, layer2_path)


class LegDeleteArgSchema(NtSchema):
    leg_id = fields.Integer()


@dpv_rpc.register(
    arg_schema=LegDeleteArgSchema,
    lock_method=lambda arg: arg.leg_id)
def leg_delete(arg):
    leg_id = arg.leg_id
    target_name = cmd.encode_target_name(leg_id)
    cmd.iscsi_delete(target_name, leg_id)
    layer2_name = get_layer2_name(leg_id)
    dm = cmd.DmLinear(layer2_name)
    dm.remove()
    layer1_name = get_layer1_name(leg_id)
    dm = cmd.DmLinear(layer1_name)
    dm.remove()
    cmd.lv_remove(leg_id, local_vg_name)


class LegExportArgSchema(NtSchema):
    leg_id = fields.Integer()
    ihost_name = fields.String()


@dpv_rpc.register(
    arg_schema=LegExportArgSchema,
    lock_method=lambda arg: arg.leg_id)
def leg_export(arg):
    target_name = cmd.encode_target_name(
        arg.leg_id)
    initiator_name = cmd.encode_initiator_name(
        arg.ihost_name)
    cmd.iscsi_export(target_name, initiator_name)


class LegUnexportArgSchema(NtSchema):
    leg_id = fields.Integer()
    ihost_name = fields.String()


@dpv_rpc.register(
    arg_schema=LegUnexportArgSchema,
    lock_method=lambda arg: arg.leg_id)
def leg_unexport(arg):
    target_name = cmd.encode_target_name(
        arg.leg_id)
    initiator_name = cmd.encode_initiator_name(
        arg.ihost_name)
    cmd.iscsi_unexport(target_name, initiator_name)


def start_dpv_agent():
    dpv_rpc.start_server()
