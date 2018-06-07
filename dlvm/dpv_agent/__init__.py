import os
from math import ceil
from collections import namedtuple

from marshmallow import fields

from dlvm.common.configure import cfg
from dlvm.common.marshmallow_ext import NtSchema
from dlvm.wrapper.rpc_wrapper import DpvRpc
from dlvm.wrapper import command as cmd
from dlvm.wrapper.local_ctx import backend_local
from dlvm.dpv_agent.mirror_meta import generate_mirror_meta


local_vg_name = cfg.get('device_mapper', 'local_vg')
dpv_prefix = cfg.get('device_mapper', 'dpv_prefix')
tmp_dir = cfg.get('general', 'tmp_dir')


layer1_prefix = '{dpv_prefix}-layer1'.format(
    dpv_prefix=dpv_prefix)


def get_layer1_name(leg_id):
    return '{layer1_prefix}-{leg_id}'.format(
        layer1_prefix=layer1_prefix, leg_id=leg_id)


layer2_prefix = '{dpv_prefix}-layer2'.format(
    dpv_prefix=dpv_prefix)


def get_layer2_name(leg_id):
    return '{layer2_prefix}-{leg_id}'.format(
        layer2_prefix=layer2_prefix, leg_id=leg_id)


fj_layer2_prefix = '{dpv_prefix}-fj-layer2'.format(
    dpv_prefix=dpv_prefix)


def get_fj_layer2_name(leg_id, fj_name):
    return '{fj_layer2_prefix}-{leg_id}-{fj_name}'.format(
        fj_layer2_prefix=fj_layer2_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
    )


fj_meta0_prefix = '{dpv_prefix}-fj-meta0'.format(
    dpv_prefix=dpv_prefix)


def get_fj_meta0_name(leg_id, fj_name):
    return '{fj_meta0_prefix}-{leg_id}-{fj_name}'.format(
        fj_meta0_prefix=fj_meta0_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
    )


fj_meta1_prefix = '{dpv_prefix}-fj-meta1'.format(
    dpv_prefix=dpv_prefix)


def get_fj_meta1_name(leg_id, fj_name):
    return '{fj_meta1_prefix}-{leg_id}-{fj_name}'.format(
        fj_meta1_prefix=fj_meta1_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
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
    leg_sectors = leg_size // 512
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
    cmd.iscsi_create(target_name, layer2_name, layer2_path)


class LegDeleteArgSchema(NtSchema):
    leg_id = fields.Integer()


@dpv_rpc.register(
    arg_schema=LegDeleteArgSchema,
    lock_method=lambda arg: arg.leg_id)
def leg_delete(arg):
    leg_id = arg.leg_id
    target_name = cmd.encode_target_name(leg_id)
    layer2_name = get_layer2_name(leg_id)
    cmd.iscsi_delete(target_name, layer2_name)
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


class LegInfoSchema(NtSchema):
    leg_id = fields.Integer()
    leg_size = fields.Integer()
    ihost_name = fields.String()


class DpvSyncArgSchema(NtSchema):
    dpv_info = fields.Nested(LegInfoSchema, many=True)
    dm_ctx = fields.Nested(DmContextSchema, many=False)


DmInfo = namedtuple('DmInfo', [
    'layer2_set', 'layer1_set'])

ReCreateLog = namedtuple('ReCreateLog', [
    'iscsi_info', 'dm_info', 'lv_set'])


def leg_recreate(leg_info, dm_ctx, recreate_log):
    leg_id = leg_info.leg_id
    leg_size = leg_info.leg_size

    lv_name = str(leg_id)
    recreate_log.lv_set.add(lv_name)
    lv_path = cmd.lv_get_path(lv_name, local_vg_name)
    leg_sectors = leg_size // 512
    layer1_name = get_layer1_name(leg_id)
    dm = cmd.DmLinear(layer1_name)
    table = [{
        'start': 0,
        'length': leg_sectors,
        'dev_path': lv_path,
        'offset': 0,
    }]
    layer1_path = dm.create(table)
    recreate_log.dm_info.layer1_set.add(layer1_name)

    layer2_name = get_layer2_name(leg_id)
    dm = cmd.DmLinear(layer2_name)
    table = [{
        'start': 0,
        'length': leg_sectors,
        'dev_path': layer1_path,
        'offset': 0,
    }]
    layer2_path = dm.create(table)
    recreate_log.dm_info.layer2_set.add(layer2_name)

    target_name = cmd.encode_target_name(leg_id)
    cmd.iscsi_create(target_name, layer2_name, layer2_path)

    if leg_info.ihost_name:
        initiator_name = cmd.encode_initiator_name(
            leg_info.ihost_name)
        cmd.iscsi_export(target_name, initiator_name)
        recreate_log.iscsi_info['target_name'] = initiator_name
    else:
        recreate_log.iscsi_info['target_name'] = None


def iscsi_clean(iscsi_info):
    target_list = cmd.iscsi_target_get_all()
    for target_name, initiator_list in target_list:
        if target_name not in iscsi_info:
            cmd.iscsi_target_release(target_name)
        else:
            for initiator_name in initiator_list:
                if initiator_name != iscsi_info[target_name]:
                    cmd.iscsi_unexport(target_name, initiator_name)
    cmd.iblock_release()


def dm_clean(dm_info):
    dm_name_list = cmd.dm_get_all()
    layer2_list = []
    layer1_list = []
    for name in dm_name_list:
        if name.starts_with(layer2_prefix):
            layer2_list.append(name)
        elif name.starts_with(layer1_prefix):
            layer1_list.append(name)
        else:
            logger = backend_local.req_ctx.logger
            logger.warning('unknown dm device: %s', name)
    for name in layer2_list:
        if name not in dm_info.layer2_set:
            cmd.dm_remove(name)
    for name in layer1_list:
        if name not in dm_info.layer1_set:
            cmd.dm_remove(name)


def lv_clean(lv_set):
    lv_name_list = cmd.lv_get_all(local_vg_name)
    for lv_name in lv_name_list:
        if lv_name not in lv_set:
            cmd.lv_remove(lv_name, local_vg_name)


@dpv_rpc.register(
    arg_schema=DpvSyncArgSchema,
    lock_method=lambda arg: None)
def dpv_sync(arg):
    dm_info = DmInfo(
        layer2_set=set(),
        layer1_set=set(),
    )
    recreate_log = ReCreateLog(
        iscsi_info={},
        dm_info=dm_info,
        lv_set=set())
    for leg_info in arg.dpv_info:
        leg_recreate(leg_info, arg.dm_ctx, recreate_log)
    iscsi_clean(recreate_log.iscsi_info)
    dm_clean(recreate_log.dm_info)
    lv_clean(recreate_log.lv_set)


class DpvVerifyRetSchema(NtSchema):
    status = fields.String()


@dpv_rpc.register(ret_schema=DpvVerifyRetSchema)
def dpv_ping():
    return 'ok'


def start_dpv_agent():
    dpv_rpc.start_server()
