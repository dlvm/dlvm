from math import ceil

from marshmallow import fields

from dlvm.common.utils import chunks
from dlvm.common.configure import cfg
from dlvm.common.marshmallow_ext import NtSchema
from dlvm.common.schema import DmContextSchema, \
    DlvInfoSchema
from dlvm.wrapper.rpc_wrapper import IhostRpc
from dlvm.wrapper import command as cmd
from dlvm.wrapper.local_ctx import backend_local


ihost_prefix = cfg.get('device_mapper', 'ihost_prefix')


ihost_rpc = IhostRpc()


def get_final_name(dlv_name):
    return '{ihost_prefix}-final-{dlv_name}'.format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
    )


def get_pool_meta_name(dlv_name, g_idx):
    return '{ihost_prefix}-poolmeta-{dlv_name}-{g_idx}'.format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
        g_idx=g_idx,
    )


def get_pool_data_name(dlv_name, g_idx):
    return '{ihost_prefix}-pooldata-{dlv_name}-{g_idx}'.format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
        g_idx=g_idx,
    )


def get_pool_name(dlv_name, g_idx):
    return '{ihost_prefix}-pool-{dlv_name}-{g_idx}'.format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
        g_idx=g_idx,
    )


def get_thin_name(dlv_name, g_idx):
    return '{ihost_prefix}-thin-{dlv_name}-{g_idx}'.format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
        g_idx=g_idx,
    )


def get_linear_name(dlv_name, g_idx):
    return '{ihost_prefix}-linear-{dlv_name}-{g_idx}'.format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
        g_idx=g_idx,
    )


def get_mirror_name(dlv_name, g_idx, leg0_idx, leg1_idx):
    return (
        '{ihost_prefix}-mirror-{dlv_name}'
        '-{g_idx}-{leg0_idx}-{leg1_idx}'
    ).format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
        g_idx=g_idx,
        leg0_idx=leg0_idx,
        leg1_idx=leg1_idx,
    )


def get_mirror_meta_name(dlv_name, g_idx, leg_idx):
    return (
        '{ihost_prefix}-mirrormeta-{dlv_name}'
        '-{g_idx}-{leg_idx}'
    ).format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
        g_idx=g_idx,
        leg_idx=leg_idx,
    )


def get_mirror_data_name(dlv_name, g_idx, leg_idx):
    return (
        '{ihost_prefix}-mirrordata-{dlv_name}'
        '-{g_idx}-{leg_idx}'
    ).format(
        ihost_prefix=ihost_prefix,
        dlv_name=dlv_name,
        g_idx=g_idx,
        leg_idx=leg_idx,
    )


def login_leg(leg_id, dpv_name):
    target_name = cmd.encode_target_name(leg_id)
    leg_path = cmd.iscsi_login(target_name, dpv_name)
    return leg_path


def create_mirror_leg(
        dlv_name, g_idx, leg, dev_path, dm_ctx, rebuild=False):
    leg_sectors = leg.leg_size // 512
    mirror_meta_sectors = dm_ctx.mirror_meta_blocks \
        * dm_ctx.thin_block_size // 512
    mirror_data_sectors = leg_sectors - mirror_meta_sectors
    mirror_meta_name = get_mirror_meta_name(
        dlv_name, g_idx, leg.leg_idx)
    dm = cmd.DmLinear(mirror_meta_name)
    table = [{
        'start': 0,
        'length': mirror_meta_sectors,
        'dev_path': dev_path,
        'offset': 0,
    }]
    if rebuild is True:
        dm.reload(table)
        meta_path = dm.get_path()
    else:
        meta_path = dm.create(table)

    mirror_data_name = get_mirror_data_name(
        dlv_name, g_idx, leg.leg_idx)
    dm = cmd.DmLinear(mirror_data_name)
    table = [{
        'start': 0,
        'length': mirror_data_sectors,
        'dev_path': dev_path,
        'offset': mirror_meta_sectors
    }]
    if rebuild is True:
        dm.reload(table)
        data_path = dm.get_path()
    else:
        data_path = dm.create(table)
    return meta_path, data_path


def create_mirror(dlv_name, g_idx, dm_ctx, leg0, leg1):
    logger = backend_local.req_ctx.logger
    leg0_path = None
    leg1_path = None
    assert(leg0.leg_size == leg1.leg_size)
    leg_sectors = leg0.leg_size // 512
    mirror_meta_sectors = dm_ctx.mirror_meta_blocks \
        * dm_ctx.thin_block_size // 512
    mirror_data_sectors = leg_sectors - mirror_meta_sectors
    mirror_region_sectors = dm_ctx.mirror_region_size // 512
    try:
        leg0_path = login_leg(leg0.leg_id, leg0.dpv_name)
    except Exception as e:
        logger.warning(
            'login_failed_0: %s %s %s',
            leg0.leg_id, leg0.dpv_name, str(e))
    try:
        leg1_path = login_leg(leg1.leg_id, leg1.dpv_name)
    except Exception as e:
        logger.warning(
            'login_failed_1: %s %s %s',
            leg1.leg_id, leg1.dpv_name, str(e))

    mirror_name = get_mirror_name(
        dlv_name,  g_idx, leg0.leg_idx, leg1.leg_idx)

    if leg0_path is None and leg1_path is None:
        dm = cmd.DmError(mirror_name)
        table = {
            'start': 0,
            'length': mirror_data_sectors,
        }
        mirror_path = dm.create(table)
        # FIXME: track mirror status
    else:
        if leg0_path is None:
            meta0_path = '-'
            data0_path = '-'
        else:
            meta0_path, data0_path = create_mirror_leg(
                dlv_name, g_idx, leg0, leg0_path, dm_ctx)
        if leg1_path is None:
            meta1_path = '-'
            data1_path = '-'
        else:
            meta1_path, data1_path = create_mirror_leg(
                dlv_name, g_idx, leg1, leg1_path, dm_ctx)
        dm = cmd.DmMirror(mirror_name)
        table = {
            'start': 0,
            'offset': mirror_data_sectors,
            'region_size': mirror_region_sectors,
            'meta0': meta0_path,
            'data0': data0_path,
            'meta1': meta1_path,
            'data1': data1_path,
        }
        mirror_path = dm.create(table)
        # FIXME: track mirror status

        return mirror_path, mirror_data_sectors


def create_pool_meta(dlv_name, g_idx, dm_ctx, leg0, leg1):
    assert(leg0.leg_idx == 0)
    assert(leg1.leg_idx == 1)
    assert(leg0.leg_size == leg1.leg_size)
    mirror_path, mirror_sectors = create_mirror(
        dlv_name, g_idx, dm_ctx, leg0, leg1)
    table = [{
        'start': 0,
        'length': mirror_sectors,
        'dev_path': mirror_path,
        'offset': 0,
    }]
    pool_meta_name = get_pool_meta_name(dlv_name, g_idx)
    dm = cmd.DmLinear(pool_meta_name)
    pool_meta_path = dm.create(table)
    return pool_meta_path


def create_pool_data(dlv_name, g_idx, dm_ctx, legs):
    current_sectors = 0
    table = []
    for leg0, leg1 in chunks(legs, 2):
        assert(leg0.leg_size == leg1.leg_size)
        mirror_path, mirror_sectors = create_mirror(
            dlv_name, g_idx, dm_ctx, leg0, leg1)
        line = {
            'start': current_sectors,
            'length': mirror_sectors,
            'dev_path': mirror_path,
            'offset': 0,
        }
        table.append(line)
        current_sectors += mirror_sectors
    pool_data_name = get_pool_data_name(dlv_name, g_idx)
    dm = cmd.DmLinear(pool_data_name)
    pool_data_path = dm.create(table)
    return pool_data_path


def create_group(dlv_name, group_thin_size, thin_id, dm_ctx, group):
    group_thin_sectors = group_thin_size // 512
    pool_data_sectors = group.group_size // 512
    thin_block_sectors = dm_ctx.thin_block_size // 512
    low_water_mark = dm_ctx.low_water_mark
    legs = group.legs
    legs.sort(key=lambda x: x.leg_idx)
    pool_meta_path = create_pool_meta(
        dlv_name, group.group_idx, dm_ctx, legs[0], legs[1])
    pool_data_path = create_pool_data(
        dlv_name, group.group_idx, dm_ctx, legs[2:])
    pool_name = get_pool_name(dlv_name, group.group_idx)
    dm = cmd.DmPool(pool_name)
    table = {
        'start': 0,
        'length': pool_data_sectors,
        'meta_path': pool_meta_path,
        'data_path': pool_data_path,
        'block_sectors': thin_block_sectors,
        'low_water_mark': low_water_mark,
    }
    pool_path = dm.create(table)
    if thin_id == 0:
        message = {
            'action': 'thin',
            'thin_id': 0,
        }
        dm.message(message)
    thin_name = get_thin_name(dlv_name, group.group_idx)
    dm = cmd.DmThin(thin_name)
    table = {
        'start': 0,
        'length': group_thin_sectors,
        'pool_path': pool_path,
        'thin_id': thin_id,
    }
    thin_path = dm.create(table)

    linear_name = get_linear_name(dlv_name, group.group_idx)
    dm = cmd.DmLinear(linear_name)
    table = [{
        'start': 0,
        'length': group_thin_sectors,
        'dev_path': thin_path,
        'offset': 0,
    }]
    linear_path = dm.create(table)
    return linear_path


def create_final(
        dlv_name, dm_ctx, dlv_size,
        stripe_number, group_path_list):
    dlv_sectors = dlv_size // 512
    stripe_chunk_sectors = dm_ctx.stripe_chunk_size // 512
    devices = []
    for group_path in group_path_list:
        device = {
            'dev_path': group_path,
            'offset': 0,
        }
        devices.append(device)
    table = {
        'start': 0,
        'length': dlv_sectors,
        'num': stripe_number,
        'chunk_size': stripe_chunk_sectors,
        'devices': devices,
    }
    final_name = get_final_name(dlv_name)
    dm = cmd.DmStripe(final_name)
    final_path = dm.create(table)
    return final_path


class AggregateArgSchema(NtSchema):
    dlv_name = fields.String()
    thin_id = fields.Integer()
    dlv_info = fields.Nested(DlvInfoSchema, many=False)
    dm_ctx = fields.Nested(DmContextSchema, many=False)


class AggregateRetSchema(NtSchema):
    dlv_path = fields.String()


@ihost_rpc.register(
    arg_schema=AggregateArgSchema,
    ret_schema=AggregateRetSchema,
    lock_method=lambda arg: arg.dlv_name)
def dlv_aggregate(arg):
    dlv_name = arg.dlv_name
    thin_id = arg.thin_id
    dlv_info = arg.dlv_info
    dm_ctx = arg.dm_ctx
    dlv_size = dlv_info.dlv_size
    stripe_number = dlv_info.stripe_number
    group_thin_size = ceil(dlv_size / stripe_number)
    group_path_list = []
    groups = dlv_info.groups
    groups.sort(key=lambda x: x.group_idx)
    for group in groups:
        group_path = create_group(
            dlv_name, group_thin_size, thin_id, dm_ctx, group)
        group_path_list.append(group_path)
    final_path = create_final(
        dlv_name, dm_ctx, dlv_info.dlv_size,
        dlv_info.stripe_number, group_path_list)
    return final_path


def logout_leg(leg_id):
    target_name = cmd.encode_target_name(leg_id)
    cmd.iscsi_logout(target_name)


def remove_mirror_leg(dlv_name, g_idx, leg):
    mirror_data_name = get_mirror_data_name(
        dlv_name, g_idx, leg.leg_idx)
    dm = cmd.DmLinear(mirror_data_name)
    dm.remove()

    mirror_meta_name = get_mirror_meta_name(
        dlv_name, g_idx, leg.leg_idx)
    dm = cmd.DmLinear(mirror_meta_name)
    dm.remove()


def remove_mirror(dlv_name, g_idx, leg0, leg1):
    mirror_name = get_mirror_name(
        dlv_name,  g_idx, leg0.leg_idx, leg1.leg_idx)
    dm = cmd.DmBasic(mirror_name)
    dm.remove()

    remove_mirror_leg(dlv_name, g_idx, leg0)
    remove_mirror_leg(dlv_name, g_idx, leg1)
    logout_leg(leg0.leg_id)
    logout_leg(leg1.leg_id)


def remove_pool_data(dlv_name, g_idx, legs):
    pool_data_name = get_pool_data_name(dlv_name, g_idx)
    dm = cmd.DmLinear(pool_data_name)
    dm.remove()

    for leg0, leg1 in chunks(legs, 2):
        remove_mirror(dlv_name, g_idx, leg0, leg1)


def remove_pool_meta(dlv_name, g_idx, leg0, leg1):
    assert(leg0.leg_idx == 0)
    assert(leg1.leg_idx == 1)
    pool_meta_name = get_pool_meta_name(dlv_name, g_idx)
    dm = cmd.DmLinear(pool_meta_name)
    dm.remove()
    remove_mirror(dlv_name, g_idx, leg0, leg1)


def remove_group(dlv_name, group):
    linear_name = get_linear_name(dlv_name, group.group_idx)
    dm = cmd.DmLinear(linear_name)
    dm.remove()

    thin_name = get_thin_name(dlv_name, group.group_idx)
    dm = cmd.DmThin(thin_name)
    dm.remove()

    pool_name = get_pool_name(dlv_name, group.group_idx)
    dm = cmd.DmPool(pool_name)
    dm.remove()

    legs = group.legs
    legs.sort(key=lambda x: x.leg_idx)

    remove_pool_data(dlv_name, group.group_idx, legs[2:])
    remove_pool_meta(
        dlv_name, group.group_idx, legs[0], legs[1])


def remove_final(dlv_name):
    final_name = get_final_name(dlv_name)
    dm = cmd.DmStripe(final_name)
    dm.remove()


class DegregateArgSchema(NtSchema):
    dlv_name = fields.String()
    dlv_info = fields.Nested(DlvInfoSchema, many=False)


@ihost_rpc.register(
    arg_schema=DegregateArgSchema,
    lock_method=lambda arg: arg.dlv_name)
def dlv_degregate(arg):
    dlv_name = arg.dlv_name
    dlv_info = arg.dlv_info
    remove_final(dlv_name)
    for group in dlv_info.groups:
        remove_group(dlv_name, group)


def start_ihost_agent():
    ihost_rpc.start_server()
