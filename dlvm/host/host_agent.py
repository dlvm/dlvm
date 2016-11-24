#!/usr/bin/env python

from threading import Thread, Lock
import logging
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from dlvm.utils.rpc_wrapper import WrapperRpcServer
from dlvm.utils.transaction import host_verify
from dlvm.utils.command import context_init, \
    DmBasic, DmLinear, DmStripe, DmMirror, \
    DmPool, DmThin, DmError, \
    iscsi_login, iscsi_logout
from dlvm.utils.helper import chunks, encode_target_name
from dlvm.utils.bitmap import BitMap
from dlvm.utils.queue import queue_init, report_single_leg, \
    report_multi_legs, report_pool


logger = logging.getLogger('dlvm_host')

global_rpc_lock = Lock()
global_rpc_set = set()


class RpcLock(object):

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        with global_rpc_lock:
            if self.name in global_rpc_set:
                raise Exception('rpc_conflict: %s' % self.name)
            global_rpc_set.add(self.name)

    def __exit__(self, exc_type, exc_value, traceback):
        with global_rpc_lock:
            global_rpc_set.remove(self.name)


def ping(message):
    return message


def get_final_name(dlv_name):
    final_name = '{dlv_name}'.format(dlv_name=dlv_name)
    return final_name


def get_middle_name(dlv_name):
    middle_name = '{dlv_name}-middle'.format(dlv_name=dlv_name)
    return middle_name


def get_thin_name(dlv_name):
    thin_name = '{dlv_name}-thin'.format(dlv_name=dlv_name)
    return thin_name


def get_pool_name(dlv_name):
    pool_name = '{dlv_name}-pool'.format(dlv_name=dlv_name)
    return pool_name


def get_thin_meta_name(dlv_name):
    return '{dlv_name}-meta'.format(dlv_name=dlv_name)


def get_thin_data_name(dlv_name):
    return '{dlv_name}-data'.format(dlv_name=dlv_name)


def get_mirror_meta_name(dlv_name, g_idx, l_idx):
    return '{dlv_name}-{g_idx}-{l_idx}-meta'.format(
        dlv_name=dlv_name,
        g_idx=g_idx,
        l_idx=l_idx,
    )


def get_mirror_data_name(dlv_name, g_idx, l_idx):
    return '{dlv_name}-{g_idx}-{l_idx}-data'.format(
        dlv_name=dlv_name,
        g_idx=g_idx,
        l_idx=l_idx,
    )


def get_mirror_name(dlv_name, g_idx, m_idx):
    return '{dlv_name}-{g_idx}-{m_idx}'.format(
        dlv_name=dlv_name,
        g_idx=g_idx,
        m_idx=m_idx,
    )


def get_stripe_name(dlv_name, g_idx):
    return '{dlv_name}-{g_idx}'.format(
        dlv_name=dlv_name,
        g_idx=g_idx,
    )


def bm_get_real(dlv_name, dlv_info, thin_id_list, leg_id_list):
    raise Exception('not_implement')


def bm_get_simple(dlv_name, dlv_info, thin_id_list, leg_id_list):
    bm_dict = {}
    thin_block_size = dlv_info['dm_context']['thin_block_size']
    for group in dlv_info['groups']:
        legs = group['legs']
        legs.sort(key=lambda x: x['idx'])
        for leg0, leg1 in chunks(legs, 2):
            if leg_id_list != [] and \
               leg0['leg_id'] not in leg_id_list and \
               leg1['leg_id'] not in leg_id_list:
                continue
            key = '%s-%s' % (
                leg0['leg_id'], leg1['leg_id'])
            assert(leg0['leg_size'] == leg1['leg_size'])
            bm_size = leg0['leg_size'] / thin_block_size
            bm = BitMap(bm_size)
            for i in xrange(bm_size):
                bm.set(i)
            bm_dict[key] = bm.tohexstring()
    return bm_dict


def do_bm_get(dlv_name, dlv_info, thin_id_list, leg_id):
    pool_name = get_pool_name(dlv_name)
    dm = DmPool(pool_name)
    status = dm.status()
    if status['used_data'] < conf.bm_throttle:
        return bm_get_real(
            dlv_name, dlv_info, thin_id_list, leg_id)
    else:
        return bm_get_simple(
            dlv_name, dlv_info, thin_id_list, leg_id)


def bm_get(dlv_name, tran, dlv_info, thin_id_list, leg_id):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        return do_bm_get(dlv_name, dlv_info, thin_id_list, leg_id)


def generate_dm_context(dmc):
    thin_block_size = dmc['thin_block_size']
    thin_block_sectors = thin_block_size / 512
    mirror_meta_blocks = dmc['mirror_meta_blocks']
    mirror_meta_sectors = mirror_meta_blocks * thin_block_sectors
    mirror_region_size = dmc['mirror_region_size']
    mirror_region_sectors = mirror_region_size / 512
    stripe_chunk_blocks = dmc['stripe_chunk_blocks']
    stripe_chunk_sectors = stripe_chunk_blocks * thin_block_sectors
    stripe_number = dmc['stripe_number']
    low_water_mark = dmc['low_water_mark']
    dm_context = {
        'thin_block_sectors': thin_block_sectors,
        'mirror_meta_sectors': mirror_meta_sectors,
        'mirror_region_sectors': mirror_region_sectors,
        'stripe_chunk_sectors': stripe_chunk_sectors,
        'stripe_number': stripe_number,
        'low_water_mark': low_water_mark,
    }
    return dm_context


def mirror_check(args):
    mirror_name = args['mirror_name']
    dm = DmMirror(mirror_name)
    try:
        status = dm.status()
    except Exception as e:
        logger.info('mirror status failed: %s %s', mirror_name, e)
        return False
    else:
        if status['first_hc'] == 'A' and status['second_hc'] == 'A':
            return False
        else:
            return True


def mirror_action(args):
    mirror_name = args['mirror_name']
    dlv_name = args['dlv_name']
    leg0_id = args['leg0_id']
    leg1_id = args['leg1_id']
    dm = DmMirror(mirror_name)
    status = dm.status()
    if status['first_hc'] == 'A' and status['second_hc'] == 'A':
        pass
    elif status['first_hc'] != 'A' and status['second_hc'] == 'A':
        report_single_leg(dlv_name, leg0_id)
    elif status['first_hc'] == 'A' and status['second_hc'] != 'A':
        report_single_leg(dlv_name, leg1_id)
    else:
        report_multi_legs(dlv_name, leg0_id, leg1_id)


def mirror_event(args):
    dm = DmMirror(args['mirror_name'])
    dm.wait_event(mirror_check, mirror_action, args)


def create_mirror_leg(dlv_name, g_idx, leg, dev_path, dm_context):
    mirror_meta_sectors = dm_context['mirror_meta_sectors']
    mirror_meta_name = get_mirror_meta_name(dlv_name, g_idx, leg['idx'])
    dm = DmLinear(mirror_meta_name)
    table = [{
        'start': 0,
        'length': mirror_meta_sectors,
        'dev_path': dev_path,
        'offset': 0,
    }]
    meta_path = dm.create(table)
    leg_sectors = leg['leg_size'] / 512
    mirror_data_sectors = leg_sectors - mirror_meta_sectors
    mirror_data_name = get_mirror_data_name(dlv_name, g_idx, leg['idx'])
    dm = DmLinear(mirror_data_name)
    table = [{
        'start': 0,
        'length': mirror_data_sectors,
        'dev_path': dev_path,
        'offset': mirror_meta_sectors,
    }]
    data_path = dm.create(table)
    return meta_path, data_path


def login_leg(leg_id, dpv_name):
    target_name = encode_target_name(leg_id)
    leg_path = iscsi_login(target_name, dpv_name)
    return leg_path


def create_mirror(dlv_name, g_idx, m_idx, leg0, leg1, dm_context):
    leg0_path = None
    leg1_path = None
    assert(leg0['leg_size'] == leg1['leg_size'])
    leg_sectors = leg0['leg_size'] / 512
    mirror_meta_sectors = dm_context['mirror_meta_sectors']
    mirror_data_sectors = leg_sectors - mirror_meta_sectors
    mirror_region_sectors = dm_context['mirror_region_sectors']
    try:
        leg0_path = login_leg(leg0['leg_id'], leg0['dpv_name'])
    except Exception as e:
        logger.warning(
            'login_failed_0: %s %s %s',
            leg0['leg_id'],
            leg0['dpv_name'],
            e,
        )
    try:
        leg1_path = login_leg(leg1['leg_id'], leg1['dpv_name'])
    except Exception as e:
        logger.warning(
            'login_failed_1: %s %s %s',
            leg0['leg_id'],
            leg0['dpv_name'],
            e,
        )
    if leg0_path is None and leg1_path is None:
        mirror_name = get_mirror_name(dlv_name, g_idx, m_idx)
        dm = DmError(mirror_name)
        table = {
            'start': 0,
            'length': mirror_data_sectors,
        }
        mirror_path = dm.create(table)
        report_multi_legs(dlv_name, leg0['leg_id'], leg1['leg_id'])
        return mirror_path
    elif leg0_path is None:
        meta1_path, data1_path = create_mirror_leg(
            dlv_name, g_idx, leg1, leg1_path, dm_context)
        mirror_name = get_mirror_name(dlv_name, g_idx, m_idx)
        dm = DmLinear(mirror_name)
        table = [{
            'start': 0,
            'length': mirror_data_sectors,
            'dev_path': data1_path,
            'offset': 0,
        }]
        mirror_path = dm.create(table)
        report_single_leg(dlv_name, leg0['leg_id'])
        return mirror_path
    elif leg1_path is None:
        meta0_path, data0_path = create_mirror_leg(
            dlv_name, g_idx, leg0, leg0_path, dm_context)
        mirror_name = get_mirror_name(dlv_name, g_idx, m_idx)
        dm = DmLinear(mirror_name)
        table = [{
            'start': 0,
            'length': mirror_data_sectors,
            'dev_path': data0_path,
            'offset': 0,
        }]
        mirror_path = dm.create(table)
        report_single_leg(dlv_name, leg1['leg_id'])
        return mirror_path
    else:
        meta0_path, data0_path = create_mirror_leg(
            dlv_name, g_idx, leg0, leg0_path, dm_context)
        meta1_path, data1_path = create_mirror_leg(
            dlv_name, g_idx, leg1, leg1_path, dm_context)
        mirror_name = get_mirror_name(dlv_name, g_idx, m_idx)
        dm = DmMirror(mirror_name)
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
        args = {
            'dlv_name': dlv_name,
            'mirror_name': mirror_name,
            'leg0_id': leg0['leg_id'],
            'leg1_id': leg1['leg_id'],
        }
        t = Thread(target=mirror_event, args=(args,))
        t.start()
        return mirror_path


def create_thin_meta(dlv_name, group, dm_context):
    assert(group['idx'] == 0)
    legs = group['legs']
    legs.sort(key=lambda x: x['idx'])
    leg0 = legs[0]
    leg1 = legs[1]
    mirror_path = create_mirror(dlv_name, 0, 0, leg0, leg1, dm_context)
    thin_meta_name = get_thin_meta_name(dlv_name)
    dm = DmLinear(thin_meta_name)
    table = [{
        'start': 0,
        'length': group['group_size'] / 512,
        'dev_path': mirror_path,
        'offset': 0,
    }]
    thin_meta_path = dm.create(table)
    return thin_meta_path


def create_stripe(dlv_name, group, dm_context):
    stripe_chunk_sectors = dm_context['stripe_chunk_sectors']
    stripe_number = dm_context['stripe_number']
    legs = group['legs']
    legs.sort(key=lambda x: x['idx'])
    group_sectors = group['group_size'] / 512
    devices = []
    table = {
        'start': 0,
        'length': group_sectors,
        'num': stripe_number,
        'chunk_size': stripe_chunk_sectors,
        'devices': devices,
    }
    for m_idx, (leg0, leg1) in enumerate(chunks(legs, 2)):
        mirror_path = create_mirror(
            dlv_name, group['idx'], m_idx, leg0, leg1, dm_context)
        device = {
            'dev_path': mirror_path,
            'offset': 0,
        }
        devices.append(device)
    stripe_name = get_stripe_name(dlv_name, group['idx'])
    dm = DmStripe(stripe_name)
    stripe_path = dm.create(table)
    return stripe_path


def create_thin_data(dlv_name, groups, dm_context):
    current_sectors = 0
    table = []
    for group in groups:
        stripe_path = create_stripe(dlv_name, group, dm_context)
        group_sectors = group['group_size'] / 512
        line = {
            'start': current_sectors,
            'length': group_sectors,
            'dev_path': stripe_path,
            'offset': 0,
        }
        table.append(line)
        current_sectors += group_sectors
    thin_data_name = get_thin_data_name(dlv_name)
    dm = DmLinear(thin_data_name)
    thin_data_path = dm.create(table)
    return thin_data_path


def pool_check(args):
    pool_name = args['pool_name']
    dm = DmPool(pool_name)
    try:
        status = dm.status()
    except Exception as e:
        logger.info('pool status failed: %s %s', pool_name, e)
        return False
    else:
        used_data = status['used_data']
        total_data = status['total_data']
        low_water_mark = args['low_water_mark']
        if total_data - used_data <= low_water_mark:
            return True
        else:
            return False


def pool_action(args):
    report_pool(args['dlv_name'])


def pool_event(args):
    dm = DmPool(args['pool_name'])
    dm.wait_event(pool_check, pool_action, args)


def create_final(
        dlv_name, dlv_size, thin_id,
        thin_meta_path, thin_data_path, data_size,
        dm_context):
    dlv_sectors = dlv_size / 512
    thin_data_sectors = data_size / 512
    thin_block_sectors = dm_context['thin_block_sectors']
    low_water_mark = dm_context['low_water_mark']
    pool_name = get_pool_name(dlv_name)
    dm = DmPool(pool_name)
    table = {
        'start': 0,
        'length': thin_data_sectors,
        'meta_path': thin_meta_path,
        'data_path': thin_data_path,
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
    thin_name = get_thin_name(dlv_name)
    dm = DmThin(thin_name)
    table = {
        'start': 0,
        'length': dlv_sectors,
        'pool_path': pool_path,
        'thin_id': thin_id,
    }
    thin_path = dm.create(table)

    middle_name = get_middle_name(dlv_name)
    dm = DmLinear(middle_name)
    table = [{
        'start': 0,
        'length': dlv_sectors,
        'dev_path': thin_path,
        'offset': 0,
    }]
    middle_path = dm.create(table)

    final_name = get_final_name(dlv_name)
    dm = DmLinear(final_name)
    table = [{
        'start': 0,
        'length': dlv_sectors,
        'dev_path': middle_path,
        'offset': 0,
    }]
    final_path = dm.create(table)
    args = {
        'dlv_name': dlv_name,
        'pool_name': pool_name,
        'low_water_mark': low_water_mark,
    }
    t = Thread(target=pool_event, args=(args,))
    t.start()
    return final_path


def do_dlv_aggregate(dlv_name, dlv_info):
    dlv_size = dlv_info['dlv_size']
    data_size = dlv_info['data_size']
    thin_id = dlv_info['thin_id']
    dm_context = generate_dm_context(dlv_info['dm_context'])
    groups = dlv_info['groups']
    groups.sort(key=lambda x: x['idx'])
    thin_meta_path = create_thin_meta(dlv_name, groups[0], dm_context)
    thin_data_path = create_thin_data(
        dlv_name, groups[1:], dm_context)
    final_path = create_final(
        dlv_name, dlv_size, thin_id,
        thin_meta_path, thin_data_path, data_size,
        dm_context,
    )
    return final_path


def dlv_aggregate(dlv_name, tran, dlv_info):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        return do_dlv_aggregate(dlv_name, dlv_info)


def remove_final(dlv_name):
    final_name = get_final_name(dlv_name)
    dm = DmLinear(final_name)
    dm.remove()
    middle_name = get_middle_name(dlv_name)
    dm = DmLinear(middle_name)
    dm.remove()
    thin_name = get_thin_name(dlv_name)
    dm = DmThin(thin_name)
    dm.remove()
    pool_name = get_pool_name(dlv_name)
    dm = DmPool(pool_name)
    dm.remove()


def remove_mirror_leg(dlv_name, g_idx, leg):
    mirror_meta_name = get_mirror_meta_name(dlv_name, g_idx, leg['idx'])
    dm = DmLinear(mirror_meta_name)
    dm.remove()
    mirror_data_name = get_mirror_data_name(dlv_name, g_idx, leg['idx'])
    dm = DmLinear(mirror_data_name)
    dm.remove()


def logout_leg(leg_id):
    target_name = encode_target_name(leg_id)
    iscsi_logout(target_name)


def remove_mirror(dlv_name, g_idx, m_idx, leg0, leg1):
    mirror_name = get_mirror_name(dlv_name, g_idx, m_idx)
    # not sure whether it is mirror or linear or error,
    # so use DmBasic
    dm = DmBasic(mirror_name)
    dm.remove()
    remove_mirror_leg(dlv_name, g_idx, leg0)
    remove_mirror_leg(dlv_name, g_idx, leg1)
    logout_leg(leg0['leg_id'])
    logout_leg(leg1['leg_id'])


def remove_stripe(dlv_name, group):
    legs = group['legs']
    legs.sort(key=lambda x: x['idx'])
    stripe_name = get_stripe_name(dlv_name, group['idx'])
    dm = DmStripe(stripe_name)
    dm.remove()
    for m_idx, (leg0, leg1) in enumerate(chunks(legs, 2)):
        remove_mirror(
            dlv_name, group['idx'], m_idx, leg0, leg1)


def remove_thin_data(dlv_name, groups):
    thin_data_name = get_thin_data_name(dlv_name)
    dm = DmLinear(thin_data_name)
    dm.remove()
    for group in groups:
        remove_stripe(dlv_name, group)


def remove_thin_meta(dlv_name, group):
    thin_meta_name = get_thin_meta_name(dlv_name)
    dm = DmLinear(thin_meta_name)
    dm.remove()
    legs = group['legs']
    legs.sort(key=lambda x: x['idx'])
    leg0 = legs[0]
    leg1 = legs[1]
    remove_mirror(dlv_name, 0, 0, leg0, leg1)


def do_dlv_degregate(dlv_name, dlv_info):
    groups = dlv_info['groups']
    groups.sort(key=lambda x: x['idx'])
    remove_final(dlv_name)
    remove_thin_data(dlv_name, groups[1:])
    remove_thin_meta(dlv_name, groups[0])


def dlv_degregate(dlv_name, tran, dlv_info):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        do_dlv_degregate(dlv_name, dlv_info)


def do_dlv_suspend(dlv_name, dlv_info):
    dlv_sectors = dlv_info['dlv_size'] / 512

    final_name = get_final_name(dlv_name)
    dm = DmLinear(final_name)
    dm.suspend()

    middle_name = get_middle_name(dlv_name)
    dm = DmError(middle_name)
    table = {
        'start': 0,
        'length': dlv_sectors,
    }
    dm.reload(table)

    thin_name = get_thin_name(dlv_name)
    dm = DmThin(thin_name)
    dm.remove()

    pool_name = get_pool_name(dlv_name)
    dm = DmPool(pool_name)
    dm.remove()


def dlv_suspend(dlv_name, tran, dlv_info):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        do_dlv_suspend(dlv_name, dlv_info)


def do_dlv_resume(dlv_name, dlv_info):
    dlv_sectors = dlv_info['dlv_size'] / 512
    data_sectors = dlv_info['data_size'] / 512
    thin_id = dlv_info['thin_id']
    dm_context = generate_dm_context(dlv_info['dm_context'])
    thin_block_sectors = dm_context['thin_block_sectors']
    low_water_mark = dm_context['low_water_mark']

    thin_data_name = get_thin_data_name(dlv_name)
    dm = DmLinear(thin_data_name)
    thin_data_path = dm.get_path()
    thin_meta_name = get_thin_meta_name(dlv_name)
    dm = DmLinear(thin_meta_name)
    thin_meta_path = dm.get_path()
    pool_name = get_pool_name(dlv_name)
    dm = DmPool(pool_name)
    table = {
        'start': 0,
        'length': data_sectors,
        'meta_path': thin_meta_path,
        'data_path': thin_data_path,
        'block_sectors': thin_block_sectors,
        'low_water_mark': low_water_mark,
    }
    pool_path = dm.create(table)

    thin_name = get_thin_name(dlv_name)
    dm = DmThin(thin_name)
    table = {
        'start': 0,
        'length': dlv_sectors,
        'pool_path': pool_path,
        'thin_id': thin_id,
    }
    thin_path = dm.create(table)

    middle_name = get_middle_name(dlv_name)
    dm = DmLinear(middle_name)
    table = [{
        'start': 0,
        'length': dlv_sectors,
        'dev_path': thin_path,
        'offset': 0,
    }]
    dm.reload(table)

    final_name = get_final_name(dlv_name)
    dm = DmLinear(final_name)
    dm.resume()
    args = {
        'dlv_name': dlv_name,
        'pool_name': pool_name,
        'low_water_mark': low_water_mark,
    }
    t = Thread(target=pool_event, args=(args,))
    t.start()


def dlv_resume(dlv_name, tran, dlv_info):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        do_dlv_resume(dlv_name, dlv_info)


def do_snapshot_create(dlv_name, thin_id, ori_thin_id):
    pool_name = get_pool_name(dlv_name)
    dm = DmPool(pool_name)
    message = {
        'action': 'snap',
        'thin_id': thin_id,
        'ori_thin_id': ori_thin_id,
    }
    dm.message(message)


def snapshot_create(dlv_name, tran, thin_id, ori_thin_id):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        do_snapshot_create(
            dlv_name, thin_id, ori_thin_id)


def do_snapshot_delete(dlv_name, thin_id):
    pool_name = get_pool_name(dlv_name)
    dm = DmPool(pool_name)
    message = {
        'action': 'delete',
        'thin_id': thin_id,
    }
    dm.message(message)


def snapshot_delete(dlv_name, tran, thin_id):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        do_snapshot_delete(dlv_name, thin_id)


def do_remirror(dlv_name, dlv_info, src_id, dst_leg):
    dm_context = generate_dm_context(dlv_info['dm_context'])
    src_leg = None
    m_idx = None
    for group in dlv_info['groups']:
        legs = group['legs']
        legs.sort(key=lambda x: x['idx'])
        for m, (leg0, leg1) in enumerate(chunks(legs, 2)):
            if leg0['leg_id'] == src_id:
                src_leg = leg0
            elif leg1['leg_id'] == src_id:
                src_leg = leg1
            if src_leg is not None:
                m_idx = m
                g_idx = group['idx']
                break
        if src_leg is not None:
            break
    if src_leg is None:
        raise Exception('src_id not found: %s' % src_id)

    leg_sectors = src_leg['leg_size'] / 512
    mirror_meta_sectors = dm_context['mirror_meta_sectors']
    mirror_data_sectors = leg_sectors - mirror_meta_sectors
    mirror_region_sectors = dm_context['mirror_region_sectors']

    dst_path = None
    try:
        dst_path = login_leg(
            dst_leg['leg_id'], dst_leg['dpv_name'])
    except Exception as e:
        logger.warning(
            'remirror_dst_failed: %s %s %s',
            dst_leg['leg_id'],
            dst_leg['dpv_name'],
            e,
        )
    src_path = login_leg(
        src_leg['leg_id'], src_leg['dpv_name'])
    if dst_path is None:
        src_meta_path, src_data_path = create_mirror_leg(
            dlv_name, g_idx, src_leg, src_path, dm_context)
        mirror_name = get_mirror_name(dlv_name, g_idx, m_idx)
        dm = DmLinear(mirror_name)
        table = [{
            'start': 0,
            'length': mirror_data_sectors,
            'dpv_path': src_data_path,
            'offset': 0,
        }]
        dm.reload(table)
        report_single_leg(dlv_name, dst_leg['leg_id'])
    else:
        src_meta_path, src_data_path = create_mirror_leg(
            dlv_name, g_idx, src_leg, src_path, dm_context)
        dst_meta_path, dst_data_path = create_mirror_leg(
            dlv_name, g_idx, dst_leg, dst_path, dm_context)
        mirror_name = get_mirror_name(dlv_name, g_idx, m_idx)
        dm = DmMirror(mirror_name)
        if src_leg['idx'] < dst_leg['idx']:
            leg0 = src_leg
            meta0_path = src_meta_path
            data0_path = src_data_path
            leg1 = dst_leg
            meta1_path = dst_meta_path
            data1_path = dst_data_path
        else:
            leg0 = dst_leg
            meta0_path = dst_meta_path
            data0_path = dst_data_path
            leg1 = src_leg
            meta1_path = src_meta_path
            data1_path = src_data_path
        assert(leg0['idx'] + 1 == leg1['idx'])
        table = {
            'start': 0,
            'offset': mirror_data_sectors,
            'region_size': mirror_region_sectors,
            'meta0': meta0_path,
            'data0': data0_path,
            'meta1': meta1_path,
            'data1': data1_path,
        }
        dm.reload(table)
        args = {
            'dlv_name': dlv_name,
            'mirror_name': mirror_name,
            'leg0_id': leg0['leg_id'],
            'leg1_id': leg1['leg_id'],
        }
        t = Thread(target=mirror_event, args=(args,))
        t.start()


def remirror(dlv_name, tran, dlv_info, src_id, dst_leg):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        do_remirror(dlv_name, dlv_info, src_id, dst_leg)


def main():
    loginit()
    context_init(conf, logger)
    queue_init()
    s = WrapperRpcServer(conf.host_listener, conf.host_port)
    s.register_function(ping)
    s.register_function(bm_get)
    s.register_function(dlv_aggregate)
    s.register_function(dlv_degregate)
    s.register_function(dlv_suspend)
    s.register_function(dlv_resume)
    s.register_function(snapshot_create)
    s.register_function(snapshot_delete)
    s.register_function(remirror)
    logger.info('host_agent start')
    s.serve_forever()
