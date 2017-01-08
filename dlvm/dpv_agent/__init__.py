#!/usr/bin/env python

import os
import time
from threading import Thread, Lock
import logging
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from dlvm.utils.rpc_wrapper import WrapperRpcServer
from dlvm.utils.obt import dpv_verify
from dlvm.utils.command import context_init, \
    DmBasic, DmLinear, DmMirror, \
    lv_create, lv_remove, lv_get_path, vg_get_size, \
    run_dd, \
    iscsi_create, iscsi_delete, \
    iscsi_export, iscsi_unexport, \
    iscsi_login, iscsi_logout
from dlvm.utils.helper import encode_target_name, encode_initiator_name
from dlvm.utils.bitmap import BitMap
from dlvm.utils.queue import queue_init, \
    report_mj_mirror_failed, report_mj_mirror_complete
from mirror_meta import generate_mirror_meta


logger = logging.getLogger('dlvm_dpv')


class DpvError(Exception):
    pass

global_rpc_lock = Lock()
global_rpc_set = set()


class RpcLock(object):

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        with global_rpc_lock:
            if self.name in global_rpc_set:
                raise DpvError('rpc_conflict: %s' % self.name)
            global_rpc_set.add(self.name)

    def __exit__(self, exc_type, exc_value, traceback):
        with global_rpc_lock:
            global_rpc_set.remove(self.name)


mj_thread_set = set()
mj_thread_lock = Lock()


def mj_thread_add(leg_id):
    with mj_thread_lock:
        mj_thread_set.add(leg_id)


def mj_thread_remove(leg_id):
    with mj_thread_lock:
        if leg_id in mj_thread_set:
            mj_thread_set.remove(leg_id)


def mj_thread_check(leg_id):
    with mj_thread_lock:
        return leg_id in mj_thread_set


def ping(message):
    return message


def get_dpv_info():
    total_size, free_size = vg_get_size(conf.local_vg)
    return {
        'total_size': str(total_size),
        'free_size': str(free_size),
    }


def get_layer1_name(leg_id):
    return '{dlvm_prefix}-{leg_id}-1'.format(
        dlvm_prefix=conf.dlvm_prefix, leg_id=leg_id)


def get_layer2_name(leg_id):
    return '{dlvm_prefix}-{leg_id}-2'.format(
        dlvm_prefix=conf.dlvm_prefix, leg_id=leg_id)


def get_layer2_name_mj(leg_id, mj_name):
    return '{dlvm_prefix}-{leg_id}-mj-{mj_name}'.format(
        dlvm_prefix=conf.dlvm_prefix,
        leg_id=leg_id,
        mj_name=mj_name,
    )


def get_mj_meta0_name(leg_id, mj_name):
    return '{leg_id}-{mj_name}-0'.format(
        leg_id=leg_id, mj_name=mj_name)


def get_mj_meta1_name(leg_id, mj_name):
    return '{leg_id}-{mj_name}-1'.format(
        leg_id=leg_id, mj_name=mj_name)


def do_leg_create(leg_id, leg_size, dm_context):
    lv_path = lv_create(leg_id, leg_size, conf.local_vg)
    leg_sectors = leg_size / 512
    layer1_name = get_layer1_name(leg_id)
    dm = DmLinear(layer1_name)
    table = [{
        'start': 0,
        'length': leg_sectors,
        'dev_path': lv_path,
        'offset': 0,
    }]
    layer1_path = dm.create(table)

    layer2_name = get_layer2_name(leg_id)
    dm = DmLinear(layer2_name)
    table = [{
        'start': 0,
        'length': leg_sectors,
        'dev_path': layer1_path,
        'offset': 0,
    }]
    layer2_path = dm.create(table)

    thin_block_size = dm_context['thin_block_size']
    mirror_meta_blocks = dm_context['mirror_meta_blocks']
    mirror_meta_size = thin_block_size * mirror_meta_blocks
    mirror_data_size = leg_size - mirror_meta_size
    mirror_region_size = dm_context['mirror_region_size']
    file_name = 'dlvm-leg-{leg_id}'.format(leg_id=leg_id)
    file_path = os.path.join(conf.tmp_dir, file_name)
    bm = BitMap(mirror_data_size/mirror_region_size)
    generate_mirror_meta(
        file_path,
        mirror_meta_size,
        mirror_data_size,
        mirror_region_size,
        bm,
    )
    run_dd(file_path, layer2_path)
    os.remove(file_path)

    target_name = encode_target_name(leg_id)
    iscsi_create(target_name, leg_id, layer2_path)


def leg_create(leg_id, leg_size, dm_context, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_leg_create(leg_id, leg_size, dm_context)


def do_leg_delete(leg_id):
    layer2_name = get_layer2_name(leg_id)
    dm = DmLinear(layer2_name)
    layer2_path = dm.get_path()
    target_name = encode_target_name(leg_id)
    iscsi_delete(target_name, layer2_path)
    dm.remove()
    layer1_name = get_layer1_name(leg_id)
    dm = DmLinear(layer1_name)
    dm.remove()
    lv_remove(leg_id, conf.local_vg)

    file_name = 'dlvm-leg-{leg_id}'.format(leg_id=leg_id)
    file_path = os.path.join(conf.tmp_dir, file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)


def leg_delete(leg_id, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_leg_delete(leg_id)


def do_leg_export(leg_id, host_name):
    target_name = encode_target_name(leg_id)
    initiator_name = encode_initiator_name(host_name)
    iscsi_export(target_name, initiator_name)


def leg_export(leg_id, host_name, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_leg_export(leg_id, host_name)


def do_leg_unexport(leg_id, host_name):
    target_name = encode_target_name(leg_id)
    initiator_name = encode_initiator_name(host_name)
    iscsi_unexport(target_name, initiator_name)


def leg_unexport(leg_id, host_name, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_leg_unexport(leg_id, host_name)


def do_mj_leg_export(leg_id, mj_name, src_name, leg_size):
    leg_sectors = leg_size / 512
    layer1_name = get_layer1_name(leg_id)
    dm = DmLinear(layer1_name)
    layer1_path = dm.get_path()
    layer2_name = get_layer2_name_mj(leg_id, mj_name)
    dm = DmLinear(layer2_name)
    table = [{
        'start': 0,
        'length': leg_sectors,
        'dev_path': layer1_path,
        'offset': 0,
    }]
    layer2_path = dm.create(table)
    target_name = encode_target_name(layer2_name)
    iscsi_create(target_name, layer2_name, layer2_path)
    initiator_name = encode_initiator_name(src_name)
    iscsi_export(target_name, initiator_name)


def mj_leg_export(
        leg_id, mj_name, src_name, leg_size, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_mj_leg_export(leg_id, mj_name, src_name, leg_size)


def do_mj_leg_unexport(leg_id, mj_name, src_name):
    layer2_name = get_layer2_name_mj(leg_id, mj_name)
    target_name = encode_initiator_name(layer2_name)
    initiator_name = encode_initiator_name(src_name)
    iscsi_unexport(target_name, initiator_name)
    dm = DmLinear(layer2_name)
    layer2_path = dm.get_path()
    iscsi_delete(target_name, layer2_path)
    dm.remove()


def mj_leg_unexport(
        leg_id, mj_name, src_name, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_mj_leg_unexport(leg_id, mj_name, src_name)


def do_mj_login(leg_id, mj_name, dst_name, dst_id):
    dst_layer2_name = get_layer2_name_mj(dst_id, mj_name)
    target_name = encode_target_name(dst_layer2_name)
    iscsi_login(target_name, dst_name)
    mj_meta0_name = get_mj_meta0_name(leg_id, mj_name)
    lv_create(mj_meta0_name, conf.mj_meta_size, conf.local_vg)
    mj_meta1_name = get_mj_meta1_name(leg_id, mj_name)
    lv_create(mj_meta1_name, conf.mj_meta_size, conf.local_vg)


def mj_login(
        leg_id, mj_name,
        dst_name, dst_id, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_mj_login(
            leg_id, mj_name, dst_name, dst_id)


def mj_mirror_event(args):
    leg_id = args['leg_id']
    mj_name = args['mj_name']
    mirror_name = args['mirror_name']
    dm = DmMirror(mirror_name)
    while 1:
        time.sleep(conf.mj_mirror_interval)
        if mj_thread_check(leg_id) is False:
            break
        try:
            status = dm.status()
        except Exception as e:
            logger.info('mj mirror status failed: %s %s', args, e)
            report_mj_mirror_failed(mj_name)
            break
        if status['hc0'] == 'D' or status['hc1'] == 'D':
            report_mj_mirror_failed(mj_name)
            break
        elif status['hc0'] == 'A' and status['hc1'] == 'A':
            report_mj_mirror_complete(mj_name)
            break


def do_mj_mirror_start(
        leg_id, mj_name, dst_name, dst_id, leg_size, dmc, bm):
    layer1_name = get_layer1_name(leg_id)
    dm = DmBasic(layer1_name)
    dm_type = dm.get_type()
    if dm_type == 'raid':
        return
    bm = BitMap.fromhexstring(bm)
    file_name = 'dlvm-{mj_name}'.format(mj_name=mj_name)
    file_path = os.path.join(conf.tmp_dir, file_name)
    mj_meta0_name = get_mj_meta0_name(leg_id, mj_name)
    mj_meta0_path = lv_get_path(mj_meta0_name, conf.local_vg)
    mj_meta1_name = get_mj_meta1_name(leg_id, mj_name)
    mj_meta1_path = lv_get_path(mj_meta1_name, conf.local_vg)
    generate_mirror_meta(
        file_path,
        conf.mj_meta_size,
        leg_size,
        dmc['thin_block_size'],
        bm,
    )
    run_dd(file_path, mj_meta0_path)
    run_dd(file_path, mj_meta1_path)
    os.remove(file_path)

    leg_sectors = leg_size / 512
    dst_layer2_name = get_layer2_name_mj(dst_id, mj_name)
    target_name = encode_target_name(dst_layer2_name)
    # the dst should have already login, the iscsi_login is
    # an idempotent option, call it again just for getting dev path
    dst_dev_path = iscsi_login(target_name, dst_name)
    src_dev_path = lv_get_path(leg_id, conf.local_vg)
    layer1_name = get_layer1_name(leg_id)
    dm = DmMirror(layer1_name)
    table = {
        'start': 0,
        'offset': leg_sectors,
        'region_size': dmc['thin_block_size'],
        'meta0': mj_meta0_path,
        'data0': src_dev_path,
        'meta1': mj_meta1_path,
        'data1': dst_dev_path,
    }
    dm.reload(table)
    mj_thread_add(leg_id)
    args = {
        'leg_id': leg_id,
        'mj_name': mj_name,
        'mirror_name': layer1_name,
    }
    t = Thread(target=mj_mirror_event, args=(args,))
    t.start()


def mj_mirror_start(
        leg_id, mj_name,
        dst_name, dst_id,
        leg_size, dmc, bm, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_mj_mirror_start(
            leg_id, mj_name, dst_name, dst_id, leg_size, dmc, bm)


def do_mj_mirror_stop(leg_id, mj_name, dst_id, leg_size):
    mj_thread_remove(leg_id)
    leg_sectors = leg_size / 512
    layer1_name = get_layer1_name(leg_id)
    dm = DmLinear(layer1_name)
    lv_path = lv_get_path(leg_id, conf.local_vg)
    table = [{
        'start': 0,
        'length': leg_sectors,
        'dev_path': lv_path,
        'offset': 0,
    }]
    dm.reload(table)
    mj_meta0_name = get_mj_meta0_name(leg_id, mj_name)
    lv_remove(mj_meta0_name, conf.local_vg)
    mj_meta1_name = get_mj_meta1_name(leg_id, mj_name)
    lv_remove(mj_meta1_name, conf.local_vg)
    dst_layer2_name = get_layer2_name_mj(dst_id, mj_name)
    target_name = encode_target_name(dst_layer2_name)
    iscsi_logout(target_name)


def mj_mirror_stop(
        leg_id, mj_name, dst_id, leg_size, tran):
    with RpcLock(leg_id):
        dpv_verify(leg_id, tran['major'], tran['minor'])
        do_mj_mirror_stop(
            leg_id, mj_name, dst_id, leg_size)


def do_mj_mirror_status(leg_id):
    layer1_name = get_layer1_name(leg_id)
    dm = DmBasic(layer1_name)
    dm_type = dm.get_type()
    if dm_type != 'raid':
        raise DpvError('wrong dm_type: %s' % dm_type)
    dm = DmMirror(layer1_name)
    status = dm.status()
    return {
        'hc0': status['hc0'],
        'hc1': status['hc1'],
        'curr': status['curr'],
        'total': status['total'],
        'sync_action': status['sync_action'],
        'mismatch_cnt': status['mismatch_cnt'],
    }


def mj_mirror_status(leg_id):
    with RpcLock(leg_id):
        return do_mj_mirror_status(leg_id)


def main():
    loginit()
    context_init(conf, logger)
    queue_init()
    s = WrapperRpcServer(conf.dpv_listener, conf.dpv_port)
    s.register_function(ping)
    s.register_function(get_dpv_info)
    s.register_function(leg_create)
    s.register_function(leg_delete)
    s.register_function(leg_export)
    s.register_function(leg_unexport)
    s.register_function(mj_leg_export)
    s.register_function(mj_leg_unexport)
    s.register_function(mj_login)
    s.register_function(mj_mirror_start)
    s.register_function(mj_mirror_stop)
    s.register_function(mj_mirror_status)
    logger.info('dpv_agent start')
    s.serve_forever()
