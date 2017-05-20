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
    iscsi_login, iscsi_logout, \
    iscsi_target_get_all, iscsi_target_delete, \
    iscsi_backstore_get_all, iscsi_backstore_delete, \
    dm_get_all, lv_get_all
from dlvm.utils.helper import encode_target_name, encode_initiator_name
from dlvm.utils.bitmap import BitMap
from dlvm.utils.queue import queue_init, \
    report_fj_mirror_failed, report_fj_mirror_complete
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
            logger.info('lock resource: %s', self.name)

    def __exit__(self, exc_type, exc_value, traceback):
        with global_rpc_lock:
            global_rpc_set.remove(self.name)
            logger.info('unlock resource: %s', self.name)


fj_thread_set = set()
fj_thread_lock = Lock()


def fj_thread_add(leg_id):
    with fj_thread_lock:
        fj_thread_set.add(leg_id)


def fj_thread_remove(leg_id):
    with fj_thread_lock:
        if leg_id in fj_thread_set:
            fj_thread_set.remove(leg_id)


def fj_thread_check(leg_id):
    with fj_thread_lock:
        return leg_id in fj_thread_set


def ping(message):
    return message


def dpv_get_info():
    total_size, free_size = vg_get_size(conf.local_vg)
    return {
        'total_size': str(total_size),
        'free_size': str(free_size),
    }


def get_layer1_name(leg_id):
    return '{dpv_prefix}-{leg_id}-1'.format(
        dpv_prefix=conf.dpv_prefix, leg_id=leg_id)


def get_layer2_name(leg_id):
    return '{dpv_prefix}-{leg_id}-2'.format(
        dpv_prefix=conf.dpv_prefix, leg_id=leg_id)


def get_layer2_name_fj(leg_id, fj_name):
    return '{dpv_prefix}-{leg_id}-fj-{fj_name}'.format(
        dpv_prefix=conf.dpv_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
    )


def get_fj_meta0_name(leg_id, fj_name):
    return '{dpv_prefix}-{leg_id}-{fj_name}-0'.format(
        dpv_prefix=conf.dpv_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
    )


def get_fj_meta1_name(leg_id, fj_name):
    return '{dpv_prefix}-{leg_id}-{fj_name}-1'.format(
        dpv_prefix=conf.dpv_prefix,
        leg_id=leg_id,
        fj_name=fj_name,
    )


def do_leg_create(leg_id, leg_size, dm_context, init):
    if init is True:
        lv_path = lv_create(leg_id, leg_size, conf.local_vg)
    else:
        lv_path = lv_get_path(leg_id, conf.local_vg)
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
    if init is True:
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
        run_dd(
            '/dev/zero',
            layer2_path,
            bs=thin_block_size,
            seek=mirror_meta_blocks,
            count=1,
        )
        os.remove(file_path)

    target_name = encode_target_name(leg_id)
    iscsi_create(target_name, leg_id, layer2_path)


def leg_create(leg_id, obt, leg_size, dm_context):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        do_leg_create(leg_id, int(leg_size), dm_context, True)


def do_leg_delete(leg_id):
    layer2_name = get_layer2_name(leg_id)
    dm = DmLinear(layer2_name)
    # layer2_path = dm.get_path()
    target_name = encode_target_name(leg_id)
    iscsi_delete(target_name, leg_id)
    dm.remove()
    layer1_name = get_layer1_name(leg_id)
    dm = DmLinear(layer1_name)
    dm.remove()
    lv_remove(leg_id, conf.local_vg)

    file_name = 'dlvm-leg-{leg_id}'.format(leg_id=leg_id)
    file_path = os.path.join(conf.tmp_dir, file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)


def leg_delete(leg_id, obt):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        do_leg_delete(leg_id)


def do_leg_export(leg_id, ihost_name):
    target_name = encode_target_name(leg_id)
    initiator_name = encode_initiator_name(ihost_name)
    iscsi_export(target_name, initiator_name)


def leg_export(leg_id, obt, ihost_name):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        do_leg_export(leg_id, ihost_name)


def do_leg_unexport(leg_id, ihost_name):
    target_name = encode_target_name(leg_id)
    initiator_name = encode_initiator_name(ihost_name)
    iscsi_unexport(target_name, initiator_name)


def leg_unexport(leg_id, obt, ihost_name):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        do_leg_unexport(leg_id, ihost_name)


def do_fj_leg_export(leg_id, fj_name, src_name, leg_size):
    leg_sectors = leg_size / 512
    layer1_name = get_layer1_name(leg_id)
    dm = DmLinear(layer1_name)
    layer1_path = dm.get_path()
    layer2_name = get_layer2_name_fj(leg_id, fj_name)
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


def fj_leg_export(
        leg_id, obt, fj_name, src_name, leg_size):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        do_fj_leg_export(leg_id, fj_name, src_name, int(leg_size))


def do_fj_leg_unexport(leg_id, fj_name, src_name):
    layer2_name = get_layer2_name_fj(leg_id, fj_name)
    target_name = encode_target_name(layer2_name)
    initiator_name = encode_initiator_name(src_name)
    iscsi_unexport(target_name, initiator_name)
    iscsi_delete(target_name, layer2_name)
    dm = DmLinear(layer2_name)
    dm.remove()


def fj_leg_unexport(
        leg_id, obt, fj_name, src_name):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        do_fj_leg_unexport(leg_id, fj_name, src_name)


def do_fj_login(leg_id, fj_name, dst_name, dst_id):
    dst_layer2_name = get_layer2_name_fj(dst_id, fj_name)
    target_name = encode_target_name(dst_layer2_name)
    iscsi_login(target_name, dst_name)
    fj_meta0_name = get_fj_meta0_name(leg_id, fj_name)
    lv_create(fj_meta0_name, conf.fj_meta_size, conf.local_vg)
    fj_meta1_name = get_fj_meta1_name(leg_id, fj_name)
    lv_create(fj_meta1_name, conf.fj_meta_size, conf.local_vg)


def fj_login(
        leg_id, obt, fj_name,
        dst_name, dst_id):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        do_fj_login(
            leg_id, fj_name, dst_name, dst_id)


def fj_mirror_event(args):
    leg_id = args['leg_id']
    fj_name = args['fj_name']
    mirror_name = args['mirror_name']
    dm = DmMirror(mirror_name)
    while 1:
        time.sleep(conf.fj_mirror_interval)
        if fj_thread_check(leg_id) is False:
            break
        try:
            status = dm.status()
        except Exception as e:
            logger.info('fj mirror status failed: %s %s', args, e)
            report_fj_mirror_failed(fj_name)
            break
        if status['hc0'] == 'D' or status['hc1'] == 'D':
            report_fj_mirror_failed(fj_name)
            break
        elif status['hc0'] == 'A' and status['hc1'] == 'A':
            report_fj_mirror_complete(fj_name)
            break


def do_fj_mirror_start(
        leg_id, fj_name, dst_name, dst_id, leg_size, dmc, bm):
    layer1_name = get_layer1_name(leg_id)
    dm = DmBasic(layer1_name)
    dm_type = dm.get_type()
    if dm_type == 'raid':
        return
    logger.info('bm=[%s]', bm)
    bm = BitMap.fromhexstring(bm)
    file_name = 'dlvm-{fj_name}'.format(fj_name=fj_name)
    file_path = os.path.join(conf.tmp_dir, file_name)
    fj_meta0_name = get_fj_meta0_name(leg_id, fj_name)
    fj_meta0_path = lv_get_path(fj_meta0_name, conf.local_vg)
    fj_meta1_name = get_fj_meta1_name(leg_id, fj_name)
    fj_meta1_path = lv_get_path(fj_meta1_name, conf.local_vg)
    generate_mirror_meta(
        file_path,
        conf.fj_meta_size,
        leg_size,
        dmc['thin_block_size'],
        bm,
    )
    run_dd(file_path, fj_meta0_path)
    run_dd(file_path, fj_meta1_path)
    os.remove(file_path)

    leg_sectors = leg_size / 512
    dst_layer2_name = get_layer2_name_fj(dst_id, fj_name)
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
        'meta0': fj_meta0_path,
        'data0': src_dev_path,
        'meta1': fj_meta1_path,
        'data1': dst_dev_path,
    }
    dm.reload(table)
    fj_thread_add(leg_id)
    args = {
        'leg_id': leg_id,
        'fj_name': fj_name,
        'mirror_name': layer1_name,
    }
    t = Thread(target=fj_mirror_event, args=(args,))
    t.start()


def fj_mirror_start(
        leg_id, obt, fj_name,
        dst_name, dst_id,
        leg_size, dmc, bm):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        logger.info('leg_size: [%s]', leg_size)
        do_fj_mirror_start(
            leg_id, fj_name, dst_name, dst_id, int(leg_size), dmc, bm)


def do_fj_mirror_stop(leg_id, fj_name, dst_id, leg_size):
    fj_thread_remove(leg_id)
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
    fj_meta0_name = get_fj_meta0_name(leg_id, fj_name)
    lv_remove(fj_meta0_name, conf.local_vg)
    fj_meta1_name = get_fj_meta1_name(leg_id, fj_name)
    lv_remove(fj_meta1_name, conf.local_vg)
    dst_layer2_name = get_layer2_name_fj(dst_id, fj_name)
    target_name = encode_target_name(dst_layer2_name)
    iscsi_logout(target_name)


def fj_mirror_stop(
        leg_id, obt, fj_name, dst_id, leg_size):
    with RpcLock(leg_id):
        dpv_verify(leg_id, obt['major'], obt['minor'])
        do_fj_mirror_stop(
            leg_id, fj_name, dst_id, int(leg_size))


def do_fj_mirror_status(leg_id):
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


def fj_mirror_status(leg_id):
    with RpcLock(leg_id):
        return do_fj_mirror_status(leg_id)


def dpv_release_unused(leg_id_list):
    logger.debug('leg_id_list: %s', leg_id_list)
    target_name_set = set()
    backstore_name_set = set()
    dm_name_set = set()
    lv_name_set = set(leg_id_list)
    for leg_id in leg_id_list:
        target_name_set.add(encode_target_name(leg_id))
        backstore_name_set.add(leg_id)
        dm_name_set.add(get_layer1_name(leg_id))
        dm_name_set.add(get_layer2_name(leg_id))
    iscsi_target_list = iscsi_target_get_all(conf.target_prefix)
    for iscsi_target in iscsi_target_list:
        if iscsi_target not in target_name_set:
            iscsi_target_delete(iscsi_target)
    iscsi_backstore_list = iscsi_backstore_get_all(conf.dpv_prefix)
    logger.debug('iscsi_backstore_list: %s', iscsi_backstore_list)
    logger.debug('backstore_name_set: %s', backstore_name_set)
    for iscsi_backstore in iscsi_backstore_list:
        if iscsi_backstore not in backstore_name_set:
            iscsi_backstore_delete(iscsi_backstore)
    dm_name_list = dm_get_all(conf.dpv_prefix)
    logger.debug('dm_name_list: %s', dm_name_list)
    logger.debug('dm_name_set: %s', dm_name_set)
    level_1_list = []
    for dm_name in dm_name_list:
        if dm_name not in dm_name_set:
            if dm_name.endswith('-1'):
                level_1_list.append(dm_name)
            else:
                dm = DmBasic(dm_name)
                dm.remove()
    for dm_name in level_1_list:
        dm = DmBasic(dm_name)
        dm.remove()
    lv_name_list = lv_get_all(conf.local_vg)
    logger.debug('lv_name_list: %s', lv_name_list)
    logger.debug('lv_name_set: %s', lv_name_set)
    for lv_name in lv_name_list:
        if lv_name not in lv_name_set:
            lv_remove(lv_name, conf.local_vg)


def dpv_sync(dpv_info, obt):
    leg_id_list = []
    for leg_info in dpv_info:
        leg_id = leg_info['leg_id']
        leg_size = leg_info['leg_size']
        dm_context = leg_info['dm_context']
        ihost_name = leg_info['ihost_name']
        with RpcLock(leg_id):
            do_leg_create(leg_id, int(leg_size), dm_context, False)
            if ihost_name is None:
                do_leg_unexport(leg_id, ihost_name)
            else:
                do_leg_export(leg_id, ihost_name)
        leg_id_list.append(leg_id)
    dpv_release_unused(leg_id_list)
    return dpv_get_info()


def main():
    loginit()
    context_init(conf, logger)
    queue_init()
    s = WrapperRpcServer(conf.dpv_listener, conf.dpv_port, logger)
    s.register_function(ping)
    s.register_function(dpv_get_info)
    s.register_function(dpv_sync)
    s.register_function(leg_create)
    s.register_function(leg_delete)
    s.register_function(leg_export)
    s.register_function(leg_unexport)
    s.register_function(fj_leg_export)
    s.register_function(fj_leg_unexport)
    s.register_function(fj_login)
    s.register_function(fj_mirror_start)
    s.register_function(fj_mirror_stop)
    s.register_function(fj_mirror_status)
    logger.info('dpv_agent start')
    s.serve_forever()
