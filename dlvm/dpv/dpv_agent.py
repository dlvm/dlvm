#!/usr/bin/env python

import os
from threading import Thread, Lock
import logging
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from dlvm.utils.rpc_wrapper import WrapperRpcServer
from dlvm.utils.transaction import dpv_verify
from dlvm.utils.command import context_init, \
    DmLinear, \
    lv_create, lv_remove, \
    run_dd, \
    iscsi_create, iscsi_delete, \
    iscsi_export, iscsi_unexport
from dlvm.utils.helper import encode_target_name, encode_initiator_name
from dlvm.utils.bitmap import BitMap
from dlvm.utils.queue import queue_init
from mirror_meta import generate_mirror_meta


logger = logging.getLogger('dlvm_dpv')


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


def get_layer1_name(leg_id):
    return '{dlvm_prefix}-{leg_id}-1'.format(
        dlvm_prefix=conf.dlvm_prefix, leg_id=leg_id)


def get_layer2_name(leg_id):
    return '{dlvm_prefix}-{leg_id}-2'.format(
        dlvm_prefix=conf.dlvm_prefix, leg_id=leg_id)


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


def main():
    loginit()
    context_init(conf, logger)
    queue_init()
    s = WrapperRpcServer(conf.dpv_listener, conf.dpv_port)
    s.register_function(ping)
    s.register_function(leg_create)
    s.register_function(leg_delete)
    s.register_function(leg_export)
    s.register_function(leg_unexport)
    logger.info('dpv_agent start')
    s.serve_forever()
