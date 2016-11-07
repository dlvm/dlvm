#!/usr/bin/env python

from threading import Lock
import logging
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from dlvm.utils.rpc_wrapper import WrapperRpcServer
from dlvm.utils.transaction import host_verify
from dlvm.utils.command import DmPool
from dlvm.utils.helper import chunks
from dlvm.utils.bitmap import BitMap


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


def main():
    loginit()
    s = WrapperRpcServer(conf.host_listener, conf.host_port)
    s.register_function(ping)
    s.register_function(bm_get)
    logger.info('host_agent start')
    s.serve_forever()


if __name__ == '__main__':
    main()
