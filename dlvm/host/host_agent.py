#!/usr/bin/env python

from threading import Lock
import logging
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from dlvm.utils.rpc_wrapper import WrapperRpcServer
from dlvm.utils.transaction import host_verify


# class NullHandler(logging.Handler):
#     def emit(self, record):
#         pass


logger = logging.getLogger('dlvm_host')
# logger.addHandler(NullHandler())

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


def do_bm_get(dlv_name, dlv_info, thin_id_list, leg_id):
    pass


def bm_get(dlv_name, tran, dlv_info, thin_id_list, leg_id):
    with RpcLock(dlv_name):
        host_verify(dlv_name, tran['major'], tran['minor'])
        return do_bm_get(dlv_name, dlv_info, thin_id_list, leg_id)


def main():
    loginit()
    s = WrapperRpcServer(conf.host_listener, conf.host_port)
    s.register_function(ping)
    logger.info('host_agent start')
    s.serve_forever()


if __name__ == '__main__':
    main()
