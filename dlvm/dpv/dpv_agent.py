#!/usr/bin/env python

from threading import Thread, Lock
import logging
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit
from dlvm.utils.rpc_wrapper import WrapperRpcServer
from dlvm.utils.transaction import dpv_verify
from dlvm.utils.command import context_init
from dlvm.utils.helper import encode_target_name
from dlvm.utils.bitmap import BitMap
from dlvm.utils.queue import queue_init


logger = logging.getLogger('dlvm_dpv')


def ping(message):
    return message


def main():
    loginit()
    context_init(conf, logger)
    queue_init()
    s = WrapperRpcServer(conf.dpv_listener, conf.dpv_port)
    s.register_function(ping)
    logger.info('dpv_agent start')
    s.serve_forever()
