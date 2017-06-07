#!/usr/bin/env python

import logging
import sys
import uuid
import yaml
from dlvm.utils.loginit import loginit
from dlvm.client.layer2 import Layer2

logger = logging.getLogger('dlvm_monitor')

default_conf_path = '/etc/dlvm/client.yml'


def get_client():
    with open(default_conf_path) as f:
        conf = yaml.safe_load(f)
    client = Layer2(conf['api_server_list'])
    return client


def handle_single_leg_failed(dlv_name, leg_id):
    client = get_client()
    fj_name = uuid.uuid4().hex
    ret = client.fj_create(fj_name, dlv_name, leg_id)
    if not ret['success']:
        raise Exception(ret)


def handle_fj_mirror_complete(fj_name):
    client = get_client()
    ret = client.fj_finish(fj_name)
    if not ret['success']:
        raise Exception(ret)


max_ej_size = 100 * 1024 * 1024 * 1024
min_ej_size = 1 * 1024 * 1024 * 1024


def handle_pool_full(dlv_name):
    client = get_client()
    ret = client.dlv_display(dlv_name)
    if ret['status_code'] != 200:
        raise Exception(ret)
    data_size = ret['data']['body']['data_size']
    ej_size = min(max_ej_size, data_size)
    ej_size = max(ej_size, min_ej_size)
    ej_name = uuid.uuid4().hex
    ret = client.ej_create(ej_name, ej_size, dlv_name)
    if not ret['success']:
        raise Exception(ret)


def handle_other(args):
    logger.warning('dlvm_event: %s', args)


def handle_event():
    action = sys.argv[1]
    if action == 'single_leg_failed':
        dlv_name = sys.argv[2]
        leg_id = sys.argv[3]
        handle_single_leg_failed(dlv_name, leg_id)
    elif action == 'fj_mirror_complete':
        fj_name = sys.argv[2]
        handle_fj_mirror_complete(fj_name)
    elif action == 'pool_full':
        dlv_name = sys.argv[2]
        handle_pool_full(dlv_name)
    else:
        handle_other(sys.argv[1:])


def main():
    loginit()
    args = sys.argv[1:]
    try:
        logger.info('handle_event: %s', args)
        handle_event()
    except Exception:
        logger.error('handle_event failed: %s', args, exc_info=True)


if __name__ == '__main__':
    main()
