#!/usr/bin/env python

import logging
import time
from layer1 import Layer1
from fsm import fsm_register, fsm_start, fsm_resume

logger = logging.getLogger('dlvm_client')


def dpv_available_action(client, obt, obt_args):
    kwargs = {
        'dpv_name': obt_args['dpv_name'],
        'action': 'available',
        't_id': obt['t_id'],
        't_owner': obt['t_owner'],
        't_stage': obt['t_stage'],
    }
    ret = client.dpv_put(**kwargs)
    return ret


def dpv_available_check(client, obt, obt_args):
    retry = 0
    while retry < obt_args['max_retry']:
        dpv_name = obt_args['dpv_name']
        ret = client.dpv_get(dpv_name=dpv_name)
        if ret['body']['status'] == 'available':
            return 'ok', ret
        time.sleep(obt_args['interval'])
        retry += 1
    return 'err', ret


dpv_available_stage_info = {
    'init_stage_num': 1,
    'stages': {
        1: {
            'action': dpv_available_action,
            'check': dpv_available_check,
            'ok': -1,
            'err': -2,
        },
    },
}

fsm_register('dpv_available', dpv_available_stage_info)


def thost_available_action(client, obt, obt_args):
    kwargs = {
        'thost_name': obt_args['thost_name'],
        'action': 'available',
        't_id': obt['t_id'],
        't_owner': obt['t_owner'],
        't_stage': obt['t_stage'],
    }
    ret = client.thost_put(**kwargs)
    return ret


def thost_available_check(client, obt, obt_args):
    retry = 0
    while retry < obt_args['max_retry']:
        thost_name = obt_args['thost_name']
        ret = client.thost_get(thost_name=thost_name)
        if ret['body']['status'] == 'available':
            return 'ok', ret
        time.sleep(obt_args['interval'])
        retry += 1
    return 'err', ret


thost_available_stage_info = {
    'init_stage_num': 1,
    'stages': {
        1: {
            'action': thost_available_action,
            'check': thost_available_check,
            'ok': -1,
            'err': -2,
        },
    },
}

fsm_register('thost_available', thost_available_stage_info)


class Layer2(object):

    def __init__(self, api_server_list):
        self.client = Layer1(api_server_list)

    def dpv_list(self):
        ret = self.client.dpvs_get()
        return ret['data']

    def dpv_display(self, dpv_name):
        ret = self.client.dpv_get(dpv_name=dpv_name)
        return ret['data']

    def dpv_create(self, dpv_name):
        ret = self.client.dpvs_post(dpv_name=dpv_name)
        return ret['data']

    def dpv_remove(self, dpv_name):
        ret = self.client.dpv_delete(dpv_name=dpv_name)
        return ret['data']

    def dpv_available(self, dpv_name):
        obt_args = {
            'dpv_name': dpv_name,
            'max_retry': 10,
            'interval': 1,
        }
        return fsm_start(
            'dpv_available', self.client, obt_args)

    def dpv_unavailable(self, dpv_name):
        ret = self.client.dpv_put(
            dpv_name=dpv_name, action='unavailable')
        return ret['data']

    def obt_list(self):
        ret = self.client.obts_get()
        return ret['data']

    def obt_get(self, t_id):
        ret = self.client.obt_get(t_id=t_id)
        return ret['data']

    def obt_resume(self, t_id):
        return fsm_resume(self.client, t_id)

    def thost_list(self):
        ret = self.client.thosts_get()
        return ret['data']

    def thost_get(self, thost_name):
        ret = self.client.thost_get(thost_name=thost_name)
        return ret['data']

    def thost_delete(self, thost_name):
        ret = self.client.thost_delete(thost_name=thost_name)
        return ret['data']

    def thost_unavailable(self, thost_name):
        ret = self.client.thost_put(
            thost_name=thost_name, action='unavailable')
        return ret['data']

    def thost_available(self, thost_name):
        obt_args = {
            'thost_name': thost_name,
            'max_retry': 10,
            'interval': 1,
        }
        return fsm_start(
            'thost_available', self.client, obt_args)
