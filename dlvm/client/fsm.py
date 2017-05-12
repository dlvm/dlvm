#!/usr/bin/env python

import uuid
import json
from dlvm.utils.error import FsmFailed
import logging


logger = logging.getLogger('dlvm_client')

fsm = {}


def fsm_register(name, stage_info):
    fsm[name] = stage_info
    logger.debug('fsm_register: [%s] [%s]', name, stage_info)


max_retry = 3


def fsm_run(
        client, obt, stages, init_num,
        history, obt_args):
    stage_dict = {}
    stage_num = init_num
    while stage_num > 0:
        if stage_num not in stage_dict:
            stage_dict[stage_num] = 0
        stage_dict[stage_num] += 1
        if stage_dict[stage_num] > max_retry:
            error_msg = {
                'obt': obt,
                'history': history,
            }
            raise FsmFailed(error_msg)
        info = {}
        info['stage_num'] = stage_num
        stage = stages[stage_num]
        action = stage['action']
        obt['t_stage'] = stage_num
        ret = action(client, obt, obt_args)
        info['action_ret'] = ret
        check = stage['check']
        status, msg = check(client, obt_args)
        assert(status in ('ok', 'err'))
        info['check_status'] = status
        info['check_msg'] = msg
        history.append(info)
        stage_num = stage[status]

    client.obt_delete(t_id=obt['t_id'], t_owner=obt['t_owner'])
    if stage_num == -1:
        return {'success': True}
    else:
        return {
            'success': False,
            'history': history,
        }


def fsm_start(name, client, obt_args):
    t_id = uuid.uuid4().hex
    t_owner = uuid.uuid4().hex
    t_stage = 0
    annotation = {
        'name': name,
    }
    annotation.update(obt_args)
    annotation = json.dumps(annotation)
    client.obts_post(
        t_id=t_id, t_owner=t_owner, t_stage=t_stage, annotation=annotation)
    stage_info = fsm[name]
    init_num = stage_info['init_stage_num']
    stages = stage_info['stages']
    obt = {
        't_id': t_id,
        't_owner': t_owner,
    }
    history = []
    return fsm_run(
        client, obt, stages, init_num, history, obt_args)


def fsm_resume(client, t_id):
    ret = client.obt_get(t_id=t_id)
    t_owner = ret['data']['body']['t_owner']
    t_stage = ret['data']['body']['t_stage']
    new_owner = uuid.uuid4().hex
    annotation = ret['data']['body']['annotation']
    client.obt_put(
        t_id=t_id, t_owner=t_owner, action='preempt', new_owner=new_owner)
    annotation = json.loads(annotation)
    name = annotation['name']
    obt_args = annotation
    del obt_args['name']
    history = []
    stage_info = fsm[name]
    info = {}
    stages = stage_info['stages']
    stage_num = t_stage
    obt = {
        't_id': t_id,
        't_owner': new_owner,
    }
    if stage_num == 0:
        stage_num = stage_info['init_stage_num']
        return fsm_run(
            client, obt, stages, stage_num, history, obt_args)
    info['stage_num'] = stage_num
    stage = stages[stage_num]
    check = stage['check']
    status, msg = check(client, obt_args)
    assert(status in ('ok', 'err'))
    info['check_status'] = status
    info['check_msg'] = msg
    history.append(info)
    init_num = stage[status]
    return fsm_run(
        client, obt, stages, init_num, history, obt_args)
