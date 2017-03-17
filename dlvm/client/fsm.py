#!/usr/bin/env python

import uuid
import json
import logging
from dlvm.utils.error import FsmFailed
from dlvm.utils.configure import conf


logger = logging.getLogger('dlvm_client')

fsm = {}


def fsm_register(name, stage_info):
    fsm[name] = stage_info
    logger.debug('fsm_register: [%s] [%s]', name, stage_info)


def fsm_run(client, obt, stage, stage_num, max_retry, history, kwargs):
    stage_dict = {}
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
        action = stage['action']
        obt['t_stage'] = stage_num
        ret = action(client, obt, **kwargs)
        info['action_ret'] = ret
        check = stage['check']
        status, msg = check(client, **kwargs)
        assert(status in ('ok', 'err'))
        info['check_status'] = status
        info['check_msg'] = msg
        history.append(info)
        stage_num = stage[status]

    client.obt_delete(t_id=obt['t_id'], t_owner=obt['t_owner'])
    if stage_num == -1:
        success = True
    else:
        success = False
    return success, history


def fsm_start(name, client, **kwargs):
    t_id = uuid.uuid4()
    t_owner = uuid.uuid4()
    t_stage = 0
    annotation = {
        'name': name,
    }
    annotation.update(kwargs)
    annotation = json.dumps(annotation)
    client.obts_post(
        t_id=t_id, t_owner=t_owner, t_stage=t_stage, annotation=annotation)
    stage_info = fsm[name]
    stage_num = stage_info['init_stage_num']
    stage = stage_info['stages'][stage_num]
    obt = {
        't_id': t_id,
        't_owner': t_owner,
    }
    history = []
    max_retry = conf.fsm_max_retry
    return fsm_run(client, obt, stage, stage_num, max_retry, history, kwargs)


def fsm_resume(client, t_id):
    ret = client.obt_get(t_id=t_id)
    t_owner = ret['data']['body']['t_owner']
    new_owner = uuid.uuid4()
    annotation = ret['data']['body']['annotation']
    client.obt_put(
        t_id=t_id, t_owner=t_owner, action='preempt', new_owner=new_owner)
    annotation = json.loads(annotation)
    name = annotation['name']
    kwargs = annotation
    del kwargs['name']
    history = []
    stage_info = fsm[name]
    info = []
    stage_num = ret['t_stage']
    info['stage_num': stage_num]
    stage = stage_info['stages'][stage_num]
    check = stage['check']
    status, msg = check(client, **kwargs)
    assert(status in ('ok', 'err'))
    info['check_status'] = status
    info['check_msg'] = msg
    history.append(info)
    stage_num = stage[status]
    obt = {
        't_id': t_id,
        't_owner': new_owner,
    }
    max_retry = conf.fsm_max_retry
    return fsm_run(obt, stage, stage_num, max_retry, history, kwargs)
