#!/usr/bin/env python

import logging
from subprocess import Popen, PIPE
from celery import Celery
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit

logger = logging.getLogger('dlvm_monitor')

app = Celery(
    'monitor',
    broker=conf.broker_url,
)

app.conf.update(
    task_acks_late=True,
)


def run_cmd(cmd):
    cmd = ' '.join(cmd)
    sub = Popen(cmd, stdout=PIPE, shell=True)
    logger.info('cmd enter: [%s]', cmd)
    out, err = sub.communicate()
    logger.info('cmd exit: [%s] [%s] [%s]', out, err, sub.returncode)
    if sub.returncode != 0:
        raise Exception('cmd failed')


@app.task
def single_leg_failed(dlv_name, leg_id):
    cmd_name = conf.monitor_single_leg_failed
    cmd = [
        cmd_name,
        dlv_name,
        leg_id,
    ]
    run_cmd(cmd)


@app.task
def multi_legs_failed(dlv_name, leg0_id, leg1_id):
    cmd_name = conf.monitor_multi_legs_failed
    cmd = [
        cmd_name,
        dlv_name,
        leg0_id,
        leg1_id,
    ]
    run_cmd(cmd)


@app.task
def pool_full(dlv_name):
    cmd_name = conf.monitor_pool_full
    cmd = [
        cmd_name,
        dlv_name,
    ]
    run_cmd(cmd)


def start():
    loginit()
    argv = [
        'worker',
        '--loglevel=INFO',
    ]
    app.worker_main(argv)
