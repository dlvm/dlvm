#!/usr/bin/env python

import logging
from subprocess import Popen, PIPE
from celery import Celery
from dlvm.utils.configure import conf
from dlvm.utils.loginit import loginit

logger = logging.getLogger('dlvm_monitor')

app = Celery('monitor')


def run_cmd(cmd):
    cmd = ' '.join(cmd)
    sub = Popen(cmd, stdout=PIPE, shell=True)
    logger.info('cmd enter: [%s]', cmd)
    out, err = sub.communicate()
    logger.info('cmd exit: [%s] [%s] [%s]', out, err, sub.returncode)
    if sub.returncode != 0:
        raise Exception('cmd failed')


def create_tasks():
    app.conf.update(
        task_acks_late=True,
        broker_url=conf.broker_url,
    )

    d = {}

    @app.task
    def single_leg_failed(dlv_name, leg_id):
        cmd_name = conf.monitor_single_leg_failed
        cmd = [
            cmd_name,
            dlv_name,
            leg_id,
        ]
        run_cmd(cmd)
    d['single_leg_failed'] = single_leg_failed

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
    d['multi_legs_failed'] = multi_legs_failed

    @app.task
    def pool_full(dlv_name):
        cmd_name = conf.monitor_pool_full
        cmd = [
            cmd_name,
            dlv_name,
        ]
        run_cmd(cmd)
    d['pool_full'] = pool_full

    @app.task
    def mj_mirror_failed(mj_name):
        cmd_name = conf.monitor_mj_mirror_failed
        cmd = [
            cmd_name,
            mj_name,
        ]
        run_cmd(cmd)
    d['mj_mirror_failed'] = mj_mirror_failed

    @app.task
    def mj_mirror_complete(mj_name):
        cmd_name = conf.monitor_mj_mirror_complete
        cmd = [
            cmd_name,
            mj_name,
        ]
        run_cmd(cmd)
    d['mj_mirror_complete'] = mj_mirror_complete
    return d


def start():
    loginit()
    create_tasks()
    argv = [
        'worker',
        '--loglevel=INFO',
    ]
    app.worker_main(argv)
