#!/usr/bin/env python

from dlvm.monitor.monitor import create_tasks

queue_functions = None


def queue_init():
    global queue_functions
    if queue_functions is None:
        queue_functions = create_tasks()


def report_single_leg(dlv_name, leg_id):
    queue_functions['single_leg_failed'].delay(
        dlv_name, leg_id)


def report_multi_legs(dlv_name, leg0_id, leg1_id):
    queue_functions['multi_legs_failed'].delay(
        dlv_name, leg0_id, leg1_id)


def report_pool(dlv_name):
    queue_functions['pool_full'].delay(dlv_name)


def report_mj_mirror_failed(mj_name):
    queue_functions['mj_mirror_failed'].delay(mj_name)


def report_mj_mirror_complete(mj_name):
    queue_functions['mj_mirror_complete'].delay(mj_name)
