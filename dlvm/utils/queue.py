#!/usr/bin/env python

from dlvm.monitor import create_tasks

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


def report_fj_mirror_failed(fj_name):
    queue_functions['fj_mirror_failed'].delay(fj_name)


def report_fj_mirror_complete(fj_name):
    queue_functions['fj_mirror_complete'].delay(fj_name)
