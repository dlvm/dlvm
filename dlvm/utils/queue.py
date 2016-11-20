#!/usr/bin/env python

from dlvm.monitor.monitor import single_leg_failed, \
    multi_legs_failed, pool_full


def report_single_leg(dlv_name, leg_id):
    single_leg_failed.delay(dlv_name, leg_id)


def report_multi_legs(dlv_name, leg0_id, leg1_id):
    multi_legs_failed.delay(dlv_name, leg0_id, leg1_id)


def report_pool(dlv_name):
    pool_full.delay(dlv_name)
