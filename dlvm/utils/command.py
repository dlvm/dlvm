#!/usr/bin/env python

import os
import time
from subprocess import Popen, PIPE


class DevAbsentError(Exception):
    pass


def verify_dev_path(dev_path):
    retry = 3
    while retry > 0:
        if os.path.exists(dev_path):
            return
        time.sleep(0.1)
        retry -= 1
    raise DevAbsentError(dev_path)


class CmdError(Exception):
    pass


class CmdResult(object):

    def __init__(self, out, err, rcode):
        self.out = out
        self.err = err
        self.rcode = rcode


class Context(object):

    def __init__(self, conf, logger):
        self.conf = conf
        self.logger = logger


ctx = Context(None, None)


def context_init(conf, logger):
    ctx.conf = conf
    ctx.logger = logger


def run_cmd(cmd, inp=None, accept_error=False):
    if ctx.conf.sudo is True:
        cmd.insert(0, 'sudo')
    cmd = ' '.join(cmd)
    ctx.logger.debug('cmd enter: [%s]', cmd)
    ctx.logger.debug('cmd input: [%s]', inp)
    sub = Popen(cmd, stdout=PIPE, shell=True)
    out, err = sub.communicate(input=inp)
    ctx.logger.debug('cmd exit: [%s] [%s] [%s]', out, err, sub.returncode)
    if not accept_error and sub.returncode != 0:
        raise CmdError(str(cmd))
    return CmdResult(out, err, sub.returncode)


def dm_get_path(name):
    return '/dev/mapper/{name}'.format(name=name)


def dm_create(name, table):
    cmd = [
        ctx.conf.dmsetup_path,
        'status',
        name,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        cmd = [
            ctx.conf.dmsetup_path,
            'create',
            name,
        ]
        run_cmd(cmd, inp=table)
    dm_path = dm_get_path(name)
    verify_dev_path(dm_path)
    return dm_path


def dm_remove(name):
    cmd = [
        ctx.conf.dmsetup_path,
        'status',
        name,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode == 0:
        cmd = [
            ctx.conf.dmsetup_path,
            'remove',
            name,
        ]
        run_cmd(cmd)


def dm_message(name, message):
    cmd = [
        ctx.conf.dmsetup_path,
        'message',
        name,
        '0',
        message,
    ]
    run_cmd(cmd, accept_error=True)


def dm_wait(name, event_number):
    cmd = [
        ctx.conf.dmsetup_path,
        'wait',
        name,
        event_number,
    ]
    run_cmd(cmd)


def dm_suspend(name):
    cmd = [
        ctx.conf.dmsetup_path,
        'suspend',
        name,
    ]
    run_cmd(cmd)


def dm_resume(name):
    cmd = [
        ctx.conf.dmsetup_path,
        'resume',
        name,
    ]
    run_cmd(cmd)


def dm_reload(name, table):
    cmd = [
        ctx.conf.dmsetup_path,
        'reload',
        name,
        '--table',
        table,
    ]
    run_cmd(cmd)


def dm_status(name):
    cmd = [
        ctx.conf.dmsetup_path,
        'status',
        name,
    ]
    r = run_cmd(cmd)
    return r.out


def dm_info(name):
    cmd = [
        ctx.conf.dmsetup_path,
        'info',
        name,
    ]
    r = run_cmd(cmd)
    return r.out


class DmBasic(object):

    def __init__(self, name):
        self.name = name

    def create(self, param):
        table = self._format_table(param)
        dm_create(self.name, table)

    def reload(self, param):
        table = self._format_table(param)
        self.suspend()
        try:
            dm_reload(self.name, table)
        finally:
            self.resume()

    def message(self, param):
        message = self._format_message(param)
        dm_message(self.name, message)

    def remove(self):
        dm_remove(self.name)

    def suspend(self):
        dm_suspend(self.name)

    def get_path(self):
        return dm_get_path(self.name)

    def resume(self):
        dm_resume(self.name)

    def status(self):
        status = dm_status(self.name)
        return self._extract_status(status)

    def info(self):
        info = dm_info(self.name)
        return self._extract_info(info)

    def _format_table(self, param):
        raise Exception('not implement')

    def _format_message(self, param):
        raise Exception('not implement')

    def _extract_status(self, param):
        raise Exception('not implement')

    def _extract_info(self, param):
        raise Exception('not implement')


class DmLinear(DmBasic):

    def _format_table(self, param):
        line_strs = []
        for line in param:
            line_str = '{start} {length} linear {dev_path} {offset}'.format(
                **line)
            line_strs.append(line_str)
        table = line_strs.join('\n')
        return table


class DmStripe(DmBasic):

    def _format_table(self, param):
        header = '{start} {length} {num} {chunk_size}'.format(
            start=param['start'],
            length=param['length'],
            num=param['num'],
            chunk_size=param['chunk_size'],
        )
        devs = []
        for device in param['devices']:
            dev = '{dev_path} {offset}'.format(
                dev_path=device['dev_path'],
                offset=device['offset'],
            )
            devs.append(dev)
        dev_info = devs.join(' ')
        table = '{header} {dev_info}'.foramt(
            header=header, dev_info=dev_info)
        return table


class DmMirror(DmBasic):

    def _format_table(self, param):
        table = (
            '{start} {offset} raid raid1 '
            '3 0 region_size {region_size} '
            '2 {meta0} {data0} {meta1} {data1}'
        ).format(**param)
        return table


class DmPool(DmBasic):

    def _format_talbe(self, param):
        table = (
            '{start} {length} thin-pool '
            '{meta_path} {data_path} '
            '{block_sectors} {low_water_mark}'
        ).format(**param)
        return table

    def _format_message(self, param):
        if param['action'] == 'thin':
            message = 'create_thin {thin_id}'.format(
                thin_id=param['thin_id'])
        elif param['action'] == 'snap':
            message = 'create_snap {thin_id} {ori_thin_id}'.format(
                thin_id=param['thin_id'],
                ori_thin_id=param['ori_thin_id'],
            )
        elif param['action'] == 'delete':
            message = 'delete {thin_id}'.format(
                thin_id=param['thin_id'])
        else:
            assert(False)
        return message

    def _extract_status(self, status_str):
        status = {}
        items = status_str.split()
        status['start'] = int(items[0])
        status['length'] = int(items[1])
        status['type'] = items[2]
        status['transaction_id'] = items[3]
        used_meta, total_meta = map(int, items[4].split())
        status['used_meta'] = used_meta
        status['total_meta'] = total_meta
        used_data, total_data = map(int, items[5].split())
        status['used_data'] = used_data
        status['total_data'] = total_data
        return status

    def _extract_info(self, info_str):
        info = {}
        lines = info_str.split('\n')
        items = lines[0].split()
        info['name'] = items[-1]
        items = lines[1].split()
        info['status'] = items[-1]
        items = lines[2].split()
        info['read_ahead'] = int(items[-1])
        items = lines[3].split()
        info['tables_present'] = items[-1]
        items = lines[4].split()
        info['open_count'] = int(items[-1])
        items = lines[5].split()
        info['event_number'] = int(items[-1])
        itmes = lines[6].split()
        info['major'] = int(itmes[-2])
        info['minor'] = int(itmes[-1])
        items = lines[7].split()
        info['number_of_targets'] = int(items[-1])
        return info


class DmThin(DmBasic):

    def _format_table(self, param):
        table = '{start} {length} thin {pool_path} {thin_id}'.format(
            **param)
        return table


class DmError(DmBasic):

    def _format_talbe(self, param):
        table = '{start} {length} error'.format(**param)
        return table
