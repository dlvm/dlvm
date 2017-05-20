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


class CmdPath(object):

    default_paths = [
        '/bin', '/usr/bin', '/usr/local/bin',
        '/sbin', '/usr/sbin', '/usr/local/sbin',
    ]

    def __init__(self, paths):
        self.paths = []
        for path in paths:
            self.paths.append(path)
        for path in self.default_paths:
            self.paths.append(path)
        self.path_dict = {}

    def get_path(self, name):
        if name in self.path_dict:
            return self.path_dict[name]
        for path in self.paths:
            full_path = os.path.join(path, name)
            if os.path.exists(full_path):
                self.path_dict[name] = full_path
                return full_path
        raise Exception('unknown cmd: %s %s' % (name, self.paths))


class Context(object):

    def __init__(self, conf, logger, cp):
        self.conf = conf
        self.logger = logger
        self.cp = cp


ctx = Context(None, None, None)


def context_init(conf, logger):
    ctx.conf = conf
    ctx.logger = logger
    ctx.cp = CmdPath(conf.cmd_paths)


def run_cmd(cmd, inp=None, accept_error=False):
    if ctx.conf.sudo is True:
        cmd.insert(0, 'sudo')
    cmd = ' '.join(cmd)
    ctx.logger.debug('cmd enter: [%s]', cmd)
    ctx.logger.debug('cmd input: [%s]', inp)
    sub = Popen(cmd, stdin=PIPE, stdout=PIPE, shell=True)
    out, err = sub.communicate(input=inp)
    ctx.logger.debug('cmd exit: [%s] [%s] [%s]', out, err, sub.returncode)
    if not accept_error and sub.returncode != 0:
        raise CmdError(str(cmd))
    return CmdResult(out, err, sub.returncode)


def dm_get_path(name):
    return '/dev/mapper/{name}'.format(name=name)


def dm_create(name, table):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'status',
        name,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        cmd = [
            ctx.cp.get_path('dmsetup'),
            'create',
            name,
        ]
        run_cmd(cmd, inp=table)
    dm_path = dm_get_path(name)
    verify_dev_path(dm_path)
    return dm_path


def dm_remove(name):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'status',
        name,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode == 0:
        cmd = [
            ctx.cp.get_path('dmsetup'),
            'remove',
            name,
        ]
        run_cmd(cmd)


def dm_message(name, message):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'message',
        name,
        '0',
        message,
    ]
    run_cmd(cmd, accept_error=True)


def dm_wait(name, event_number):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'wait',
        name,
        str(event_number),
    ]
    run_cmd(cmd)


def dm_suspend(name):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'suspend',
        name,
    ]
    run_cmd(cmd)


def dm_resume(name):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'resume',
        name,
    ]
    run_cmd(cmd)


def dm_reload(name, table):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'reload',
        name,
    ]
    run_cmd(cmd, inp=table)


def dm_status(name):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'status',
        name,
    ]
    r = run_cmd(cmd)
    return r.out


def dm_info(name):
    cmd = [
        ctx.cp.get_path('dmsetup'),
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
        return dm_create(self.name, table)

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

    def get_type(self):
        status = dm_status(self.name)
        items = status.split()
        return items[2]

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
        info = {}
        lines = param.split('\n')
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
        items = lines[6].split()
        info['major'] = int(items[-2][:-1])
        info['minor'] = int(items[-1])
        items = lines[7].split()
        info['number_of_targets'] = int(items[-1])
        return info

    def wait(self, event_number):
        dm_wait(self.name, event_number)

    def wait_event(self, check, action, args):
        info = self.info()
        event_number = info['event_number']
        ret = check(args)
        if ret is True:
            action(args)
        else:
            self.wait(event_number)
            ret = check(args)
            if ret is True:
                action(args)


class DmLinear(DmBasic):

    def _format_table(self, param):
        line_strs = []
        for line in param:
            line_str = '{start} {length} linear {dev_path} {offset}'.format(
                **line)
            line_strs.append(line_str)
        table = '\n'.join(line_strs)
        return table


class DmStripe(DmBasic):

    def _format_table(self, param):
        header = '{start} {length} striped {num} {chunk_size}'.format(
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
        dev_info = ' '.join(devs)
        table = '{header} {dev_info}'.format(
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

    def _extract_status(self, param):
        status = {}
        items = param.split()
        status['start'] = int(items[0])
        status['length'] = int(items[1])
        status['type'] = items[2]
        status['raid_type'] = items[3]
        status['devices_num'] = int(items[4])
        status['hc0'] = items[5][0]
        status['hc1'] = items[5][1]
        curr, total = map(int, items[6].split('/'))
        status['curr'] = curr
        status['total'] = total
        status['sync_action'] = items[7]
        status['mismatch_cnt'] = items[8]
        return status


class DmPool(DmBasic):

    def _format_table(self, param):
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
        used_meta, total_meta = map(int, items[4].split('/'))
        status['used_meta'] = used_meta
        status['total_meta'] = total_meta
        used_data, total_data = map(int, items[5].split('/'))
        status['used_data'] = used_data
        status['total_data'] = total_data
        return status


class DmThin(DmBasic):

    def _format_table(self, param):
        table = '{start} {length} thin {pool_path} {thin_id}'.format(
            **param)
        return table


class DmError(DmBasic):

    def _format_table(self, param):
        table = '{start} {length} error'.format(**param)
        return table


def dm_get_all(prefix):
    cmd = [
        ctx.cp.get_path('dmsetup'),
        'status',
    ]
    r = run_cmd(cmd)
    dm_devices = r.out.split('\n')
    dm_name_list = []
    for dm_device in dm_devices:
        if dm_device.startswith(prefix) is True:
            end = dm_device.find(':')
            dm_name = dm_device[:end]
            dm_name_list.append(dm_name)
    return dm_name_list


def lv_get_path(lv_name, vg_name):
    return '/dev/{vg_name}/{lv_name}'.format(
        vg_name=vg_name, lv_name=lv_name)


def lv_create(lv_name, lv_size, vg_name):
    lv_path = lv_get_path(lv_name, vg_name)
    cmd = [
        ctx.cp.get_path('lvm'),
        'lvs',
        lv_path,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        cmd = [
            ctx.cp.get_path('lvm'),
            'lvcreate',
            '-n', lv_name,
            '-L', str(lv_size)+'B',
            vg_name,
        ]
        run_cmd(cmd)
    return lv_path


def lv_remove(lv_name, vg_name):
    lv_path = lv_get_path(lv_name, vg_name)
    cmd = [
        ctx.cp.get_path('lvm'),
        'lvs',
        lv_path,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode == 0:
        cmd = [
            ctx.cp.get_path('lvm'),
            'lvremove',
            '-f',
            lv_path,
        ]
        run_cmd(cmd)


def lv_get_all(vg_name):
    vg_selector = 'vg_name={vg_name}'.format(vg_name=vg_name)
    cmd = [
        ctx.cp.get_path('lvm'),
        'lvs',
        '-S', vg_selector,
        '--noheadings',
        '-o', 'lv_name',
    ]
    r = run_cmd(cmd)
    lv_name_list = []
    items = r.out.split('\n')
    for item in items[:-1]:
        lv_name_list.append(item.strip())
    return lv_name_list


def vg_get_size(vg_name):
    cmd = [
        ctx.cp.get_path('lvm'),
        'vgs',
        '-o', 'vg_size,vg_free',
        '--units', 'b',
        '--nosuffix', '--noheadings',
        vg_name,
    ]
    r = run_cmd(cmd)
    sizes = r.out.strip().split(' ')
    total_size = int(sizes[0].strip())
    free_size = int(sizes[1].strip())
    return total_size, free_size


def iscsi_get_context(target_name, iscsi_ip_port):
    cmd = [
        ctx.cp.get_path('iscsiadm'),
        '-m', 'node',
        '-o', 'show',
        '-T', target_name,
        '-I', ctx.conf.initiator_iface,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        cmd = [
            ctx.cp.get_path('iscsiadm'),
            '-m', 'discovery',
            '-t', 'sendtargets',
            '-p', iscsi_ip_port,
            '-I', ctx.conf.initiator_iface,
        ]
        run_cmd(cmd)
        cmd = [
            ctx.cp.get_path('iscsiadm'),
            '-m', 'node',
            '-o', 'show',
            '-T', target_name,
            '-I', ctx.conf.initiator_iface,
        ]
        r = run_cmd(cmd)
    return iscsi_extract_context(r.out)


def iscsi_extract_context(output):
    items = output.split('\n')
    address = None
    port = None
    for item in items:
        if item.startswith('node.conn[0].address'):
            address = item.split()[-1]
        elif item.startswith('node.conn[0].port'):
            port = item.split()[-1]
        if address is not None and port is not None:
            break
    assert(address is not None)
    assert(port is not None)
    context = {}
    context['address'] = address
    context['port'] = port
    return context


def iscsi_get_path(target_name, context):
    iscsi_path = ctx.conf.iscsi_path_fmt.format(
        address=context['address'],
        port=context['port'],
        target_name=target_name,
    )
    return iscsi_path


def iscsi_login(target_name, dpv_name):
    iscsi_ip_port = '{dpv_name}:{iscsi_port}'.format(
        dpv_name=dpv_name, iscsi_port=ctx.conf.iscsi_port)
    context = iscsi_get_context(
        target_name, iscsi_ip_port)
    iscsi_path = iscsi_get_path(target_name, context)
    if os.path.exists(iscsi_path):
        return iscsi_path
    cmd = [
        ctx.cp.get_path('iscsiadm'),
        '-m', 'node',
        '--login',
        '-T', target_name,
        '-p', iscsi_ip_port,
        '-I', ctx.conf.initiator_iface,
    ]
    run_cmd(cmd)
    verify_dev_path(iscsi_path)
    return iscsi_path


def iscsi_logout(target_name):
    # iscsiadm -m node -o show -T target_name
    cmd = [
        ctx.cp.get_path('iscsiadm'),
        '-m', 'node',
        '-o', 'show',
        '-T', target_name,
        '-I', ctx.conf.initiator_iface,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        return
    context = iscsi_extract_context(r.out)
    iscsi_path = iscsi_get_path(target_name, context)
    if os.path.exists(iscsi_path):
        cmd = [
            ctx.cp.get_path('iscsiadm'),
            '-m', 'node',
            '--logout',
            '-T', target_name,
            '-I', ctx.conf.initiator_iface,
        ]
        run_cmd(cmd)
    # iscsiadm -m node -T target_name -o delete
    cmd = [
        ctx.cp.get_path('iscsiadm'),
        '-m', 'node',
        '-T', target_name,
        '-o', 'delete',
        '-I', ctx.conf.initiator_iface,
    ]
    run_cmd(cmd)


def iscsi_create(target_name, dev_name, dev_path):
    backstore_path = '/backstores/iblock/{dev_name}'.format(
        dev_name=dev_name)
    cmd = [
        ctx.cp.get_path('targetcli'),
        backstore_path,
        'ls',
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        dev = 'dev={dev_path}'.format(
            dev_path=dev_path)
        name = 'name={dev_name}'.format(
            dev_name=dev_name)
        cmd = [
            ctx.cp.get_path('targetcli'),
            '/backstores/iblock',
            'create',
            dev,
            name,
        ]
        run_cmd(cmd)

    target_path = '/iscsi/{target_name}'.format(
        target_name=target_name)
    cmd = [
        ctx.cp.get_path('targetcli'),
        target_path,
        'ls',
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        cmd = [
            ctx.cp.get_path('targetcli'),
            '/iscsi',
            'create',
            target_name,
        ]
        run_cmd(cmd)

    lun0 = '{target_path}/tpg1/luns/lun0'.format(
        target_path=target_path)
    cmd = [
        ctx.cp.get_path('targetcli'),
        lun0,
        'ls',
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        lun_path = '{target_path}/tpg1/luns'.format(
            target_path=target_path)
        cmd = [
            ctx.cp.get_path('targetcli'),
            lun_path,
            'create',
            backstore_path,
        ]
        run_cmd(cmd)

    portal_path = '/iscsi/{target_name}/tpg1/portals'.format(
        target_name=target_name,
    )
    export_portal_path = '{portal_path}/0.0.0.0:{iscsi_port}'.format(
        portal_path=portal_path,
        iscsi_port=ctx.conf.iscsi_port)
    cmd = [
        ctx.cp.get_path('targetcli'),
        export_portal_path,
        'ls',
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        ip_port = 'ip_port={iscsi_port}'.format(
            iscsi_port=ctx.conf.iscsi_port)
        cmd = [
            ctx.cp.get_path('targetcli'),
            portal_path,
            'create',
            'ip_address=0.0.0.0',
            ip_port,
        ]
        run_cmd(cmd)


def iscsi_delete(target_name, dev_name):
    target_path = '/iscsi/{target_name}'.format(
        target_name=target_name)
    cmd = [
        ctx.cp.get_path('targetcli'),
        target_path,
        'ls',
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode == 0:
        cmd = [
            ctx.cp.get_path('targetcli'),
            '/iscsi',
            'delete',
            target_name,
        ]
        run_cmd(cmd)

    backstore_path = '/backstores/iblock/{dev_name}'.format(
        dev_name=dev_name)
    cmd = [
        ctx.cp.get_path('targetcli'),
        backstore_path,
        'ls',
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode == 0:
        cmd = [
            ctx.cp.get_path('targetcli'),
            '/backstores/iblock',
            'delete',
            dev_name,
        ]
        run_cmd(cmd)


def iscsi_export(target_name, initiator_name):
    acl_path = '/iscsi/{target_name}/tpg1/acls'.format(
        target_name=target_name,
    )
    initiator_path = '{acl_path}/{initiator_name}'.format(
        acl_path=acl_path,
        initiator_name=initiator_name,
    )
    cmd = [
        ctx.cp.get_path('targetcli'),
        initiator_path,
        'ls',
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        cmd = [
            ctx.cp.get_path('targetcli'),
            acl_path,
            'create',
            initiator_name,
        ]
        run_cmd(cmd)

    assign_userid = 'userid={iscsi_userid}'.format(
        iscsi_userid=ctx.conf.iscsi_userid)
    cmd = [
        ctx.cp.get_path('targetcli'),
        initiator_path,
        'set',
        'auth',
        assign_userid,
    ]
    run_cmd(cmd)

    assign_password = 'password={iscsi_password}'.format(
        iscsi_password=ctx.conf.iscsi_password)
    cmd = [
        ctx.cp.get_path('targetcli'),
        initiator_path,
        'set',
        'auth',
        assign_password,
    ]
    run_cmd(cmd)


def iscsi_unexport(target_name, initiator_name):
    acl_path = '/iscsi/{target_name}/tpg1/acls'.format(
        target_name=target_name,
    )
    initiator_path = '{acl_path}/{initiator_name}'.format(
        acl_path=acl_path,
        initiator_name=initiator_name,
    )
    cmd = [
        ctx.cp.get_path('targetcli'),
        initiator_path,
        'ls',
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode == 0:
        cmd = [
            ctx.cp.get_path('targetcli'),
            acl_path,
            'delete',
            initiator_name,
        ]
        run_cmd(cmd)


def iscsi_login_get_all(prefix):
    cmd = [
        ctx.cp.get_path('iscsiadm'),
        '-m',
        'node',
        '-I', ctx.conf.initiator_iface,
    ]
    r = run_cmd(cmd, accept_error=True)
    if r.rcode != 0:
        return []
    lines = r.out.split('\n')
    target_name_list = []
    for line in lines[:-1]:
        target_name = line.split(' ')[1]
        if target_name.startswith(prefix):
            target_name_list.append(target_name)
    return target_name_list


def iscsi_target_get_all(prefix):
    cmd = [
        ctx.cp.get_path('targetcli'),
        '/iscsi/',
        'ls',
        'depth=1',
    ]
    r = run_cmd(cmd)
    raw_list = r.out.split('\n')[1:-1]
    target_list = []
    for item in raw_list:
        start = item.find(prefix)
        if start == -1:
            continue
        stop = item.find(' ...')
        target_name = item[start:stop]
        target_list.append(target_name)
    return target_list


def iscsi_target_delete(target_name):
    cmd = [
        ctx.cp.get_path('targetcli'),
        '/iscsi',
        'delete',
        target_name,
    ]
    run_cmd(cmd)


def iscsi_backstore_get_all(prefix):
    cmd = [
        ctx.cp.get_path('targetcli'),
        '/backstores/iblock',
        'ls',
        'depth=1',
    ]
    r = run_cmd(cmd)
    raw_list = r.out.split('\n')[1:-1]
    dev_list = []
    for item in raw_list:
        if item.find(prefix) == -1:
            continue
        p = ' o- '
        start = item.find(p) + len(p)
        stop = item.find(' ...')
        dev_name = item[start:stop]
        dev_list.append(dev_name)
    return dev_list


def iscsi_backstore_delete(dev_name):
    cmd = [
        ctx.cp.get_path('targetcli'),
        '/backstores/iblock',
        'delete',
        dev_name,
    ]
    run_cmd(cmd)


def run_dd(in_path, out_path, bs=None, count=None, seek=None, skip=None):
    inp = 'if={in_path}'.format(in_path=in_path)
    outp = 'of={out_path}'.format(out_path=out_path)
    cmd = [
        ctx.cp.get_path('dd'),
        inp,
        outp,
    ]
    if bs is not None:
        bs_param = 'bs={bs}'.format(bs=bs)
        cmd.append(bs_param)
    if count is not None:
        count_param = 'count={count}'.format(count=count)
        cmd.append(count_param)
    if seek is not None:
        seek_param = 'seek={seek}'.format(seek=seek)
        cmd.append(seek_param)
    if skip is not None:
        skip_param = 'skip={skip}'.format(skip=skip)
        cmd.append(skip_param)
    run_cmd(cmd)
