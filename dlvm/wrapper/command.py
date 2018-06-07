import os
import time
from threading import Lock

from dlvm.common.configure import cfg
from dlvm.wrapper.cmd_wrapper import run_cmd


dpv_prefix = cfg.get('device_mapper', 'dpv_prefix')
target_prefix = cfg.get('iscsi', 'target_prefix')
initiator_prefix = cfg.get('iscsi', 'initiator_prefix')
initiator_iface = cfg.get('iscsi', 'iface')
iscsi_path_fmt = cfg.get('iscsi', 'path_fmt')
iscsi_userid = cfg.get('iscsi', 'iscsi_userid')
iscsi_password = cfg.get('iscsi', 'iscsi_password')
iscsi_port = cfg.getint('iscsi', 'iscsi_port')


cmd_lock = Lock()


def exclude(func):

    def wrapper(*args, **kwargs):
        with cmd_lock:
            return func(*args, **kwargs)

    return wrapper


def lv_get_path(lv_name, vg_name):
    return '/dev/{vg_name}/{lv_name}'.format(
        vg_name=vg_name, lv_name=lv_name)


def lv_create(lv_name, lv_size, vg_name):
    lv_path = lv_get_path(lv_name, vg_name)
    cmd = [
        'lvm',
        'lvs',
        lv_path,
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        cmd = [
            'lvm',
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
        'lvm',
        'lvs',
        lv_path,
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode == 0:
        cmd = [
            'lvm',
            'lvremove',
            '-f',
            lv_path,
        ]
        run_cmd(cmd)


def lv_get_all(vg_name):
    vg_selector = 'vg_name={vg_name}'.format(vg_name=vg_name)
    cmd = [
        'lvm',
        'lvs',
        '-S', vg_selector,
        '--noheadings',
        '-o', 'lv_name',
    ]
    r = run_cmd(cmd)
    lv_name_list = []
    items = r.stdout.decode('utf-8').split('\n')
    for item in items[:-1]:
        lv_name_list.append(item.strip())
    return lv_name_list


def vg_get_size(vg_name):
    cmd = [
        'lvm',
        'vgs',
        '-o', 'vg_size,vg_free',
        '--units', 'b',
        '--nosuffix', '--noheadings',
        vg_name,
    ]
    r = run_cmd(cmd)
    sizes = r.stdout.decode('utf-8').strip().split(' ')
    total_size = int(sizes[0].strip())
    free_size = int(sizes[1].strip())
    return total_size, free_size


def encode_target_name(leg_id):
    return '{target_prefix}.{leg_id}'.format(
        target_prefix=target_prefix,
        leg_id=leg_id,
    )


def encode_initiator_name(host_name):
    return '{initiator_prefix}.{host_name}'.format(
        initiator_prefix=initiator_prefix,
        host_name=host_name,
    )


def dm_dd(src, dst, bs, count, skip=0, seek=0):
    cmd = [
        'dm_dd',
        src,
        dst,
        str(bs),
        str(count),
        str(skip),
        str(seek),
    ]
    run_cmd(cmd)


def verify_dev_path(dev_path):
    retry = 3
    while retry > 0:
        if os.path.exists(dev_path):
            return
        time.sleep(0.1)
        retry -= 1
    raise Exception('dev_path not exist: %s' % dev_path)


def dm_get_path(name):
    return '/dev/mapper/{name}'.format(name=name)


def dm_create(name, table):
    cmd = [
        'dmsetup',
        'status',
        name,
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        cmd = [
            'dmsetup',
            'create',
            name,
        ]
        run_cmd(cmd, inp=table)
    dm_path = dm_get_path(name)
    verify_dev_path(dm_path)
    return dm_path


def dm_remove(name):
    cmd = [
        'dmsetup',
        'status',
        name,
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode == 0:
        cmd = [
            'dmsetup',
            'remove',
            name,
        ]
        run_cmd(cmd)


def dm_message(name, message):
    cmd = [
        'dmsetup',
        'message',
        name,
        '0',
        message,
    ]
    run_cmd(cmd, check=False)


def dm_wait(name, event_number):
    cmd = [
        'dmsetup',
        'wait',
        name,
        str(event_number),
    ]
    run_cmd(cmd)


def dm_suspend(name):
    cmd = [
        'dmsetup',
        'suspend',
        name,
    ]
    run_cmd(cmd)


def dm_resume(name):
    cmd = [
        'dmsetup',
        'resume',
        name,
    ]
    run_cmd(cmd)


def dm_reload(name, table):
    cmd = [
        'dmsetup',
        'reload',
        name,
    ]
    run_cmd(cmd, inp=table)


def dm_status(name):
    cmd = [
        'dmsetup',
        'status',
        name,
    ]
    r = run_cmd(cmd)
    return r.stdout.decode('utf-8')


def dm_info(name):
    cmd = [
        'dmsetup',
        'info',
        name,
    ]
    r = run_cmd(cmd)
    return r.stdout.decode('utf-8')


class DmBasic():

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
        table_str = '{start} {length} thin {pool_path} {thin_id}'
        if 'ori_path' in param:
            table_str += ' {ori_path}'
        table = table_str.format(**param)
        return table


class DmError(DmBasic):

    def _format_table(self, param):
        table = '{start} {length} error'.format(**param)
        return table


def dm_get_all():
    cmd = [
        'dmsetup',
        'status',
    ]
    r = run_cmd(cmd)
    lines = r.stdout.decode('utf-8').split('\n')
    dm_name_list = []
    for line in lines:
        if line.startswith(dpv_prefix):
            end = line.find(':')
            name = line[:end]
            dm_name_list.append(name)
    return dm_name_list


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


def iscsi_get_context(target_name, iscsi_ip_port):
    cmd = [
        'iscsiadm',
        '-m', 'node',
        '-o', 'show',
        '-T', target_name,
        '-I', initiator_iface,
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        cmd = [
            'iscsiadm',
            '-m', 'discovery',
            '-t', 'sendtargets',
            '-p', iscsi_ip_port,
            '-I', initiator_iface,
        ]
        run_cmd(cmd)
        cmd = [
            'iscsiadm',
            '-m', 'node',
            '-o', 'show',
            '-T', target_name,
            '-I', initiator_iface,
        ]
        r = run_cmd(cmd)
    return iscsi_extract_context(r.stdout.decode('utf-8'))


def iscsi_get_path(target_name, context):
    iscsi_path = iscsi_path_fmt.format(
        address=context['address'],
        port=context['port'],
        target_name=target_name,
    )
    return iscsi_path


def iscsi_login(target_name, dpv_name):
    iscsi_ip_port = '{dpv_name}:{iscsi_port}'.format(
        dpv_name=dpv_name, iscsi_port=iscsi_port)
    context = iscsi_get_context(
        target_name, iscsi_ip_port)
    iscsi_path = iscsi_get_path(target_name, context)
    if os.path.exists(iscsi_path):
        return iscsi_path
    cmd = [
        'iscsiadm',
        '-m', 'node',
        '--login',
        '-T', target_name,
        '-p', iscsi_ip_port,
        '-I', initiator_iface,
    ]
    run_cmd(cmd)
    verify_dev_path(iscsi_path)
    return iscsi_path


def iscsi_logout(target_name):
    # iscsiadm -m node -o show -T target_name
    cmd = [
        'iscsiadm',
        '-m', 'node',
        '-o', 'show',
        '-T', target_name,
        '-I', initiator_iface,
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        return
    context = iscsi_extract_context(r.stdout.decode('utf-8'))
    iscsi_path = iscsi_get_path(target_name, context)
    if os.path.exists(iscsi_path):
        cmd = [
            'iscsiadm',
            '-m', 'node',
            '--logout',
            '-T', target_name,
            '-I', initiator_iface,
        ]
        run_cmd(cmd)
    # iscsiadm -m node -T target_name -o delete
    cmd = [
        'iscsiadm',
        '-m', 'node',
        '-T', target_name,
        '-o', 'delete',
        '-I', initiator_iface,
    ]
    run_cmd(cmd)


@exclude
def iscsi_create(target_name, dev_name, dev_path):
    backstore_path = '/backstores/iblock/{dev_name}'.format(
        dev_name=dev_name)
    cmd = [
        'targetcli',
        backstore_path,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        dev = 'dev={dev_path}'.format(
            dev_path=dev_path)
        name = 'name={dev_name}'.format(
            dev_name=dev_name)
        cmd = [
            'targetcli',
            '/backstores/iblock',
            'create',
            dev,
            name,
        ]
        run_cmd(cmd)

    target_path = '/iscsi/{target_name}'.format(
        target_name=target_name)
    cmd = [
        'targetcli',
        target_path,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        cmd = [
            'targetcli',
            '/iscsi',
            'create',
            target_name,
        ]
        run_cmd(cmd)

    lun0 = '{target_path}/tpg1/luns/lun0'.format(
        target_path=target_path)
    cmd = [
        'targetcli',
        lun0,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        lun_path = '{target_path}/tpg1/luns'.format(
            target_path=target_path)
        cmd = [
            'targetcli',
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
        iscsi_port=iscsi_port)
    cmd = [
        'targetcli',
        export_portal_path,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        ip_port = 'ip_port={iscsi_port}'.format(
            iscsi_port=iscsi_port)
        cmd = [
            'targetcli',
            portal_path,
            'create',
            'ip_address=0.0.0.0',
            ip_port,
        ]
        run_cmd(cmd)


@exclude
def iscsi_delete(target_name, dev_name):
    target_path = '/iscsi/{target_name}'.format(
        target_name=target_name)
    cmd = [
        'targetcli',
        target_path,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode == 0:
        cmd = [
            'targetcli',
            '/iscsi',
            'delete',
            target_name,
        ]
        run_cmd(cmd)

    backstore_path = '/backstores/iblock/{dev_name}'.format(
        dev_name=dev_name)
    cmd = [
        'targetcli',
        backstore_path,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode == 0:
        cmd = [
            'targetcli',
            '/backstores/iblock',
            'delete',
            dev_name,
        ]
        run_cmd(cmd)


@exclude
def iscsi_export(target_name, initiator_name):
    acl_path = '/iscsi/{target_name}/tpg1/acls'.format(
        target_name=target_name,
    )
    initiator_path = '{acl_path}/{initiator_name}'.format(
        acl_path=acl_path,
        initiator_name=initiator_name,
    )
    cmd = [
        'targetcli',
        initiator_path,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode != 0:
        cmd = [
            'targetcli',
            acl_path,
            'create',
            initiator_name,
        ]
        run_cmd(cmd)

    assign_userid = 'userid={iscsi_userid}'.format(
        iscsi_userid=iscsi_userid)
    cmd = [
        'targetcli',
        initiator_path,
        'set',
        'auth',
        assign_userid,
    ]
    run_cmd(cmd)

    assign_password = 'password={iscsi_password}'.format(
        iscsi_password=iscsi_password)
    cmd = [
        'targetcli',
        initiator_path,
        'set',
        'auth',
        assign_password,
    ]
    run_cmd(cmd)


@exclude
def iscsi_unexport(target_name, initiator_name):
    acl_path = '/iscsi/{target_name}/tpg1/acls'.format(
        target_name=target_name,
    )
    initiator_path = '{acl_path}/{initiator_name}'.format(
        acl_path=acl_path,
        initiator_name=initiator_name,
    )
    cmd = [
        'targetcli',
        initiator_path,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode == 0:
        cmd = [
            'targetcli',
            acl_path,
            'delete',
            initiator_name,
        ]
        run_cmd(cmd)


@exclude
def iscsi_target_get_all():
    cmd = [
        'targetcli',
        '/iscsi/'
        'ls',
        'depth=1',
    ]
    r = run_cmd(cmd)
    raw_list = r.stdout.decode('utf-8').split('\n')[1:-1]
    target_list = []
    for item in raw_list:
        start = item.find(target_prefix)
        if start == -1:
            continue
        stop = item.find(' ...')
        target_name = item[start:stop]
        # FIXME use real initiator list
        target_list.append(target_name, [])


@exclude
def iscsi_target_release(target_name):
    target_path = '/iscsi/{target_name}'.format(
        target_name=target_name)
    cmd = [
        'targetcli',
        target_path,
        'ls',
    ]
    r = run_cmd(cmd, check=False)
    if r.returncode == 0:
        cmd = [
            'targetcli',
            '/iscsi',
            'delete',
            target_name,
        ]
        run_cmd(cmd)


@exclude
def iblock_release():
    cmd = [
        'targetcli',
        'ls',
        '/backstores/iblock',
        'depth=1',
    ]
    r = run_cmd(cmd)
    lines = r.stdout.decode('utf-8').split('\n')[1:-1]
    for line in lines:
        start = line.find('-') + 1
        stop = line.find('.')
        dev_name = line[start:stop].strip()
        if dev_name.startswith(dpv_prefix):
            if 'not in use' in line:
                cmd = [
                    'targetcli',
                    '/backstores/iblock',
                    'delete',
                    dev_name,
                ]
                run_cmd(cmd)
