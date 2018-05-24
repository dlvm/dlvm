import os
import time

from dlvm.common.configure import cfg
from dlvm.wrapper.cmd_wrapper import run_cmd


target_prefix = cfg.get('iscsi', 'target_prefix')
initiator_prefix = cfg.get('iscsi', 'initiator_prefix')


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
