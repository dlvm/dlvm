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
    if r.rcode == 0:
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
    items = r.out.split('\n')
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
    sizes = r.out.strip().split(' ')
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
