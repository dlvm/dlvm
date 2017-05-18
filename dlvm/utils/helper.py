#!/usr/bin/env python

from configure import conf


def chunks(array, n):
    """Yield successive n-sized chunks from array."""
    for i in range(0, len(array), n):
        yield array[i:i+n]


def encode_target_name(leg_id):
    return '{target_prefix}.{leg_id}'.format(
        target_prefix=conf.target_prefix,
        leg_id=leg_id,
    )


def encode_initiator_name(host_name):
    return '{initiator_prefix}.{host_name}'.format(
        initiator_prefix=conf.initiator_prefix,
        host_name=host_name,
    )


def group_encode(group):
    if 'group_size' in group:
        group['group_size'] = str(group['group_size'])
        if 'legs' in group:
            for leg in group['legs']:
                if 'leg_size' in leg:
                    leg['leg_size'] = str(leg['leg_size'])


def group_decode(group):
    if 'group_size' in group:
        group['group_size'] = int(group['group_size'])
        if 'legs' in group:
            for leg in group['legs']:
                if 'leg_size' in leg:
                    leg['leg_size'] = int(leg['leg_size'])


def dlv_info_encode(dlv_info):
    if 'dlv_size' in dlv_info:
        dlv_info['dlv_size'] = str(dlv_info['dlv_size'])
    if 'data_size' in dlv_info:
        dlv_info['data_size'] = str(dlv_info['data_size'])
    if 'groups' in dlv_info:
        for group in dlv_info['groups']:
            group_encode(group)


def dlv_info_decode(dlv_info):
    if 'dlv_size' in dlv_info:
        dlv_info['dlv_size'] = int(dlv_info['dlv_size'])
    if 'data_size' in dlv_info:
        dlv_info['data_size'] = int(dlv_info['data_size'])
    if 'groups' in dlv_info:
        for group in dlv_info['groups']:
            group_decode(group)
