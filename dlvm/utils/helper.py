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
