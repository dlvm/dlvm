#!/usr/bin/env python

import struct
import array


dm_raid_fields = [
    ('__le32', 'magic'),
    ('__le32', 'features'),
    ('__le32', 'num_devices'),
    ('__le32', 'array_positions'),
    ('__le64', 'events'),
    ('__le64', 'failed_devices'),
    ('__le64', 'disk_recovery_offset'),
    ('__le64', 'array_resync_offset'),
    ('__le32', 'level'),
    ('__le32', 'layout'),
    ('__le32', 'stripe_sectors'),
]
dm_raid_offset = 0

raid1_fields = [
    ('__le32', 'magic'),
    ('__le32', 'version'),
    ('__u8', 'uuid', 16),
    ('__le64', 'event'),
    ('__le64', 'event_cleared'),
    ('__le64', 'sync_size'),
    ('__le32', 'state'),
    ('__le32', 'chunksize'),
    ('__le32', 'daemon_sleep'),
    ('__le32', 'write_behand'),
    ('__le32', 'sectors_reserved'),
]
raid1_offset = 0x1000

field_mapping = {
    '__le32': 'I',
    '__le64': 'Q',
    '__u8': 'B',
}

bm_offset = 0x1100


def write_struct(offset, fields, f, **kw):
    fmt = '<'
    items = []
    for field in fields:
        if len(field) == 2:
            fmt = '%s%s' % (fmt, field_mapping[field[0]])
            items.append(kw[field[1]])
        else:
            assert(len(field) == 3)
            for i in xrange(field[2]):
                fmt = '%s%s' % (fmt, field_mapping[field[0]])
                items.append(kw[field[1]][i])
    s = struct.Struct(fmt)
    raw = s.pack(*items)
    f.seek(offset)
    f.write(raw)


def generate_mirror_meta(
        file_path, meta_size, data_size, chunk_size, bm):
    data_sectors = data_size / 512
    a = array.array('B', [0 for i in xrange(meta_size)])
    dm_raid_sb = {
        'magic': 0x64526d44,
        'features': 0,
        'num_devices': 2,
        'array_positions': 0,
        'events': 2,
        'failed_devices': 0,
        'disk_recovery_offset': 0xffffffffffffffff,
        'array_resync_offset': 0,
        'level': 1,
        'layout': 0,
        'stripe_sectors': 0,
    }

    raid1_sb = {
        'magic': 0x6d746962,
        'version': 4,
        'uuid': [0]*16,
        'event': 02,
        'event_cleared': 1,
        'sync_size': data_sectors,
        'state': 0,
        'chunksize': chunk_size,
        'daemon_sleep': 5,
        'write_behand': 0,
        'sectors_reserved': 0,
    }

    with open(file_path, 'wb') as f:
        a.tofile(f)
        write_struct(dm_raid_offset, dm_raid_fields, f, **dm_raid_sb)
        write_struct(raid1_offset, raid1_fields, f, **raid1_sb)
        f.seek(bm_offset)
        bm.bitmap.tofile(f)
