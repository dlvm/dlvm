from dlvm.common.configure import cfg
from dlvm.dpv_agent import DmContextSchema


thin_block_size = cfg.getsize('device_mapper', 'thin_block_size')
mirror_meta_blocks = cfg.getint('device_mapper', 'mirror_meta_blocks')
mirror_region_blocks = cfg.getint('device_mapper', 'mirror_region_blocks')
mirror_region_size = thin_block_size * mirror_region_blocks
stripe_chunk_size = cfg.getsize('device_mapper', 'stripe_chunk_size')
low_water_mark = cfg.getint('device_mapper', 'low_water_mark')


def get_dm_ctx():
    return DmContextSchema.nt(
        thin_block_size,
        mirror_meta_blocks,
        mirror_region_size,
        stripe_chunk_size,
        low_water_mark)
