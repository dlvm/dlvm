from marshmallow import fields
from dlvm.common.configure import cfg
from dlvm.common.marshmallow_ext import NtSchema
from dlvm.wrapper.rpc_wrapper import DpvRpc
from dlvm.common import command as cmd


dpv_rpc = DpvRpc()


class DpvGetInfoRetSchema(NtSchema):
    total_size = fields.Integer()
    free_size = fields.Integer()


@dpv_rpc.register(ret_schema=DpvGetInfoRetSchema)
def dpv_get_info():
    vg_name = cfg.get('device_mapper', 'local_vg')
    total_size, free_size = cmd.vg_get_size(vg_name)
    return DpvGetInfoRetSchema.nt(total_size, free_size)


class DmContextSchema(NtSchema):
    thin_block_size = fields.Integer()
    mirror_meta_blocks = fields.Integer()
    mirror_region_size = fields.Integer()
    stripe_chunk_size = fields.Integer()
    low_water_mark = fields.Integer()


class LegCreateArgSchema(NtSchema):
    leg_id = fields.Integer()
    leg_size = fields.Integer()
    dm_context = fields.Nested(DmContextSchema, many=False)


@dpv_rpc.register(arg_schema=LegCreateArgSchema)
def leg_create(arg):
    pass


def start_dpv_agent():
    dpv_rpc.start_server()
