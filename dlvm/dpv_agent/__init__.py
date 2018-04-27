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
    vg_name = cfg.get('storage', 'local_vg')
    total_size, free_size = cmd.vg_get_size(vg_name)
    return DpvGetInfoRetSchema.nt(total_size, free_size)


def start_dpv_agent():
    dpv_rpc.start_server()
