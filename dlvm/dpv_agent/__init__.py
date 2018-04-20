from dlvm.common.configure import cfg
from dlvm.wrapper.rpc_wrapper import DpvServer
from dlvm.core import command as cmd

server = DpvServer()


@server.register
def dpv_get_info():
    vg_name = cfg.get('storage', 'local_vg')
    total_size, free_size = cmd.vg_get_size(vg_name)
    return {
        'total_size': total_size,
        'free_size': free_size,
    }


def start_dpv_agent():
    server.serve_forever()
