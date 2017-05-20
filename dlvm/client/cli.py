#!/usr/bin/env python

import argparse
import logging
import json
import yaml
from layer2 import Layer2


logger = logging.getLogger('dlvm_client')
LOG_MAPPING = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
}


def convert_to_byte(inp):
    assert(len(inp) > 1)
    val = int(inp[:-1])
    if inp[-1] == 'G' or inp[-1] == 'g':
        val *= (1024 * 1024 * 1024)
    elif inp[-1] == 'M' or inp[-1] == 'm':
        val *= (1024 * 1024)
    elif inp[-1] == 'K' or inp[-1] == 'k':
        val *= 1024
    else:
        assert(True)
    return val


default_conf = {
    'api_server_list': ['localhost:9521'],
}


def get_conf(inp):
    with open(inp) as f:
        conf = yaml.safe_load(f)
        return conf


CLI_CMDS = {
    'dpv': {
        'help': 'manage dpv',
        'cmds': {
            'list': {
                'help': 'list dpvs',
            },
            'create': {
                'help': 'create dpv',
                'arguments': {
                    'dpv_name': {
                        'help': 'dpv hostname',
                    },
                },
            },
            'delete': {
                'help': 'delete dpv',
                'arguments': {
                    'dpv_name': {
                        'help': 'dpv hostname',
                    },
                },
            },
            'display': {
                'help': 'show dpv',
                'arguments': {
                    'dpv_name': {
                        'help': 'dpv hostname',
                    },
                },
            },
            'available': {
                'help': 'set dpv to available status',
                'arguments': {
                    'dpv_name': {
                        'help': 'dpv hostname',
                    },
                },
            },
            'unavailable': {
                'help': 'set dpv to unavailable status',
                'arguments': {
                    'dpv_name': {
                        'help': 'dpv hostname',
                    }
                },
            },
        },
    },
    'dvg': {
        'help': 'manage dvg',
        'cmds': {
            'list': {
                'help': 'list dvgs',
            },
            'create': {
                'help': 'create dvg',
                'arguments': {
                    'dvg_name': {
                        'help': 'dvg name',
                    },
                },
            },
            'delete': {
                'help': 'delete dvg',
                'arguments': {
                    'dvg_name': {
                        'help': 'dvg name',
                    },
                },
            },
            'extend': {
                'help': 'extend dvg',
                'arguments': {
                    'dvg_name': {
                        'help': 'dvg_name',
                    },
                    'dpv_name': {
                        'help': 'dpv_hostname',
                    },
                },
            },
            'reduce': {
                'help': 'reduce dvg',
                'arguments': {
                    'dvg_name': {
                        'help': 'dvg_name',
                    },
                    'dpv_name': {
                        'help': 'dpv_hostname',
                    },
                },
            },
            'display': {
                'help': 'display dvg',
                'arguments': {
                    'dvg_name': {
                        'help': 'dvg_name',
                    },
                },
            }
        },
    },
    'dlv': {
        'help': 'manage dlv',
        'cmds': {
            'list': {
                'help': 'list dlvs',
            },
            'create': {
                'help': 'create dlv',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                    'dlv_size': {
                        'type': convert_to_byte,
                        'help': 'dlv size',
                    },
                    'init_size': {
                        'type': convert_to_byte,
                        'help': 'dlv init size',
                    },
                    'stripe_number': {
                        'type': int,
                        'help': 'partition count',
                    },
                    'dvg_name': {
                        'help': 'dvg name',
                    },
                },
            },
            'delete': {
                'help': 'delete dlv',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                },
            },
            'attach': {
                'help': 'attach dlv to a ihost',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                    'ihost_name': {
                        'help': 'ihost_name',
                    },
                },
            },
            'detach': {
                'help': 'detach dlv from a ihost',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                    'ihost_name': {
                        'help': 'ihost name',
                    },
                },
            },
            'set_snap': {
                'help': 'switch dlv to a specific snapshot',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                    'snap_name': {
                        'help': 'snap name',
                    },
                },
            },
            'display': {
                'help': 'display dlv',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                },
            },
        },
    },
    'snap': {
        'help': 'manage snapshot',
        'cmds': {
            'list': {
                'help': 'list snapshots',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                },
            },
            'display': {
                'help': 'display snapshot',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                    'snap_name': {
                        'help': 'snapshot name',
                    },
                },
            },
            'create': {
                'help': 'create snapshot',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                    'snap_name': {
                        'help': 'snapshot name',
                    },
                    'ori_snap_name': {
                        'help': 'original snapshot name',
                    },
                },
            },
            'delete': {
                'help': 'delete snapshot',
                'arguments': {
                    'dlv_name': {
                        'help': 'dlv name',
                    },
                    'snap_name': {
                        'help': 'snapshot name',
                    },
                },
            },
        },
    },
    'ihost': {
        'help': 'manage ihost',
        'cmds': {
            'list': {
                'help': 'list ihosts',
            },
            'create': {
                'help': 'create ihost',
                'arguments': {
                    'ihost_name': {
                        'help': 'ihost hostname',
                    },
                },
            },
            'delete': {
                'help': 'delete ihost',
                'arguments': {
                    'ihost_name': {
                        'help': 'ihost hostname',
                    },
                },
            },
            'display': {
                'help': 'show ihost',
                'arguments': {
                    'ihost_name': {
                        'help': 'ihost hostname',
                    },
                },
            },
            'available': {
                'help': 'set ihost to available status',
                'arguments': {
                    'ihost_name': {
                        'help': 'ihost hostname',
                    },
                },
            },
            'unavailable': {
                'help': 'set ihost to unavailable status',
                'arguments': {
                    'ihost_name': {
                        'help': 'ihost hostname',
                    },
                },
            },
        },
    },
    'fj': {
        'help': 'manage failover job',
        'cmds': {
            'list': {
                'help': 'list fjs',
            },
            'display': {
                'help': 'show fj',
                'arguments': {
                    'fj_name': {
                        'help': 'fj name',
                        'required': True,
                    },
                    'with_process': {
                        'help': 'show fj process',
                        'choices': ['true', 'false'],
                        'default': 'false',
                    },
                },
            },
            'create': {
                'help': 'create fj',
                'arguments': {
                    'fj_name': {
                        'help': 'fj name',
                        'required': True,
                    },
                    'dlv_name': {
                        'help': 'dlv name',
                        'required': True,
                    },
                    'ori_id': {
                        'help': 'ori leg id',
                        'required': True,
                    },
                },
            },
            'cancel': {
                'help': 'cancel fj',
                'arguments': {
                    'fj_name': {
                        'help': 'fj_name',
                        'required': True,
                    },
                },
            },
            'finish': {
                'help': 'finish fj',
                'arguments': {
                    'fj_name': {
                        'help': 'fj_name',
                        'required': True,
                    },
                },
            },
        },
    },
    'ej': {
        'help': 'manage extend job',
        'cmds': {
            'list': {
                'help': 'list ejs',
            },
            'display': {
                'help': 'show ej',
                'arguments': {
                    'ej_name': {
                        'help': 'ej name',
                        'required': True,
                    },
                },
            },
            'create': {
                'help': 'create ej',
                'arguments': {
                    'ej_name': {
                        'help': 'ej name',
                        'required': True,
                    },
                    'dlv_name': {
                        'help': 'dlv name',
                        'required': True,
                    },
                    'ej_size': {
                        'help': 'ej size',
                        'type': convert_to_byte,
                        'required': True,
                    },
                },
            },
        },
    },
    'obt': {
        'help': 'manage obt',
        'cmds': {
            'list': {
                'help': 'list obts',
            },
            'display': {
                'help': 'show obt',
                'arguments': {
                    't_id': {
                        'help': 'obt id',
                        'required': True,
                    },
                },
            },
            'resume': {
                'help': 'resume obt',
                'arguments': {
                    't_id': {
                        'help': 'obt id',
                        'required': True,
                    },
                },
            },
            'cancel': {
                'help': 'cancel obt',
                'arguments': {
                    't_id': {
                        'help': 'obt id',
                        'required': True,
                    },
                },
            },
        },
    },
}


def generate_func(cmd_name, sub_name, kwarg_list):
    def func(args):
        kwargs = {}
        for kwarg in kwarg_list:
            kwargs[kwarg] = getattr(args, kwarg)
        layer2_method_name = '{cmd_name}_{sub_name}'.format(
            cmd_name=cmd_name, sub_name=sub_name)
        client = Layer2(args.conf['api_server_list'])
        layer2_method = getattr(client, layer2_method_name)
        ret = layer2_method(**kwargs)
        print(json.dumps(ret, indent=4))
    return func


def generate_parser(cli_cmds):
    parser = argparse.ArgumentParser(
        prog='dlvm',
        add_help=True,
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='0.0.1',
    )
    parser.add_argument(
        '--log_level',
        choices=['debug', 'info', 'warning', 'error'],
        default='info',
    )
    parser.add_argument(
        '--conf',
        type=get_conf,
        default=default_conf,
        help='dlvm client configuration file path'
    )

    main_subparsers = parser.add_subparsers(help='dlvm commands')

    for cmd_name in cli_cmds:
        parser1 = main_subparsers.add_parser(
            cmd_name,
            help=cli_cmds[cmd_name]['help'],
        )
        parser2 = parser1.add_subparsers(
            help=cli_cmds[cmd_name]['help'],
        )
        for sub_name in cli_cmds[cmd_name]['cmds']:
            parser3 = parser2.add_parser(
                sub_name,
                help=cli_cmds[cmd_name]['cmds'][sub_name]['help'],
            )
            arguments = cli_cmds[cmd_name]['cmds'][sub_name].get(
                'arguments', {})
            if arguments:
                for name in arguments:
                    arg_name = '--{name}'.format(name=name)
                    parser3.add_argument(arg_name, **arguments[name])
            func = generate_func(cmd_name, sub_name, arguments.keys())
            parser3.set_defaults(func=func)
    return parser


def main():
    parser = generate_parser(CLI_CMDS)
    args = parser.parse_args()
    console = logging.StreamHandler()
    logger.setLevel(LOG_MAPPING[args.log_level])
    logger.addHandler(console)
    return args.func(args)


if __name__ == '__main__':
    main()
