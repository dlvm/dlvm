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


def dpv_list(args):
    client = Layer2(args.conf['api_server_list'])
    ret = client.dpv_list()
    print(json.dumps(ret, indent=4))


def dpv_display(args):
    client = Layer2(args.conf['api_server_list'])
    ret = client.dpv_display(args.dpv_name)
    print(json.dumps(ret, indent=4))


def dpv_create(args):
    client = Layer2(args.conf['api_server_list'])
    ret = client.dpv_create(args.dpv_name)
    print(json.dumps(ret, indent=4))


def dpv_delete(args):
    client = Layer2(args.conf['api_server_list'])
    ret = client.dpv_delete(args.dpv_name)
    print(json.dumps(ret, indent=4))


def dpv_available(args):
    print('dpv_available')


def dpv_unavailable(args):
    print('dpv_unavailable')


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
                'help': 'list_dpvs',
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
            'show': {
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
}


def generate_func(cmd_name, sub_name, kwarg_list):
    def func(args):
        kwargs = {}
        for kwarg in kwarg_list:
            kwargs[kwarg] = getattr(args, kwarg)
        print('[%s] [%s] [%s]' % (cmd_name, sub_name, kwargs))
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
