#!/usr/bin/env python

import os
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
    print('dpv_list')


def dpv_display(args):
    print('dpv_display')


def dpv_create(args):
    print('dpv_create')


def dpv_delete(args):
    print('dpv_delete')


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

main_subparsers = parser.add_subparsers(help='main commands')

dpv_parser = main_subparsers.add_parser(
    'dpv',
    help='manage dpv',
)

dpv_subparsers = dpv_parser.add_subparsers(
    help='dpv subcommands',
)

dpv_list_parser = dpv_subparsers.add_parser(
    'list',
    help='list dpvs',
)
dpv_list_parser.set_defaults(func=dpv_list)

dpv_create_parser = dpv_subparsers.add_parser(
    'create',
    help='create dpv',
)
dpv_create_parser.add_argument(
    'dpv_name',
    help='dpv hostname',
)
dpv_create_parser.set_defaults(func=dpv_create)

dpv_delete_parser = dpv_subparsers.add_parser(
    'delete',
    help='delete dpv',
)
dpv_delete_parser.add_argument(
    'dpv_name',
    help='dpv hostname',
)
dpv_delete_parser.set_defaults(func=dpv_delete)

dpv_available_parser = dpv_subparsers.add_parser(
    'available',
    help='set dpv to available status',
)
dpv_available_parser.add_argument(
    'dpv_name',
    help='dpv hostname',
)
dpv_available_parser.set_defaults(func=dpv_available)

dpv_unavailable_parser = dpv_subparsers.add_parser(
    'unavailable',
    help='set dpv to unavailable status',
)
dpv_unavailable_parser.add_argument(
    'dpv_name',
    help='dpv hostname',
)
dpv_unavailable_parser.set_defaults(func=dpv_unavailable)


def main():
    args = parser.parse_args()
    console = logging.StreamHandler()
    logger.setLevel(LOG_MAPPING[args.log_level])
    logger.addHandler(console)
    return args.func(args)


if __name__ == '__main__':
    main()
