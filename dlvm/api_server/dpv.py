#!/usr/bin/env python

import logging
from dlvm.utils.rpc_wrapper import RpcClient
from dlvm.utils.configure import conf
from dlvm.utils.error import SUCCESS, EXCEED_LIMIT, DpvError
from modules import db, DistributePhysicalVolume, DistributeVolumeGroup
from handler import general_query


logger = logging.getLogger('dlvm_api')


def handle_dpvs_get(args, params):
    if args['limit'] > conf.dpv_list_limit:
        return None, EXCEED_LIMIT, 400
    dpvs = general_query(
        DistributePhysicalVolume, args, ['status', 'locked'])
    return dpvs, SUCCESS, 200


def handle_dpvs_post(args, params):
    pass
