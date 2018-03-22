#!/usr/bin/env python

from collections import OrderedDict
import uuid
import logging

from dlvm.utils.modules import db
from dlvm.utils.error import DlvmError
from dlvm.utils.rpc_wrapper import RpcClient
from dlvm.utils.configure import conf


logger = logging.getLogger('dlvm_api')


def handle_dlvm_request(handler, parser, path_args, success_code):
    request_id = uuid.uuid4().hex
    response = OrderedDict()
    response['request_id'] = request_id
    if parser is None:
        args = None
    else:
        args = parser.parse_args()
    logger.info('request_id=%s handler=%s args=<%s> path_args=<%s>',
                request_id, handler.__name__, args, path_args)
    try:
        body = handler(request_id, args, path_args)
    except Exception as e:
        db.session.rollback()
        if isinstance(e, DlvmError):
            message = e.message
            body = None
            return_code = e.return_code
            logger.warning(
                'request_id=%s error_message=<%s>',
                request_id, e.message)
        else:
            logger.error(
                'request_id=%s internal_error',
                request_id, exc_info=True)
            message = 'internal_error'
            body = None
            return_code = 500
    else:
        message = 'succeed'
        response['body'] = body
        return_code = success_code
    finally:
        db.session.close()
        response['message'] = message
        response['body'] = body
        logger.info(
            'request_id=%s response=<%s> return_code=%d',
            request_id, response, return_code)
        return response, return_code


def general_query(obj, args, filter_list):
    order_field = getattr(obj, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = obj.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = obj.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    for filter_name in filter_list:
        if args[filter_name]:
            kwarg = {filter_name: args[filter_name]}
            query = query.filter_by(**kwarg)
    query = query.limit(args['limit'])
    return query.all()


class DpvClient(RpcClient):

    def __init__(self, dpv_name, expire_time):
        logger = logging.getLogger('dlvm_api')
        super(DpvClient, self).__init__(
            dpv_name, conf.dpv_port, expire_time,
            conf.dpv_timeout, logger)
