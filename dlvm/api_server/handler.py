#!/usr/bin/env python

from collections import OrderedDict
import uuid
import logging
from modules import db


logger = logging.getLogger('dlvm_api')


def handle_dlvm_request(handler, parser, params):
    request_id = uuid.uuid4().hex
    response = OrderedDict()
    response['request_id'] = request_id
    if parser is None:
        args = None
    else:
        args = parser.parse_args()
    logger.info('request_id=%s, handler=%s, args=%s, params=%s',
                request_id, handler.__name__, args, params)
    try:
        body, message, return_code = handler(args, params)
    except:
        db.session.rollback()
        logger.error('request_id=%s', request_id, exc_info=True)
        message = 'internal_error'
        body = None
        return_code = 500
    finally:
        db.session.close()
        response['message'] = message
        response['body'] = body
        logger.info('response:<%s>, return_code=%d',
                    response, return_code)
        return response, return_code
