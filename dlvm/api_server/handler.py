#!/usr/bin/env python

from collections import OrderedDict
import uuid
import logging
from dlvm.utils.error import TransactionConflictError, TransactionMissError
from modules import db

logger = logging.getLogger('dlvm_api')


def handle_dlvm_request(params, parser, handler):
    request_id = uuid.uuid4().hex
    response = OrderedDict()
    response['request_id'] = request_id
    if parser:
        args = parser.parse_args()
    else:
        args = None
    logger.info('request_id=%s, params=%s, args=%s, handler=%s',
                request_id, params, args, handler.__name__)
    try:
        body, return_code = handler(params, args)
    except TransactionConflictError:
        db.session.rollback()
        logger.warning('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'transaction_conflict',
        }
        return_code = 400
        response['body'] = body
    except TransactionMissError:
        db.session.rollback()
        logger.warning('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'transaction_miss',
        }
        return_code = 400
        response['body'] = body
    except:
        db.session.rollback()
        logger.error('request_id=%s', request_id, exc_info=True)
        body = {
            'message': 'internal_error',
        }
        return_code = 500
        response['body'] = body
    finally:
        db.session.close()
        logger.info('request_id=%s\nbody=%s\nreturn_code=%d',
                    request_id, body, return_code)
        response['body'] = body
        return response, return_code
