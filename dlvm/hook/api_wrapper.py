from collections import OrderedDict
import uuid
import logging

from flask_restful import marshal

from dlvm.common.utils import RequestContext, WorkContext
from dlvm.common.database import Session
from dlvm.common.error import DlvmError


logger = logging.getLogger('dlvm_api')


def handle_dlvm_api(
        handler, success_code, parser, marshal_fields, args, kwargs):
    req_id = uuid.uuid4().hex
    response = OrderedDict()
    response['req_id'] = req_id
    req_ctx = RequestContext(req_id, logger)
    if parser is None:
        params = None
    else:
        params = parser.parse_args()

    try:
        session = Session()
        work_ctx = WorkContext(session, set())
        kwargs.update(zip(handler.__code__.co_varnames, args))
        raw_body = handler(req_ctx, work_ctx, params, kwargs)
        if marshal_fields is None:
            body = raw_body
        else:
            body = marshal(raw_body, marshal_fields)
        message = 'succeed'
        return_code = success_code
    except Exception as e:
        session.rollback()
        if isinstance(e, DlvmError):
            message = e.message
            body = None
            return_code = e.return_code
        else:
            message = 'internal_error'
            body = None
            return_code = 500
    finally:
        if 'session' in locals():
            session.close()
        response['message'] = message
        response['body'] = body
        return response, return_code
