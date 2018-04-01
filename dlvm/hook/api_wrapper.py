from typing import List, Tuple, Mapping, MutableMapping, \
    Callable, Any, Optional
from collections import OrderedDict
import uuid
import logging
import traceback

from flask_restful import marshal
from flask_restful.reqparse import RequestParser

from dlvm.common.utils import ReqId, RequestContext, WorkContext
from dlvm.common.database import Session
from dlvm.common.error import DlvmError
from dlvm.hook.hook import build_hook, ApiHook, ApiParam, HookRet
from dlvm.core.modules import FieldType


ori_logger = logging.getLogger('dlvm_api')

api_hook_list: List[ApiHook] = build_hook(ApiHook)


def handle_dlvm_api(
        handler: Callable[
            [RequestContext, WorkContext,
             Mapping[str, FieldType],
             Mapping[str, str]], Any],
        success_code: int,
        parser: Optional[RequestParser],
        marshal_fields: Optional[Mapping],
        kwargs: Mapping)-> Tuple[OrderedDict, int]:
    req_id = ReqId(uuid.uuid4().hex)
    response: OrderedDict = OrderedDict()
    response['req_id'] = req_id
    logger = logging.LoggerAdapter(ori_logger, {'req_id': req_id})
    req_ctx = RequestContext(req_id, logger)
    if parser is None:
        params: Mapping[str, FieldType] = {}
    else:
        params = parser.parse_args()

    session = Session()
    work_ctx = WorkContext(session, set())

    hook_param = ApiParam(handler.__name__, req_ctx, work_ctx, params, kwargs)
    hook_ret_dict: MutableMapping[
        ApiHook, Optional[HookRet]] = {}
    for hook in api_hook_list:
        try:
            hook_ret = hook.pre_hook(hook_param)
        except Exception:
            logger.error(
                'api pre_hook failed: %s %s',
                repr(hook_param), repr(hook),
                exc_info=True)
        else:
            hook_ret_dict[hook] = hook_ret
    try:
        raw_body = handler(req_ctx, work_ctx, params, kwargs)
        if marshal_fields is None:
            body = raw_body
        else:
            body = marshal(raw_body, marshal_fields)
        message = 'succeed'
        return_code = success_code
    except Exception as e:
        calltrace = traceback.format_exc()
        for hook in api_hook_list:
            hook_ret = hook_ret_dict.get(hook)
            try:
                hook.error_hook(
                    hook_param, hook_ret, e, calltrace)
            except Exception:
                logger.error(
                    'api error_hook failed: %s %s %s %s %s',
                    repr(hook), repr(hook_param),
                    hook_ret, e, calltrace,
                    exc_info=True)
        session.rollback()
        if isinstance(e, DlvmError):
            message = e.message
            body = None
            return_code = e.ret_code
        else:
            message = 'internal_error'
            body = None
            return_code = 500
    else:
        for hook in api_hook_list:
            hook_ret = hook_ret_dict.get(hook)
            try:
                hook.post_hook(
                    hook_param, hook_ret, body)
            except Exception:
                logger.error(
                    'api post_hook failed: %s %s %s %s',
                    repr(hook), repr(hook_param), hook_ret, body)
    finally:
        session.close()
        response['message'] = message
        response['body'] = body
        return response, return_code
