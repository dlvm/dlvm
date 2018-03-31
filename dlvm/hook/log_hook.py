from typing import Mapping, Optional

from dlvm.hook.hook import HookRet, RpcRet, ApiParam, ApiHook, \
    RpcServerParam, RpcServerHook, RpcClientParam, RpcClientHook


class LogApiHook(ApiHook):

    def pre_hook(self, param: ApiParam)-> Optional[HookRet]:
        param.req_ctx.logger.info('api_pre %s', ApiParam)
        return None

    def post_hook(
            self, param: ApiParam, hook_ret: Optional[HookRet],
            body: Mapping)-> None:
        param.req_ctx.logger.info('api_post %s %s', ApiParam, body)

    def error_hook(
            self, param: ApiParam, hook_ret: Optional[HookRet],
            e: Exception, calltrace: str)-> None:
        param.req_ctx.logger.warning(
            'api_error %s %s', param, calltrace)


class LogRpcServerHook(RpcServerHook):

    def pre_hook(self, param: RpcServerParam)-> Optional[HookRet]:
        param.req_ctx.logger.info('rpc_server_pre %s', param)
        return None

    def post_hook(
            self, param: RpcServerParam, hook_ret: Optional[HookRet],
            ret: RpcRet)-> None:
        param.req_ctx.logger.info('rpc_server_post %s', param, ret)

    def error_hook(
            self, param: RpcServerParam,
            hook_ret: Optional[HookRet], e: Exception,
            calltrace: str)-> None:
        param.req_ctx.logger.warning(
            'rpc_server_error %s %s', param, calltrace)


class LogRpcClientHook(RpcClientHook):

    def pre_hook(self, param: RpcClientParam)-> Optional[HookRet]:
        param.req_ctx.logger.info('rpc_clinet_pre %s', param)
        return None

    def post_hook(
            self, param: RpcClientParam,
            hook_ret: Optional[HookRet], ret: RpcRet)-> None:
        param.req_ctx.logger.info('rpc_client_post: %s %s', param, ret)

    def error_hook(
            self, param: RpcClientParam,
            hook_ret: Optional[HookRet], e: Exception,
            calltrace: str)-> None:
        param.req_ctx.logger.warning(
            'rpc_clinet_error: %s %s', param, calltrace)
