from dlvm.hook.hook import HookRet, RpcRet, \
    RpcServerContext, RpcServerHook, RpcClientContext, RpcClientHook


class LogRpcServerHook(RpcServerHook):

    def pre_hook(self, rpc_server_ctx: RpcServerContext)-> HookRet:
        rpc_server_ctx.req_ctx.logger.info(
            'rpc_server_pre %s', rpc_server_ctx)
        return HookRet(None)

    def post_hook(
            self, rpc_server_ctx: RpcServerContext,
            hook_ret: HookRet, rpc_ret: RpcRet)-> None:
        rpc_server_ctx.req_ctx.logger.info(
            'rpc_server_post %s %s',
            rpc_server_ctx, rpc_ret)

    def error_hook(
            self, rpc_server_ctx: RpcServerContext, hook_ret: HookRet,
            e: Exception, calltrace: str)-> None:
        rpc_server_ctx.req_ctx.logger.warning(
            'rpc_server-error %s %s', rpc_server_ctx, calltrace)


class LogRpcClientHook(RpcClientHook):

    def pre_hook(self, rpc_client_ctx: RpcClientContext)-> HookRet:
        rpc_client_ctx.req_ctx.logger.info(
            'rpc_client_pre %s', rpc_client_ctx)
        return HookRet(None)

    def post_hook(
            self, rpc_client_ctx: RpcClientContext,
            hook_ret: HookRet, rpc_ret: RpcRet)-> None:
        rpc_client_ctx.req_ctx.logger.info(
            'rpc_client_post: %s %s', rpc_client_ctx, rpc_ret)

    def error_hook(
            self, rpc_client_ctx: RpcClientContext, hook_ret: HookRet,
            e: Exception, calltrace: str)-> None:
        rpc_client_ctx.req_ctx.logger.warning(
            'rpc_client_error: %s %s', rpc_client_ctx, calltrace)
