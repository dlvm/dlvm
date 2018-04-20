import traceback

from dlvm.wrapper.hook import DlvmHook


class LogHook(DlvmHook):

    def pre_hook(self, hook_ctx):
        hook_ctx.req_ctx.logger.info('pre_hook: %s', hook_ctx)

    def post_hook(self, hook_ctx, hook_ret, func_ret):
        hook_ctx.req_ctx.logger.info('post_hook: %s %s', hook_ctx, func_ret)

    def error_hook(self, hook_ctx, hook_ret, err_info):
        calltrace = ''.join(traceback.format_exception(
            err_info.etype, err_info.value, err_info.tb))
        hook_ctx.req_ctx.logger.warning(
            'error_hook: %s\n%s', hook_ctx, calltrace)
