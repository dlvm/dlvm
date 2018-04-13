from abc import ABC, abstractmethod
from importlib import import_module

from dlvm.common.configure import cfg


class DlvmHook(ABC):

    @abstractmethod
    def pre_hook(self, hook_ctx):
        raise NotImplementedError

    @abstractmethod
    def post_hook(self, hook_ctx, hook_ret, func_ret):
        raise NotImplementedError

    @abstractmethod
    def error_hook(self, hook_ctx, hook_ret, err_info):
        raise NotImplementedError


def build_hook_list(hook_name):
    hook_list = []
    cfg_hook_list = cfg.getlist('hook', hook_name)
    for cfg_hook_path in cfg_hook_list:
        spliter = cfg_hook_path.rindex('.')
        mod_name = cfg_hook_path[:spliter]
        cls_name = cfg_hook_path[spliter+1:]
        mod = import_module(mod_name)
        custom_cls = getattr(mod, cls_name)
        instance = custom_cls()
        hook_list.append(instance)
    return hook_list


def run_pre_hook(name, hook_list, hook_ctx):
    hook_ret_dict = {}
    for hook in hook_list:
        try:
            hook_ret = hook.pre_hook(hook_ctx)
        except Exception:
            hook_ctx.req_ctx.logger.error(
                'pre_hook failed: %s %s %s',
                name, hook, hook_ctx, exc_info=True)
            hook_ret_dict[hook] = None
        else:
            hook_ret_dict[hook] = hook_ret
    return hook_ret_dict


def run_post_hook(name, hook_list, hook_ctx, hook_ret_dict, func_ret):
    for hook in hook_list:
        try:
            hook_ret = hook_ret_dict[hook]
            hook.post_hook(hook_ctx, hook_ret, func_ret)
        except Exception:
            hook_ctx.req_ctx.logger.error(
                'post_hook failed, %s %s %s %s %s',
                name, hook, hook_ctx, hook_ret, func_ret, exc_info=True)


def run_error_hook(name, hook_list, hook_ctx, hook_ret_dict, exc_info):
    for hook in hook_list:
        try:
            hook_ret = hook_ret_dict[hook]
            hook.error_hook(hook_ctx, hook_ret, exc_info)
        except Exception:
            hook_ctx.req_ctx.logger.error(
                'error_hook failed, %s %s %s %s %s',
                name, hook, hook_ctx, hook_ret, exc_info, exc_info=True)
