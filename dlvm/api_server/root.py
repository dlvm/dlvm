from dlvm.common.utils import HttpStatus
from dlvm.hook.api_wrapper import ApiMethod, ApiResource, ApiRet


def root_get():
    data = {'api_version': 'v1'}
    return ApiRet(data, None)


root_get_method = ApiMethod(root_get, HttpStatus.OK)

root_res = ApiResource('/', get=root_get_method)
