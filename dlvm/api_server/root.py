from dlvm.common.utils import HttpStatus
from dlvm.wrapper.api_wrapper import ApiMethod, ApiResource


def root_get():
    return 'dlvm'


root_get_method = ApiMethod(root_get, HttpStatus.OK)

root_res = ApiResource('/', get=root_get_method)
