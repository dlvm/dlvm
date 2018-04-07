from typing import Optional, Union

from dlvm.common.utils import HttpStatus


class DlvmError(Exception):

    status_code = HttpStatus.InternalServerError

    def __init__(self, message: str, exc_info: Optional[str]=None)-> None:
        self.message = message
        self.exc_info = exc_info
        super(DlvmError, self).__init__(message)


class LimitExceedError(DlvmError):

    status_code = HttpStatus.BadRequest

    def __init__(self, curr_val: int, max_val: int)-> None:
        message = 'limit_exceed curr_val={0} max_val={1}'.format(
            curr_val, max_val)
        super(LimitExceedError, self).__init__(message)


class DpvError(DlvmError):

    status_code = HttpStatus.InternalServerError

    def __init__(self, dpv_name: str)-> None:
        message = 'dpv_error dpv_name={0}'.format(dpv_name)
        super(DpvError, self).__init__(message)


class IhostError(DlvmError):

    status_code = HttpStatus.InternalServerError

    def __init__(self, ihost_name: str)-> None:
        message = 'ihost_error ihost_name={0}'.format(ihost_name)
        super(IhostError, self).__init__(message)


class ResourceDuplicateError(DlvmError):

    status_code = HttpStatus.BadRequest

    def __init__(self, res_type: str, res_name: str, exc_info: str)-> None:
        message = (
            'resource_duplicate '
            'res_type={0} '
            'res_name={1}'
        ).format(res_type, res_name)
        super(ResourceDuplicateError, self).__init__(message, exc_info)


class ResourceNotFoundError(DlvmError):

    status_code = HttpStatus.NotFound

    def __init__(self, res_type: str, res_name: str)-> None:
        message = (
            'resource_not_found '
            'res_type={0} '
            'res_name={1}'
        ).format(res_type, res_name)
        super(ResourceNotFoundError, self).__init__(message)


class ResourceBusyError(DlvmError):

    status_code = HttpStatus.BadRequest

    def __init__(
            self, res_type: str, res_name: str,
            busy_type: str, busy_id: str)-> None:
        message = (
            'resource_busy '
            'res_type={0} '
            'res_name={1} '
            'busy_type={2} '
            'busy_id={3}'
        ).format(res_type, res_name, busy_type, busy_id)
        super(ResourceBusyError, self).__init__(message)


class ResourceInvalidError(DlvmError):

    status_code = HttpStatus.BadRequest

    def __init__(
            self, res_type: str, res_name: str,
            field_name: str, field_value: Union[int, str])-> None:
        message = (
            'resource_invalid '
            'res_type={0} '
            'res_name={1} '
            'field_name={2} '
            'field_value={3}'
        ).format(res_type, res_name, field_name, field_value)
        super(ResourceInvalidError, self).__init__(message)
