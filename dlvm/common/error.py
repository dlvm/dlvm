
class DlvmError(Exception):

    def __init__(self, message, exc_info=None):
        self.exc_info = exc_info
        super(DlvmError, self).__init__(message)


class LimitExceedError(DlvmError):

    ret_code = 400

    def __init__(self, curr_val, max_val):
        message = 'limit_exceed curr_val={0} max_val={1}'.format(
            curr_val, max_val)
        super(LimitExceedError, self).__init__(message)


class DpvError(DlvmError):

    ret_code = 500

    def __init__(self, dpv_name):
        message = 'dpv_error dpv_name={0}'.format(dpv_name)
        super(DpvError, self).__init__(message)


class IhostError(DlvmError):

    ret_code = 500

    def __init__(self, ihost_name):
        message = 'ihost_error ihost_name={0}'.fomrat(ihost_name)
        super(IhostError, self).__init__(message)


class ResourceDuplicateError(DlvmError):

    ret_code = 400

    def __init__(self, res_type, res_name, exc_info):
        message = (
            'resource_duplicate '
            'res_type={0} '
            'res_name={1}'
        ).format(res_type, res_name)
        super(ResourceDuplicateError, self).__init__(message, exc_info)


class ResourceNotFoundError(DlvmError):

    ret_code = 404

    def __init__(self, res_type, res_name, exc_info):
        message = (
            'resource_not_found '
            'res_type={0} '
            'res_name={1}'
        ).format(res_type, res_name)
        super(ResourceNotFoundError, self).__init__(message, exc_info)


class ResourceBusyError(DlvmError):

    ret_code = 400

    def __init__(self, res_type, res_name, busy_type, busy_id):
        message = (
            'resource_busy '
            'res_type={0} '
            'res_name={1} '
            'busy_type={2} '
            'busy_id={3}'
        ).format(res_type, res_name, busy_type, busy_id)
        super(ResourceBusyError, self).__init__(message)


class ResourceInvalidError(DlvmError):

    ret_code = 400

    def __init__(self, res_type, res_name, field_name, field_value):
        message = (
            'resource_invalid '
            'res_type={0} '
            'res_name={1} '
            'field_name={2} '
            'field_value={3}'
        ).format(res_type, res_name, field_name, field_value)
        super(ResourceInvalidError, self).__init__(message)
