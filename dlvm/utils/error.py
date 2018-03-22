#!/usr/bin/env python


class DlvmError(Exception):

    def __init__(self, message, exc_info=None):
        self.exc_info = exc_info
        super(DlvmError, self).__init__(message)


class LimitExceedError(DlvmError):

    def __init__(self, curr_val, max_val):
        self.ret_code = 400
        message = 'limit_exceed curr_val={curr_val} max_val={max_val}'.format(
            curr_val=curr_val, max_val=max_val)
        super(LimitExceedError, self).__init__(message)


class DpvError(DlvmError):

    ret_code = 500

    def __init__(self, dpv_name, exc_info):
        message = 'dpv_error dpv_name={dpv_name}'.format(
            dpv_name=dpv_name)
        super(DpvError, self).__init__(message, exc_info)


class IhostError(DlvmError):

    ret_code = 500

    def __init__(self, ihost_name, exc_info):
        message = 'ihost_error ihost_name={ihost_name}'.fomrat(
            ihost_name=ihost_name)
        super(IhostError, self).__init__(message, exc_info)


class ResourceDuplicateError(DlvmError):

    ret_code = 400

    def __init__(self, res_type, res_name, exc_info):
        message = (
            'resource_duplicate '
            'res_type={res_type} '
            'res_name={res_name}'
        ).format(
            res_type=res_type, res_name=res_name)
        super(ResourceDuplicateError, self).__init__(message, exc_info)


class ResourceNotFoundError(DlvmError):

    ret_code = 404

    def __init__(self, res_type, res_name, exc_info):
        message = (
            'resource_not_found '
            'res_type={res_type} '
            'res_name={res_name}'
        ).format(
            res_type=res_type, res_name=res_name)
        super(ResourceNotFoundError, self).__init__(message, exc_info)


class ResourceBusyError(DlvmError):

    ret_code = 400

    def __init__(self, res_type, res_name, busy_type, busy_id):
        message = (
            'resource_busy '
            'res_type={res_type} '
            'res_name={res_name} '
            'busy_type={busy_type}'
            'busy_id={busy_id}'
        ).format(
            res_type=res_type,
            res_name=res_name,
            busy_type=busy_type,
            busy_id=busy_id,
        )
        super(ResourceBusyError, self).__init__(message)
