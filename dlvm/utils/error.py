#!/usr/bin/env python


class DlvmError(Exception):
    pass


class ExceedLimitError(DlvmError):

    def __init__(self, curr_val, max_val):
        self.ret_code = 400
        message = 'exceed limit, curr_val=%d, max_val=%d' % (
            curr_val, max_val)
        super(ExceedLimitError, self).__init__(message)


class DpvError(DlvmError):

    ret_code = 500

    def __init__(self, dpv_name):
        message = 'dpv_error: {dpv_name}'.format(
            dpv_name=dpv_name)
        super(DpvError, self).__init__(message)


class IhostError(DlvmError):

    ret_code = 500

    def __init__(self, ihost_name):
        message = 'ihost_error: {ihost_name}'.fomrat(
            ihost_name=ihost_name)
        super(IhostError, self).__init__(message)


class ResourceDuplicateError(DlvmError):

    ret_code = 400

    def __init__(self, res_type, res_name):
        message = '{res_type} {res_name} duplicate'.format(
            res_type=res_type, res_name=res_name)
        super(ResourceDuplicateError, self).__init__(message)
