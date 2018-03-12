#!/usr/bin/env python

SUCCESS = 'SUCCESS'
EXCEED_LIMIT = 'EXCEED_LIMIT'


class NoEnoughDpvError(Exception):
    pass


class DpvError(Exception):
    pass


class IhostError(Exception):
    pass


class DlvStatusError(Exception):
    pass


class FjStatusError(Exception):
    pass


class EjStatusError(Exception):
    pass


class DependenceCheckError(Exception):
    pass


class ThinMaxRetryError(Exception):
    pass


class SnapshotStatusError(Exception):
    pass


class DeleteActiveSnapshotError(Exception):
    pass


class SnapNameError(Exception):
    pass


class ApiError(Exception):
    pass


class FsmFailed(Exception):
    pass


class DlvIhostMisMatchError(Exception):
    pass


class SrcFjError(Exception):
    pass


class SrcEjError(Exception):
    pass


class CjStatusError(Exception):
    pass


class SrcStatusError(Exception):
    pass


class DstStatusError(Exception):
    pass


class RpcTimeout(Exception):
    pass
