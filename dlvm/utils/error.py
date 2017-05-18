#!/usr/bin/env python


class ObtConflictError(Exception):
    pass


class ObtMissError(Exception):
    pass


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
