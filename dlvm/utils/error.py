#!/usr/bin/env python


class ObtConflictError(Exception):
    pass


class ObtMissError(Exception):
    pass


class NoEnoughDpvError(Exception):
    pass


class DpvError(Exception):
    pass


class ThostError(Exception):
    pass


class DlvStatusError(Exception):
    pass


class HasMjsError(Exception):
    pass
