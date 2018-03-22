#!/usr/bin/env python

from dlvm.utils.modules import db, \
    DistributePhysicalVolume


def app_context(func):
    def _deco(self, *args, **kwargs):
        with self.app.app_context():
            ret = func(self, *args, **kwargs)
        return ret
    return _deco


class FixtureManager(object):

    def __init__(self, app):
        self.app = app

    @app_context
    def dpv_create(
            self, dpv_name,
            total_size, free_size, status):
        dpv = DistributePhysicalVolume(
            dpv_name=dpv_name,
            total_size=total_size,
            free_size=free_size,
            status=status,
        )
        db.session.add(dpv)
        db.session.commit()
