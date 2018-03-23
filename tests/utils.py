#!/usr/bin/env python

from dlvm.utils.modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup


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

    @app_context
    def dpv_get(self, dpv_name):
        dpvs = DistributePhysicalVolume \
              .query \
              .filter_by(dpv_name=dpv_name) \
              .limit(1) \
              .all()
        if len(dpvs) == 0:
            return None
        else:
            return dpvs[0]

    @app_context
    def dpv_set(self, dpv_name, name, value):
        dpv = DistributePhysicalVolume \
              .query \
              .filter_by(dpv_name=dpv_name) \
              .one()
        setattr(dpv, name, value)
        db.session.add(dpv)
        db.session.commit()

    @app_context
    def dvg_create(self, dvg_name):
        dvg = DistributeVolumeGroup(
            dvg_name=dvg_name,
            total_size=0,
            free_size=0,
        )
        db.session.add(dvg)
        db.session.commit()

    @app_context
    def dvg_get(self, dvg_name):
        dvgs = DistributeVolumeGroup \
               .query \
               .filter_by(dvg_name=dvg_name) \
               .limit(1) \
               .all()
        if len(dvgs) == 0:
            return None
        else:
            return dvgs[0]

    @app_context
    def dvg_set(self, dvg_name, name, value):
        dvg = DistributeVolumeGroup \
               .query \
               .filter_by(dvg_name=dvg_name) \
               .one()
        setattr(dvg, name, value)
        db.session.add(dvg)
        db.session.commit()
