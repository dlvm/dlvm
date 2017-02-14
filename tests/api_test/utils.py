#!/usr/bin/env python

import uuid
from dlvm.api_server.modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume, \
    Snapshot, Group, Leg, TargetHost, FailoverJob, \
    OwnerBasedTransaction, Counter

mirror_meta_size = 1024*1024*2


def div_round_up(dividend, divisor):
    return (dividend+divisor-1) / divisor


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
    def dpv_create(self, dpv_name, total_size, free_size, status, timestamp):
        dpv = DistributePhysicalVolume(
            dpv_name=dpv_name,
            total_size=total_size,
            free_size=free_size,
            status=status,
            timestamp=timestamp,
        )
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
    def dvg_extend(self, dvg_name, dpv_name):
        dvg = DistributeVolumeGroup \
            .query \
            .filter_by(dvg_name=dvg_name) \
            .one()
        dpv = DistributePhysicalVolume \
            .query \
            .filter_by(dpv_name=dpv_name) \
            .one()
        dpv.dvg_name = dvg_name
        db.session.add(dpv)
        dvg.free_size += dpv.free_size
        dvg.total_size += dpv.total_size
        db.session.add(dvg)
        db.session.commit()

    @app_context
    def dlv_create(
            self, dlv_name, dlv_size, data_size, partition_count, status,
            timestamp, dvg_name, igroups):
        snap_name = '%s/base' % dlv_name
        dlv = DistributeLogicalVolume(
            dlv_name=dlv_name,
            dlv_size=dlv_size,
            data_size=data_size,
            partition_count=partition_count,
            status=status,
            timestamp=timestamp,
            active_snap_name=snap_name,
            dvg_name=dvg_name,
        )
        db.session.add(dlv)
        snapshot = Snapshot(
            snap_name=snap_name,
            thin_id=0,
            ori_thin_id=0,
            status='available',
            timestamp=timestamp,
            dlv_name=dlv_name,
        )
        db.session.add(snapshot)
        db.session.commit()

        for idx, igroup in enumerate(igroups):
            group_size = igroup['group_size']
            group = Group(
                group_id=uuid.uuid4().hex,
                idx=idx,
                group_size=group_size,
                dlv_name=dlv_name,
            )
            db.session.add(group)

            if idx == 0:
                leg_size = group_size + mirror_meta_size
                legs_per_group = 2
            else:
                leg_size = div_round_up(
                    group_size, partition_count) + mirror_meta_size
                legs_per_group = 2 * partition_count
            dpvs = DistributePhysicalVolume \
                .query \
                .filter_by(dvg_name=dvg_name) \
                .limit(legs_per_group) \
                .all()
            dvg = DistributeVolumeGroup \
                .query \
                .filter_by(dvg_name=dvg_name) \
                .one()
            for i in xrange(legs_per_group):
                dpv = dpvs[i]
                dpv.free_size -= leg_size
                db.session.add(dpv)
                dvg.free_size -= leg_size
                leg = Leg(
                    leg_id=uuid.uuid4().hex,
                    idx=i,
                    group=group,
                    leg_size=leg_size,
                    dpv=dpv,
                )
                db.session.add(leg)
            db.session.add(dvg)
            db.session.commit()

    @app_context
    def snapshot_create(
            self, snap_name, timestamp, thin_id,
            ori_thin_id, status, dlv_name):
        snap_name = '{dlv_name}/{snap_name}'.format(
            dlv_name=dlv_name, snap_name=snap_name)
        snapshot = Snapshot(
            snap_name=snap_name,
            timestamp=timestamp,
            thin_id=thin_id,
            ori_thin_id=ori_thin_id,
            status=status,
            dlv_name=dlv_name,
        )
        db.session.add(snapshot)
        db.session.commit()

    @app_context
    def obt_create(self, t_id, t_owner, t_stage, timestamp):
        counter = Counter()
        db.session.add(counter)
        obt = OwnerBasedTransaction(
            t_id=t_id,
            t_owner=t_owner,
            t_stage=t_stage,
            timestamp=timestamp,
            counter=counter,
        )
        db.session.add(obt)
        db.session.commit()

    @app_context
    def thost_create(self, thost_name, status, timestamp):
        thost = TargetHost(
            thost_name=thost_name,
            status=status,
            timestamp=timestamp,
        )
        db.session.add(thost)
        db.session.commit()

    @app_context
    def dlv_attach(self, dlv_name, thost_name):
        dlv = DistributeLogicalVolume \
            .query \
            .filter_by(dlv_name=dlv_name) \
            .one()
        dlv.thost_name = thost_name
        dlv.status = 'attached'
        db.session.add(dlv)
        db.session.commit()

    @app_context
    def fj_create(self, fj_name, status, timestamp, dlv_name, g_idx, l_idx):
        fj = FailoverJob(
            fj_name=fj_name,
            status=status,
            timestamp=timestamp,
            dlv_name=dlv_name,
        )
        db.session.add(fj)

        dlv = DistributeLogicalVolume \
            .query \
            .filter_by(dlv_name=dlv_name) \
            .one()
        ori_leg = None
        for group in dlv.groups:
            if group.idx == g_idx:
                for leg in group.legs:
                    if leg.idx == l_idx:
                        ori_leg = leg
                        break
                break
        assert(ori_leg is not None)
        ori_idx = ori_leg.idx
        if ori_idx % 2 == 0:
            src_idx = ori_idx + 1
        else:
            src_idx = ori_idx - 1
        group = ori_leg.group
        src_leg = None
        for leg in group.legs:
            if leg.idx == src_idx:
                src_leg = leg
                break
        assert(src_leg is not None)
        src_leg.fj_role = 'src'
        ori_leg.fj_role = 'ori'
        src_leg.fj = fj
        ori_leg.fj = fj
        db.session.add(src_leg)
        db.session.add(ori_leg)
        legs = ori_leg.group.legs
        count = len(legs)
        dpvs = DistributePhysicalVolume \
            .query \
            .filter_by(dvg_name=dlv.dvg_name) \
            .limit(count+1) \
            .all()
        exclude_name_set = set()
        for ileg in legs:
            exclude_name_set.add(ileg.dpv_name)
        dst_dpv = None
        for dpv in dpvs:
            if dpv.dpv_name not in exclude_name_set:
                dst_dpv = dpv
                break
        assert(dst_dpv is not None)
        dst_leg = Leg(
            leg_id=uuid.uuid4().hex,
            idx=ori_leg.idx,
            leg_size=ori_leg.leg_size,
            dpv=dst_dpv,
            fj_role='dst',
            fj=fj,
        )
        db.session.add(dst_leg)
        db.session.commit()

    @app_context
    def fj_set_status(self, fj_name, status):
        fj = FailoverJob \
            .query \
            .filter_by(fj_name=fj_name) \
            .one()
        fj.status = status
        if status == 'finished':
            for leg in fj.legs:
                if leg.fj_role == 'ori':
                    leg.dpv = None
                    db.session.add(leg)
                    break
        db.session.add(fj)
        db.session.commit()
