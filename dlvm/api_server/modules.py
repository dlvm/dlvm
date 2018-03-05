#!/usr/bin/env python

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class DistributePhysicalVolume(db.Model):
    dpv_name = db.Column(
        db.String(32),
        primary_key=True,
    )
    total_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    free_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    status = db.Column(
        db.Enum('available', 'unavailable', name='dpv_status'),
        nullable=False,
    )
    dvg_name = db.Column(
        db.String(32),
        db.ForeignKey('distribute_volume_group.dvg_name'),
    )
    dvg = db.relationship(
        'DistributeVolumeGroup',
        back_populates='dpvs',
    )
    legs = db.relationship(
        'Leg',
        back_populates='dpv',
    )
    lock_id = db.Column(
        db.String(32),
        db.ForeignKey('lock.lock_id'),
    )
    lock = db.relationship(
        'Lock',
    )


class DistributeVolumeGroup(db.Model):
    dvg_name = db.Column(
        db.String(32),
        primary_key=True,
    )
    total_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    free_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    dpvs = db.relationship(
        'DistributePhysicalVolume',
        back_populates='dvg',
        lazy='dynamic',
    )
    dlvs = db.relationship(
        'DistributeLogicalVolume',
        back_populates='dvg',
        lazy='dynamic',
    )


class DistributeLogicalVolume(db.Model):
    dlv_name = db.Column(
        db.String(32),
        primary_key=True,
    )
    dlv_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    data_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    stripe_number = db.Column(
        db.Integer,
        nullable=False,
    )
    status = db.Column(
        db.Enum(
            'creating', 'available',
            'attaching', 'attached',
            'detaching', 'deleting',
            name='dlv_status',
        ),
        nullable=False,
    )
    dvg_name = db.Column(
        db.String(32),
        db.ForeignKey('distribute_volume_group.dvg_name'),
        nullable=False,
    )
    dvg = db.relationship(
        'DistributeVolumeGroup',
        back_populates='dlvs',
    )
    ihost_name = db.Column(
        db.String(32),
        db.ForeignKey('initiator_host.ihost_name'),
    )
    ihost = db.relationship(
        'InitiatorHost',
        back_populates='dlvs',
    )
    snapshots = db.relationship(
        'Snapshot',
        back_populates='dlv',
    )
    active_snap_name = db.Column(
        db.String(64),
        nullable=False,
    )
    groups = db.relationship(
        'Group',
        back_populates='dlv',
    )
    lock_id = db.Column(
        db.String(32),
        db.ForeignKey('lock.lock_id'),
    )
    lock = db.relationship(
        'Lock',
    )


class InitiatorHost(db.Model):
    ihost_name = db.Column(
        db.String(32),
        primary_key=True,
    )
    status = db.Column(
        db.Enum('available', 'unavailable', name='ihost_status'),
        nullable=False,
    )
    dlvs = db.relationship(
        'DistributeLogicalVolume',
        back_populates='ihost',
    )
    lock_id = db.Column(
        db.String(32),
        db.ForeignKey('lock.lock_id'),
    )
    lock = db.relationship(
        'Lock',
    )


class Snapshot(db.Model):
    snap_name = db.Column(
        db.String(64),
        primary_key=True,
    )
    thin_id = db.Column(
        db.Integer,
        nullable=False,
    )
    ori_thin_id = db.Column(
        db.Integer,
        nullable=False,
    )
    status = db.Column(
        db.Enum(
            'creating', 'deleting',
            'available', 'failed',
            name='snap_status',
        ),
        nullable=False,
    )
    dlv_name = db.Column(
        db.String(32),
        db.ForeignKey('distribute_logical_volume.dlv_name'),
        nullable=False,
    )
    dlv = db.relationship(
        'DistributeLogicalVolume',
        back_populates='snapshots',
    )


class Group(db.Model):
    group_id = db.Column(
        db.String(32),
        primary_key=True,
    )
    idx = db.Column(
        db.Integer,
        nullable=False,
    )
    group_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    dlv_name = db.Column(
        db.String(32),
        db.ForeignKey('distribute_logical_volume.dlv_name'),
    )
    dlv = db.relationship(
        'DistributeLogicalVolume',
        back_populates='groups',
    )
    legs = db.relationship(
        'Leg',
        back_populates='group',
    )


class Leg(db.Model):
    leg_id = db.Column(
        db.String(32),
        primary_key=True,
    )
    idx = db.Column(
        db.Integer,
    )
    leg_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    group_id = db.Column(
        db.String(32),
        db.ForeignKey('group.group_id'),
    )
    group = db.relationship(
        'Group',
        back_populates='legs',
    )
    dpv_name = db.Column(
        db.String(32),
        db.ForeignKey('distribute_physical_volume.dpv_name'),
    )
    dpv = db.relationship(
        'DistributePhysicalVolume',
        back_populates='legs',
    )


class Lock(db.Model):
    lock_id = db.Column(
        db.String(32),
        primary_key=True,
    )
    timestamp = db.Column(
        db.DateTime,
        nullable=False,
    )
    annotation = db.Column(
        db.Text,
    )
