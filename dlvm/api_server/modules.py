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
    in_sync = db.Column(
        db.Boolean,
        nullable=False,
    )
    status = db.Column(
        db.Enum('available', 'unavailable', name='dpv_status'),
        nullable=False,
    )
    timestamp = db.Column(
        db.DateTime,
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
    annotation = db.Column(
        db.Text,
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
            'creating', 'create_failed',
            'deleting', 'delete_failed',
            'attaching', 'attach_failed',
            'detaching', 'detach_failed',
            'detached', 'attached',
            name='dlv_status',
        ),
        nullable=False,
    )
    timestamp = db.Column(
        db.DateTime,
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
    fjs = db.relationship(
        'FailoverJob',
        back_populates='dlv',
    )
    ej = db.relationship(
        'ExtendJob',
        back_populates='dlv',
        uselist=False,
    )
    t_id = db.Column(
        db.String(32),
        db.ForeignKey('owner_based_transaction.t_id'),
    )
    obt = db.relationship(
        'OwnerBasedTransaction',
        back_populates='dlvs',
    )


class InitiatorHost(db.Model):
    ihost_name = db.Column(
        db.String(32),
        primary_key=True,
    )
    in_sync = db.Column(
        db.Boolean,
        nullable=False,
    )
    status = db.Column(
        db.Enum('available', 'unavailable', name='ihost_status'),
        nullable=False,
    )
    timestamp = db.Column(
        db.DateTime,
        nullable=False,
    )
    dlvs = db.relationship(
        'DistributeLogicalVolume',
        back_populates='ihost',
    )
    annotation = db.Column(
        db.Text,
    )


class Snapshot(db.Model):
    snap_name = db.Column(
        db.String(64),
        primary_key=True,
    )
    timestamp = db.Column(
        db.DateTime,
        nullable=False,
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
            'creating', 'create_failed',
            'available',
            'deleting', 'delete_failed',
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
    ej_name = db.Column(
        db.String(32),
        db.ForeignKey('extend_job.ej_name'),
    )
    ej = db.relationship(
        'ExtendJob',
        back_populates='group',
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
    fj_role = db.Column(
        db.Enum('ori', 'src', 'dst', name='leg_role'),
    )
    fj_name = db.Column(
        db.String(32),
        db.ForeignKey('failover_job.fj_name'),
    )
    fj = db.relationship(
        'FailoverJob',
        back_populates='legs',
    )


class FailoverJob(db.Model):
    fj_name = db.Column(
        db.String(32),
        primary_key=True,
    )
    legs = db.relationship(
        'Leg',
        back_populates='fj',
    )
    status = db.Column(
        db.Enum(
            'creating', 'create_failed',
            'canceling', 'cancel_failed',
            'canceled',
            'processing',
            'finishing', 'finish_failed',
            'finished',
            name='fj_status',
        ),
        nullable=False,
    )
    timestamp = db.Column(
        db.DateTime,
        nullable=False,
    )
    dlv_name = db.Column(
        db.String(32),
        db.ForeignKey('distribute_logical_volume.dlv_name'),
        nullable=False,
    )
    dlv = db.relationship(
        'DistributeLogicalVolume',
        back_populates='fjs',
    )


class ExtendJob(db.Model):
    ej_name = db.Column(
        db.String(32),
        primary_key=True,
    )
    status = db.Column(
        db.Enum(
            'creating', 'create_failed',
            'canceling', 'cancel_failed',
            'canceled',
            'created',
            'finishing', 'finish_failed',
            'finished',
            name='ej_status',
        ),
        nullable=False,
    )
    timestamp = db.Column(
        db.DateTime,
        nullable=False,
    )
    ej_size = db.Column(
        db.BigInteger,
        nullable=False,
    )
    dlv_name = db.Column(
        db.String(32),
        db.ForeignKey('distribute_logical_volume.dlv_name'),
        nullable=False,
    )
    dlv = db.relationship(
        'DistributeLogicalVolume',
        back_populates='ej',
    )
    group = db.relationship(
        'Group',
        back_populates='ej',
        uselist=False,
    )


class OwnerBasedTransaction(db.Model):
    t_id = db.Column(
        db.String(32),
        primary_key=True,
    )
    t_owner = db.Column(
        db.String(32),
        nullable=False,
    )
    t_stage = db.Column(
        db.Integer,
        nullable=False,
    )
    timestamp = db.Column(
        db.DateTime,
        nullable=False,
    )
    annotation = db.Column(
        db.Text,
    )
    count = db.Column(
        db.BigInteger().with_variant(db.Integer, "sqlite"),
        db.ForeignKey('counter.count'),
        nullable=False,
    )
    counter = db.relationship(
        'Counter',
        back_populates='transaction'
    )
    dlvs = db.relationship(
        'DistributeLogicalVolume',
        back_populates='obt',
    )


class Counter(db.Model):
    count = db.Column(
        db.BigInteger().with_variant(db.Integer, "sqlite"),
        primary_key=True,
    )
    transaction = db.relationship(
        'OwnerBasedTransaction',
        back_populates='counter',
        uselist=False,
    )
