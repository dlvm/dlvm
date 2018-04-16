import enum

from sqlalchemy import Column, BigInteger, Integer, String, \
    DateTime, Enum, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class DpvStatus(enum.Enum):
    available = 'available'
    unavailable = 'unavailable'


class DistributePhysicalVolume(Base):

    __tablename__ = 'distribute_physical_volume'

    dpv_name = Column(String(64), primary_key=True)

    total_size = Column(BigInteger, nullable=False)

    free_size = Column(BigInteger, nullable=False)

    status = Column(Enum(DpvStatus, name='dpv_status'), nullable=False)

    dvg_name = Column(
        String(32), ForeignKey('distribute_volume_group.dvg_name'))

    dvg = relationship('DistributeVolumeGroup', back_populates='dpvs')

    legs = relationship('Leg', back_populates='dpv')

    lock_id = Column(BigInteger, ForeignKey('lock.lock_id'))

    lock = relationship('Lock')


class DistributeVolumeGroup(Base):

    __tablename__ = 'distribute_volume_group'

    dvg_name = Column(String(32), primary_key=True)

    total_size = Column(BigInteger, nullable=False)

    free_size = Column(BigInteger, nullable=False)

    dpvs = relationship(
        'DistributePhysicalVolume',
        back_populates='dvg',
        lazy='dynamic')

    dlvs = relationship(
        'DistributeLogicalVolume',
        back_populates='dvg',
        lazy='dynamic')


class DlvStatus(enum.Enum):
    creating = 'creating'
    available = 'available'
    attaching = 'attaching'
    attached = 'attached'
    detaching = 'detaching'
    deleting = 'deleting'


class DistributeLogicalVolume(Base):

    __tablename__ = 'distribute_logical_volume'

    dlv_name = Column(String(32), primary_key=True)

    dlv_size = Column(BigInteger, nullable=False)

    data_size = Column(BigInteger, nullable=False)

    stripe_number = Column(Integer, nullable=False)

    status = Column(Enum(DlvStatus, name='dlv_status'), nullable=False)

    dvg_name = Column(
        String(32),
        ForeignKey('distribute_volume_group.dvg_name'),
        nullable=False)

    dvg = relationship('DistributeVolumeGroup', back_populates='dlvs')

    ihost_name = Column(String(32), ForeignKey('initiator_host.ihost_name'))

    ihost = relationship('InitiatorHost', back_populates='dlvs')

    snapshots = relationship(
        'Snapshot',
        back_populates='dlv',
        foreign_keys='[Snapshot.dlv_name]')

    active_snap_name = Column(
        String(64), ForeignKey('snapshot.snap_name'), nullable=False)

    active_snap = relationship('Snapshot', foreign_keys=[active_snap_name])

    groups = relationship('Group', back_populates='dlv')

    fjs = relationship('FailoverJob', back_populates='dlv')

    src_cjs = relationship(
        'CloneJob',
        foreign_keys='[CloneJob.src_dlv_name]',
        back_populates='src_dlv')

    dst_cj = relationship(
        'CloneJob',
        foreign_keys='[CloneJob.dst_dlv_name]',
        uselist=False,
        back_populates='src_dlv')

    lock_id = Column(BigInteger, ForeignKey('lock.lock_id'))

    lock = relationship('Lock')


class IhostStatus(enum.Enum):
    available = 'available'
    unavailable = 'unavailable'


class InitiatorHost(Base):

    __tablename__ = 'initiator_host'

    ihost_name = Column(String(64), primary_key=True)

    status = Column(Enum(IhostStatus, name='ihost_status'), nullable=False)

    dlvs = relationship('DistributeLogicalVolume', back_populates='ihost')


class SnapStatus(enum.Enum):
    creating = 'creating'
    deleting = 'deleting'
    available = 'available'
    failed = 'failed'


class Snapshot(Base):

    __tablename__ = 'snapshot'

    snap_name = Column(String(64), primary_key=True)

    thin_id = Column(Integer, nullable=False)

    ori_thin_id = Column(Integer, nullable=False)

    status = Column(Enum(SnapStatus, name='snap_status'), nullable=False)

    dlv_name = Column(
        String(32),
        ForeignKey('distribute_logical_volume.dlv_name'),
        nullable=False)

    dlv = relationship(
        'DistributeLogicalVolume',
        foreign_keys=[dlv_name],
        back_populates='snapshots')


class Group(Base):

    __tablename__ = 'group'

    group_id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True, autoincrement=True)

    group_idx = Column(Integer, nullable=False)

    group_size = Column(BigInteger, nullable=False)

    dlv_name = Column(
        String(32), ForeignKey('distribute_logical_volume.dlv_name'))

    dlv = relationship('DistributeLogicalVolume', back_populates='groups')

    legs = relationship('Leg', back_populates='group')


class Leg(Base):

    __tablename__ = 'leg'

    leg_id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True, autoincrement=True)

    leg_idx = Column(Integer)

    leg_size = Column(BigInteger, nullable=False)

    group_id = Column(BigInteger, ForeignKey('group.group_id'))

    group = relationship('Group', back_populates='legs')

    dpv_name = Column(
        String(32),
        ForeignKey('distribute_physical_volume.dpv_name'))

    dpv = relationship('DistributePhysicalVolume', back_populates='legs')

    ori_fj = relationship(
        'FailoverJob', foreign_keys='[FailoverJob.ori_leg_id]',
        uselist=False, back_populates='ori_leg')

    src_fj = relationship(
        'FailoverJob', foreign_keys='[FailoverJob.src_leg_id]',
        uselist=False, back_populates='src_leg')

    dst_fj = relationship(
        'FailoverJob', foreign_keys='[FailoverJob.dst_leg_id]',
        uselist=False, back_populates='dst_leg')


class FailoverJob(Base):

    __tablename__ = 'failover_job'

    fj_name = Column(String(32), primary_key=True)

    dlv_name = Column(
        String(32), ForeignKey('distribute_logical_volume.dlv_name'))

    dlv = relationship('DistributeLogicalVolume', back_populates='fjs')

    ori_leg_id = Column(BigInteger, ForeignKey('leg.leg_id'))

    src_leg_id = Column(BigInteger, ForeignKey('leg.leg_id'))

    dst_leg_id = Column(BigInteger, ForeignKey('leg.leg_id'))

    ori_leg = relationship(
        'Leg', foreign_keys=[ori_leg_id], back_populates='ori_fj')

    src_leg = relationship(
        'Leg', foreign_keys=[src_leg_id], back_populates='src_fj')

    dst_leg = relationship(
        'Leg', foreign_keys=[dst_leg_id], back_populates='dst_fj')

    lock_id = Column(BigInteger, ForeignKey('lock.lock_id'))

    lock = relationship('Lock')


class CloneJob(Base):

    __tablename__ = 'clone_job'

    cj_name = Column(String(32), primary_key=True)

    src_dlv_name = Column(
        String(32), ForeignKey('distribute_logical_volume.dlv_name'))

    dst_dlv_name = Column(
        String(32), ForeignKey('distribute_logical_volume.dlv_name'))

    src_dlv = relationship(
        'DistributeLogicalVolume',
        foreign_keys=[src_dlv_name],
        back_populates='src_cjs')

    dst_dlv = relationship(
        'DistributeLogicalVolume',
        foreign_keys=[dst_dlv_name],
        back_populates='dst_cj')


class LockType(enum.Enum):
    dlv = 'dlv'
    dpv = 'dpv'
    fj = 'fj'
    cj = 'cj'


class Lock(Base):

    __tablename__ = 'lock'

    lock_id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True, autoincrement=True)

    lock_owner = Column(
        String(32), nullable=False)

    lock_type = Column(
        Enum(LockType, name='lock_type'),
        nullable=False)

    lock_time = Column(DateTime, nullable=False)
