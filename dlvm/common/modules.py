import enum

from sqlalchemy import Column, BigInteger, Integer, String, \
    DateTime, Enum, Binary, Boolean, Text, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from dlvm.common.constant import RES_NAME_LENGTH, DNS_NAME_LENGTH, MAX_BM_SIZE

Base = declarative_base()


class ServiceStatus(enum.Enum):
    available = 'available'
    unavailable = 'unavailable'


class DiskStatus(enum.Enum):
    available = 'available'
    unavailable = 'unavailable'


class DistributePhysicalVolume(Base):

    __tablename__ = 'distribute_physical_volume'

    dpv_name = Column(String(DNS_NAME_LENGTH), primary_key=True)

    total_size = Column(BigInteger, nullable=False)

    free_size = Column(BigInteger, nullable=False)

    service_status = Column(
        Enum(ServiceStatus, name='dpv_service_status'), nullable=False)

    disk_status = Column(
        Enum(DiskStatus, name='dpv_disk_status'), nullable=False)

    dvg_name = Column(
        String(RES_NAME_LENGTH),
        ForeignKey('distribute_volume_group.dvg_name'))

    dvg = relationship('DistributeVolumeGroup', back_populates='dpvs')

    legs = relationship('Leg', back_populates='dpv')

    lock_id = Column(BigInteger, ForeignKey('dlvm_lock.lock_id'))

    lock = relationship('Lock')


class DistributeVolumeGroup(Base):

    __tablename__ = 'distribute_volume_group'

    dvg_name = Column(String(RES_NAME_LENGTH), primary_key=True)

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

    dlv_name = Column(String(RES_NAME_LENGTH), primary_key=True)

    dlv_size = Column(BigInteger, nullable=False)

    data_size = Column(BigInteger, nullable=False)

    stripe_number = Column(Integer, nullable=False)

    status = Column(Enum(DlvStatus, name='dlv_status'), nullable=False)

    bm_dirty = Column(Boolean, nullable=False)

    bm_ignore = Column(Boolean, nullable=False)

    dvg_name = Column(
        String(RES_NAME_LENGTH),
        ForeignKey('distribute_volume_group.dvg_name'),
        nullable=False)

    dvg = relationship('DistributeVolumeGroup', back_populates='dlvs')

    ihost_name = Column(String(DNS_NAME_LENGTH))

    snapshots = relationship(
        'Snapshot',
        back_populates='dlv')

    active_snap_name = Column(String(RES_NAME_LENGTH), nullable=False)

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

    lock_id = Column(BigInteger, ForeignKey('dlvm_lock.lock_id'))

    lock = relationship('Lock')


class SnapStatus(enum.Enum):
    creating = 'creating'
    deleting = 'deleting'
    available = 'available'
    failed = 'failed'


class Snapshot(Base):

    __tablename__ = 'snapshot'

    snap_id = Column(String(2*RES_NAME_LENGTH+1), primary_key=True)

    snap_name = Column(String(RES_NAME_LENGTH), nullable=False)

    thin_id = Column(Integer, nullable=False)

    ori_thin_id = Column(Integer, nullable=False)

    status = Column(Enum(SnapStatus, name='snap_status'), nullable=False)

    thin_mapping = Column(Text, nullable=False)

    dlv_name = Column(
        String(RES_NAME_LENGTH),
        ForeignKey('distribute_logical_volume.dlv_name'),
        nullable=False)

    dlv = relationship(
        'DistributeLogicalVolume',
        back_populates='snapshots')


class Group(Base):

    __tablename__ = 'dlv_group'

    group_id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True, autoincrement=True)

    group_idx = Column(Integer, nullable=False)

    group_size = Column(BigInteger, nullable=False)

    bitmap = Column(Binary(MAX_BM_SIZE), nullable=False)

    dlv_name = Column(
        String(RES_NAME_LENGTH),
        ForeignKey('distribute_logical_volume.dlv_name'))

    dlv = relationship('DistributeLogicalVolume', back_populates='groups')

    legs = relationship('Leg', back_populates='group')


class Leg(Base):

    __tablename__ = 'leg'

    leg_id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True, autoincrement=True)

    leg_idx = Column(Integer)

    leg_size = Column(BigInteger, nullable=False)

    group_id = Column(BigInteger, ForeignKey('dlv_group.group_id'))

    group = relationship('Group', back_populates='legs')

    dpv_name = Column(
        String(DNS_NAME_LENGTH),
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

    fj_name = Column(String(RES_NAME_LENGTH), primary_key=True)

    dlv_name = Column(
        String(RES_NAME_LENGTH),
        ForeignKey('distribute_logical_volume.dlv_name'))

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

    lock_id = Column(BigInteger, ForeignKey('dlvm_lock.lock_id'))

    lock = relationship('Lock')


class CloneJob(Base):

    __tablename__ = 'clone_job'

    cj_name = Column(String(RES_NAME_LENGTH), primary_key=True)

    src_dlv_name = Column(
        String(RES_NAME_LENGTH),
        ForeignKey('distribute_logical_volume.dlv_name'))

    dst_dlv_name = Column(
        String(RES_NAME_LENGTH),
        ForeignKey('distribute_logical_volume.dlv_name'))

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
    ej = 'ej'


class Lock(Base):

    __tablename__ = 'dlvm_lock'

    lock_id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True, autoincrement=True)

    lock_owner = Column(
        String(RES_NAME_LENGTH), nullable=False)

    lock_type = Column(
        Enum(LockType, name='lock_type'),
        nullable=False)

    lock_dt = Column(DateTime, nullable=False)
