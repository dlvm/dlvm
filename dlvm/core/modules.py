#!/usr/bin/env python

from typing import Union

from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Integer, String, \
    Enum, ForeignKey

FieldType = Union[int, str, None]

Base = declarative_base()


class DistributePhysicalVolume(Base):  # type: ignore

    __tablename__ = 'distribute_physical_volume'

    dpv_name = Column(
        String(32), primary_key=True)

    total_size = Column(
        BigInteger, nullable=False)

    free_size = Column(
        BigInteger, nullable=False)

    status = Column(
        Enum('available', 'unavailable', name='dpv_status'),
        nullable=False)

    dvg_name = Column(
        String(32),
        ForeignKey('distribute_volume_group.dvg_name'))

    dvg = relationship(
        'DistributeVolumeGroup', back_populates='dpvs')

    legs = relationship(
        'Leg', back_populates='dpv')

    lock_id = Column(String(32))

    lock_timestamp = Column(BigInteger)


class DistributeVolumeGroup(Base):  # type: ignore

    __tablename__ = 'distribute_volume_group'

    dvg_name = Column(
        String(32), primary_key=True)

    total_size = Column(
        BigInteger, nullable=False)

    free_size = Column(
        BigInteger, nullable=False)

    dpvs = relationship(
        'DistributePhysicalVolume',
        back_populates='dvg', lazy='dynamic')

    dlvs = relationship(
        'DistributeLogicalVolume',
        back_populates='dvg', lazy='dynamic')


class DistributeLogicalVolume(Base):  # type: ignore

    __tablename__ = 'distribute_logical_volume'

    dlv_name = Column(
        String(32), primary_key=True)

    dlv_size = Column(
        BigInteger, nullable=False)

    data_size = Column(
        BigInteger, nullable=False)

    stripe_number = Column(
        Integer, nullable=False)

    status = Column(
        Enum(
            'creating', 'available',
            'attaching', 'attached',
            'detaching', 'deleting',
            name='dlv_status'),
        nullable=False)

    dvg_name = Column(
        String(32),
        ForeignKey('distribute_volume_group.dvg_name'),
        nullable=False)

    dvg = relationship(
        'DistributeVolumeGroup', back_populates='dlvs')

    ihost_name = Column(
        String(32), ForeignKey('initiator_host.ihost_name'))

    ihost = relationship(
        'InitiatorHost', back_populates='dlvs')

    snapshots = relationship(
        'Snapshot', back_populates='dlv')

    active_snap_name = Column(
        String(64), nullable=False)

    groups = relationship(
        'Group', back_populates='dlv')

    lock_id = Column(String(32))

    lock_timestamp = Column(BigInteger)


class InitiatorHost(Base):  # type: ignore

    __tablename__ = 'initiator_host'

    ihost_name = Column(
        String(32), primary_key=True)

    status = Column(
        Enum('available', 'unavailable', name='ihost_status'),
        nullable=False)

    dlvs = relationship(
        'DistributeLogicalVolume', back_populates='ihost')

    lock_id = Column(String(32))

    lock_timestamp = Column(BigInteger)


class Snapshot(Base):  # type: ignore

    __tablename__ = 'snapshot'

    snap_name = Column(
        String(64), primary_key=True)

    thin_id = Column(
        Integer, nullable=False)

    ori_thin_id = Column(
        Integer, nullable=False)

    status = Column(
        Enum(
            'creating', 'deleting',
            'available', 'failed',
            name='snap_status'),
        nullable=False)

    dlv_name = Column(
        String(32),
        ForeignKey('distribute_logical_volume.dlv_name'),
        nullable=False)

    dlv = relationship(
        'DistributeLogicalVolume', back_populates='snapshots')


class Group(Base):  # type: ignore

    __tablename__ = 'group'

    group_id = Column(
        String(32), primary_key=True)

    idx = Column(
        Integer, nullable=False)

    group_size = Column(
        BigInteger, nullable=False)

    dlv_name = Column(
        String(32), ForeignKey('distribute_logical_volume.dlv_name'))

    dlv = relationship(
        'DistributeLogicalVolume', back_populates='groups')

    legs = relationship(
        'Leg', back_populates='group')


class Leg(Base):  # type: ignore

    __tablename__ = 'leg'

    leg_id = Column(
        String(32), primary_key=True)

    idx = Column(Integer)

    leg_size = Column(
        BigInteger, nullable=False)

    group_id = Column(
        String(32), ForeignKey('group.group_id'))

    group = relationship(
        'Group', back_populates='legs')

    dpv_name = Column(
        String(32), ForeignKey('distribute_physical_volume.dpv_name'))

    dpv = relationship(
        'DistributePhysicalVolume', back_populates='legs')
