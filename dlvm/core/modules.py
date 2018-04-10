from typing import Any, Union, Optional, Sequence

import enum
import uuid

from sqlalchemy import Column, BigInteger, Integer, String, \
    Enum, ForeignKey
from sqlalchemy.types import TypeDecorator, CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID


FieldType = Union[int, str, enum.Enum, None]

Base: Any = declarative_base()


class GUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.

    """
    impl = CHAR

    def load_dialect_impl(self, dialect):  # type: ignore
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):  # type: ignore
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):  # type: ignore
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value


class DpvStatus(enum.Enum):
    available = 'available'
    unavailable = 'unavailable'


class DistributePhysicalVolume(Base):

    __tablename__ = 'distribute_physical_volume'

    dpv_name = Column(
        String(32), primary_key=True)  # type: str

    total_size = Column(
        BigInteger, nullable=False)  # type: int

    free_size = Column(
        BigInteger, nullable=False)  # type: int

    status = Column(
        Enum(DpvStatus, name='dpv_status'),
        nullable=False)  # type: DpvStatus

    dvg_name = Column(
        String(32),
        ForeignKey('distribute_volume_group.dvg_name'))  # type: Optional[str]

    dvg = relationship(
        'DistributeVolumeGroup',
        back_populates='dpvs')  # type: Optional[DistributeVolumeGroup]

    legs = relationship(
        'Leg', back_populates='dpv')  # type: Sequence[Leg]

    lock_id = Column(String(32))  # type: Optional[str]

    lock_timestamp = Column(BigInteger)  # type: Optional[int]


class DistributeVolumeGroup(Base):

    __tablename__ = 'distribute_volume_group'

    dvg_name = Column(
        String(32), primary_key=True)  # type: str

    total_size = Column(
        BigInteger, nullable=False)  # type: int

    free_size = Column(
        BigInteger, nullable=False)  # type: int

    dpvs = relationship(
        'DistributePhysicalVolume',
        back_populates='dvg',
        lazy='dynamic')  # type: object

    dlvs = relationship(
        'DistributeLogicalVolume',
        back_populates='dvg',
        lazy='dynamic')  # type: object


class DlvStatus(enum.Enum):
    creating = 'creating'
    available = 'available'
    attaching = 'attaching'
    attached = 'attached'
    detaching = 'detaching'
    deleting = 'deleting'


class DistributeLogicalVolume(Base):

    __tablename__ = 'distribute_logical_volume'

    dlv_name = Column(
        String(32), primary_key=True)  # type: str

    dlv_size = Column(
        BigInteger, nullable=False)  # type: int

    data_size = Column(
        BigInteger, nullable=False)  # type: int

    stripe_number = Column(
        Integer, nullable=False)  # type: int

    status = Column(
        Enum(DlvStatus, name='dlv_status'),
        nullable=False)  # type: str

    dvg_name = Column(
        String(32),
        ForeignKey('distribute_volume_group.dvg_name'),
        nullable=False)  # type: str

    dvg = relationship(
        'DistributeVolumeGroup',
        back_populates='dlvs')  # type: DistributeVolumeGroup

    ihost_name = Column(
        String(32),
        ForeignKey('initiator_host.ihost_name'))  # type: Optional[str]

    ihost = relationship(
        'InitiatorHost',
        back_populates='dlvs')  # type: Optional[InitiatorHost]

    snapshots = relationship(
        'Snapshot',
        back_populates='dlv')  # type: Sequence[Snapshot]

    active_snap_name = Column(
        String(64), nullable=False)  # type: str

    groups = relationship(
        'Group',
        back_populates='dlv')  # type: Sequence[Group]

    lock_id = Column(String(32))  # type: Optional[str]

    lock_timestamp = Column(BigInteger)  # type: Optional[int]


class IhostStatus(enum.Enum):
    available = 'available'
    unavailable = 'unavailable'


class InitiatorHost(Base):

    __tablename__ = 'initiator_host'

    ihost_name = Column(
        String(32), primary_key=True)  # type: str

    status = Column(
        Enum(IhostStatus, name='ihost_status'),
        nullable=False)  # type: IhostStatus

    dlvs = relationship(
        'DistributeLogicalVolume',
        back_populates='ihost')  # type: Sequence[DistributeLogicalVolume]

    lock_id = Column(String(32))  # type: Optional[str]

    lock_timestamp = Column(BigInteger)  # type: Optional[int]


class SnapStatus(enum.Enum):
    creating = 'creating'
    deleting = 'deleting'
    available = 'available'
    failed = 'failed'


class Snapshot(Base):

    __tablename__ = 'snapshot'

    snap_name = Column(
        String(64), primary_key=True)  # type: str

    thin_id = Column(
        Integer, nullable=False)  # type: int

    ori_thin_id = Column(
        Integer, nullable=False)  # type: int

    status = Column(
        Enum(SnapStatus, name='snap_status'),
        nullable=False)  # type: str

    dlv_name = Column(
        String(32),
        ForeignKey('distribute_logical_volume.dlv_name'),
        nullable=False)  # type: str

    dlv = relationship(
        'DistributeLogicalVolume',
        back_populates='snapshots')  # type: DistributeLogicalVolume


class Group(Base):

    __tablename__ = 'group'

    group_id = Column(
        GUID, primary_key=True)  # type: uuid.UUID

    group_idx = Column(
        Integer, nullable=False)  # type: int

    group_size = Column(
        BigInteger, nullable=False)  # type: int

    dlv_name = Column(
        String(32),
        ForeignKey('distribute_logical_volume.dlv_name'),
    )  # type: Optional[str]

    dlv = relationship(
        'DistributeLogicalVolume',
        back_populates='groups')  # type: Optional[DistributeLogicalVolume]

    legs = relationship(
        'Leg', back_populates='group')  # type: Sequence[Leg]


class Leg(Base):

    __tablename__ = 'leg'

    leg_id = Column(
        GUID, primary_key=True)  # type: uuid.UUID

    leg_idx = Column(Integer)  # type: int

    leg_size = Column(
        BigInteger, nullable=False)  # type: int

    group_id = Column(
        String(32), ForeignKey('group.group_id'))  # type: Optional[str]

    group = relationship(
        'Group', back_populates='legs')  # type: Optional[Group]

    dpv_name = Column(
        String(32),
        ForeignKey('distribute_physical_volume.dpv_name'),
    )  # type: Optional[str]

    dpv = relationship(
        'DistributePhysicalVolume',
        back_populates='legs')  # type: Optional[DistributePhysicalVolume]
