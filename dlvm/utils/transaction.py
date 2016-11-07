#!/usr/bin/env python

from logging import get_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from configure import conf

dpv_major_file = conf.get('defalt', 'dpv_major_file')
dpv_transaction_db = conf.get('default', 'dpv_transaction_db')
host_major_file = conf.get('defalt', 'host_major_file')
host_transaction_db = conf.get('default', 'host_transaction_db')

conflict_error = 'TransactionConflict'

Base = declarative_base()


class Transaction(Base):
    __tablename__ = 'transaction'

    name = Column(String, primary_key=True)
    major = Column(Integer)
    minor = Column(Integer)

    def __repr__(self):
        return 'Transaction(name=%s major=%s minor=%s)' % (
            self.name, self.major, self.minor)


dpv_engine = create_engine(dpv_transaction_db)
DpvSession = sessionmaker(bind=dpv_engine)
host_engine = create_engine(host_transaction_db)
HostSession = sessionmaker(bind=host_engine)

context = {
    'dpv': {
        'engine': dpv_engine,
        'Session': DpvSession,
        'default_major': None,
        'major_file': dpv_major_file,
        'logger': get_logger('dlvm_dpv'),
    },
    'host': {
        'engine': host_engine,
        'Session': HostSession,
        'default_major': None,
        'major_file': host_major_file,
        'logger': get_logger('host_dpv'),
    },
}


def init_transaction_db(role, major):
    c = context[role]
    Base.metadata.create_all(c['engine'])
    c['default_major'] = major
    with open(c['major_file'], 'w') as f:
        f.write(str(major))


def get_default_major(c):
    if c['default_major'] is None:
        with open(c['major_file']) as f:
            c['default_major'] = int(f.read().strip())


def _do_verify(
        name, major, minor, default_major, session, logger):
    q = session.query(Transaction).filter_by(name=name)
    ret = session.query(q.exists()).scalar()
    if ret:
        t = q.one()
    else:
        t = Transaction(
            name=name,
            major=default_major,
            minor=0,
        )
    if t.major > major:
        logger.warning('major conflict: %s %d', t, major)
        raise Exception(conflict_error)
    elif t.major == major:
        if t.minor >= minor:
            logger.warning('minor conflict: %s %d', t, minor)
            raise Exception(conflict_error)

    t.major = major
    t.minor = minor
    session.add(t)
    session.commit()


def do_verify(name, major, minor, c):
    get_default_major(c)
    session = c['Session']()
    try:
        _do_verify(
            name,
            major,
            minor,
            c['default_major'],
            session,
            c['logger'],
        )
    except:
        session.rollback()
        raise
    finally:
        session.close()


def host_verify(name, major, minor):
    do_verify(name, major, minor, context['host'])


def dpv_verify(name, major, minor):
    do_verify(name, major, minor, context['dpv'])
