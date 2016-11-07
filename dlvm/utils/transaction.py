#!/usr/bin/env python

from logging import getLogger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from configure import conf

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


def get_context():
    dpv_engine = create_engine(conf.dpv_transaction_db)
    DpvSession = sessionmaker(bind=dpv_engine)
    host_engine = create_engine(conf.host_transaction_db)
    HostSession = sessionmaker(bind=host_engine)

    context = {
        'dpv': {
            'engine': dpv_engine,
            'Session': DpvSession,
            'default_major': None,
            'major_file': conf.dpv_major_file,
            'logger': getLogger('dlvm_dpv'),
        },
        'host': {
            'engine': host_engine,
            'Session': HostSession,
            'default_major': None,
            'major_file': conf.host_major_file,
            'logger': getLogger('host_dpv'),
        },
    }
    return context


def init_transaction_db(role, major):
    context = get_context()
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
    context = get_context()
    do_verify(name, major, minor, context['host'])


def dpv_verify(name, major, minor):
    context = get_context()
    do_verify(name, major, minor, context['dpv'])
