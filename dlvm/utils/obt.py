#!/usr/bin/env python

import sys
from logging import getLogger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from configure import conf

conflict_error = 'ObtConflict'

Base = declarative_base()


class Obt(Base):
    __tablename__ = 'obt'

    name = Column(String, primary_key=True)
    major = Column(Integer)
    minor = Column(Integer)

    def __repr__(self):
        return 'Obt(name=%s major=%s minor=%s)' % (
            self.name, self.major, self.minor)


def obt_db(role, work_dir):
    return 'sqlite:///{work_dir}/{role}.db'.format(
        role=role, work_dir=work_dir)


def obt_major_file(role, work_dir):
    return '{work_dir}/{role}_major_file'.format(
        role=role, work_dir=work_dir)


def get_context():
    dpv_engine = create_engine(
        obt_db('dpv', conf.work_dir))
    DpvSession = sessionmaker(bind=dpv_engine)
    ihost_engine = create_engine(
        obt_db('ihost', conf.work_dir))
    IhostSession = sessionmaker(bind=ihost_engine)

    context = {
        'dpv': {
            'engine': dpv_engine,
            'Session': DpvSession,
            'default_major': None,
            'major_file': obt_major_file('dpv', conf.work_dir),
            'logger': getLogger('dlvm_dpv'),
        },
        'ihost': {
            'engine': ihost_engine,
            'Session': IhostSession,
            'default_major': None,
            'major_file': obt_major_file('ihost', conf.work_dir),
            'logger': getLogger('ihost_dpv'),
        },
    }
    return context


def init_obt_db(role, major):
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
    q = session.query(Obt).filter_by(name=name)
    ret = session.query(q.exists()).scalar()
    if ret:
        t = q.one()
    else:
        t = Obt(
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
            int(major),
            int(minor),
            c['default_major'],
            session,
            c['logger'],
        )
    except:
        session.rollback()
        raise
    finally:
        session.close()


def ihost_verify(name, major, minor):
    context = get_context()
    do_verify(name, major, minor, context['ihost'])


def dpv_verify(name, major, minor):
    context = get_context()
    do_verify(name, major, minor, context['dpv'])


def init_obt():
    role = sys.argv[1]
    major = sys.argv[2]
    init_obt_db(role, major)
