#!/usr/bin/env python

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='dlvm',
    version='0.4.0',
    description='a distribute storage system base on lvm and iscsi',
    long_description=long_description,
    url='https://github.com/dlvm/dlvm',
    author='yupeng',
    author_email='yupeng0921@gmail.com',
    license='MIT',
    install_requires=[
        'celery>=4.0.0',
        'Flask>=0.11.1',
        'Flask-RESTful>=0.3.5',
        'Flask-SQLAlchemy>=2.1',
        'PyYAML>=3.12',
        'SQLAlchemy>=1.1.3',
        'psycopg2>=2.7',
        'requests>=2.13.0',
    ],
    classifiers=(
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ),
    keywords='dlvm storage',
    packages=find_packages(exclude=['tests*']),
    package_data={
        'dlvm': [
            'data/conf.yml',
            'data/logger.yml',
            'client/api_format.json'
        ],
    },
    data_files=[
        ('bin', ['bin/dlvm_monitor_action.py']),
    ],
    entry_points={
        'console_scripts': [
            'dlvm_dpv_agent=dlvm.dpv_agent:main',
            'dlvm_ihost_agent=dlvm.ihost_agent:main',
            'dlvm_init_db=dlvm.api_server:init_db',
            'dlvm_init_obt=dlvm.utils.obt:init_obt',
            'dlvm_monitor=dlvm.monitor:start',
            'dlvm=dlvm.client.cli:main',
        ],
    },
)
