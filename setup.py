#!/usr/bin/env python

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='dlvm',
    version='0.5.0',
    description='a distribute storage system base on lvm and iscsi',
    long_description=long_description,
    url='https://github.com/dlvm/dlvm',
    author='yupeng',
    author_email='yupeng0921@gmail.com',
    license='MIT',
    classifiers=(
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ),
    keywords='dlvm storage',
    python_requires='>=3.6',
    install_requires=[
        'Flask',
        'marshmallow>=3.0.0b1',
        'SQLAlchemy',
        'celery',
    ],
    packages=find_packages(exclude=['tests*']),
    package_data={
        'dlvm': [
            'common/default.cfg',
            'common/thin_mapping_template.xml'
        ],
    },
    data_files=[(
        'bin/dlvm_bin', [
            'binary/pdata_tools',
            'binary/dm_dd',
        ],
    )],
    entry_points={
        'console_scripts': [
            'dlvm_init_db=dlvm.common.database:create_all',
            'dlvm_dpv_agent=dlvm.dpv_agent:start_dpv_agent',
            'dlvm_ihost_agent=dlvm.ihost_agent:start_ihost_agent',
        ]
    }
)
