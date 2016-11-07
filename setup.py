#!/usr/bin/env python

from setuptools import setup, find_packages

import dlvm


setup_options = dict(
    name='dlvm',
    version=dlvm.__version__,
    description='a distribute storage system base on lvm and iscsi',
    long_description=open('README.rst').read(),
    author='yupeng',
    url='https://github.com/dlvm/dlvm',
    scripts=['bin/dlvm_manage_db'],
    packages=find_packages(exclude=['tests*']),
    data_files=[
        ('/etc/dlvm', [
            'data/dlvm.conf',
            'data/logger.yml',
        ])
    ],
    license="Apache License 2.0",
    classifiers=(
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
    ),
)

setup(**setup_options)
