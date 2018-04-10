#!/bin/bash

if [ "$1" != "" ]; then
   flake8_inp=$1
   mypy_inp=$1
else
    flake8_inp="."
    mypy_inp="./dlvm"
fi
flake8 --ignore=F401,E226 ${flake8_inp}
[ $? -eq 0 ] || exit 1
mypy --ignore-missing-imports --strict-optional --disallow-untyped-defs --disallow-untyped-calls --disallow-incomplete-defs ${mypy_inp}
[ $? -eq 0 ] || exit 1

exit 0
