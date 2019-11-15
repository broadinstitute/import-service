#!/usr/bin/env bash

if [[ "$VIRTUAL_ENV" == "" ]]
then
    echo "You're not in a virtualenv! Set that up first."
    exit 1
fi

python3 -m mypy *.py
python3 -m mypy -p functions
