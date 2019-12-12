#!/usr/bin/env bash

set -e

if [[ "$VIRTUAL_ENV" == "" ]] && [[ "$CI" == "" ]]
then
    echo "You're not in a virtualenv! Set that up first."
    exit 1
fi

python3 -m pytest
