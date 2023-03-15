#!/usr/bin/env bash

set -e

if [[ "$VIRTUAL_ENV" == "" ]]
then
    echo "You're not in a virtualenv! Set that up first."
    exit 1
fi

poetry run pytest
