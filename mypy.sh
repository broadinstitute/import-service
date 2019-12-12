#!/usr/bin/env bash

set -e

if [[ "$VIRTUAL_ENV" == "" ]]
then
    echo "You're not in a virtualenv! Set that up first."
    exit 1
fi


# There's no way to say "scan all the Python files in this project". You might think to do:
#       (shopt -s globstar; ls **/*.py)
# but that gives you everything in the venv too.
# So we have to make two calls to cover everything:
#   1. scan the .py files at the root of the repo
#   2. scan the entire "functions" package

# Unfortunately, mypy REALLY likes to follow imports.
# https://mypy.readthedocs.io/en/latest/running_mypy.html#following-imports
# So this means that errors in files that are imported by BOTH the root-files and the functions-files will
# show up twice. The good news is that you only have to fix them once ;)
python3 -m mypy *.py
python3 -m mypy -p functions
