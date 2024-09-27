#!/usr/bin/env bash
#
# MIT License
#
# (C) Copyright 2022-2024 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

VENV_PATH="/tmp/poetryvenv"

function in_venv() {
    if ! [[ -d ${VENV_PATH} ]]; then
        /usr/bin/python3 -m venv "$VENV_PATH"
    fi

    # Execute the command directly within the venv
    "${VENV_PATH}/bin/$@"
}

function prep() {
    in_venv pip install -U pip setuptools
    in_venv pip install "poetry==1.8.3"
    in_venv poetry install
}

function unittest() {
    in_venv poetry run nose2
}

function python_package() {
    in_venv poetry build
}

function typecheck() {
    in_venv poetry run mypy --implicit-optional
}

function main() {
    if [[ $# -ne 1 ]]; then
        echo "Must use exactly one argument, try \"prep\", \"unittest\", \"typecheck\", or \"python_package\""
        exit 1
    fi
    COMMAND="$1"
    if [[ $(type -t "${COMMAND}") -ne "function" ]]; then
        echo "operation \"$1\" not supported, try \"prep\", \"unittest\", \"typecheck\",  or \"python_package\""
        exit 1
    fi

    ${COMMAND}
}

main "$@"
