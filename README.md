# Introduction

The `csm-api-client` package is a Python client library for accessing
Cray System Management APIs.

## Installation as a dependency

`csm-api-client` can be installed as a dependency in your Python project:

```
export PIP_EXTRA_INDEX_URL="https://artifactory.algol60.net/artifactory/csm-python-modules/simple"
pip install csm-api-client
```

## Local installation with Poetry

This project uses `poetry` to manage dependencies and development environments.
For more information and installation instructions, see the [Poetry project
website](https://python-poetry.org/).

On macOS, using the system-provided Python installation can cause problems when
installing `poetry`. To avoid them, it is recommended to use a third-party
Python distribution instead. There are a few options for installing a
third-party Python distribution.

* Use a package manager such as Homebrew, Macports, or Nix to install a
  third-party Python distribution. For instance, `brew install python` can be
  used to install the latest version of Python from Homebrew's package
  repository.
* Use a tool such as `pyenv` to manage Python versions and installations. See
  [the `pyenv` project page](https://github.com/pyenv/pyenv/) for more details.

After installing the proper Python distribution, follow [the `poetry`
installation
instructions](https://python-poetry.org/docs/#installing-with-the-official-installer)
to install `poetry`.

To install `csm-api-client` in a local virtualenv for development:

```
$ poetry install
```

To create a release distribution tarball and wheel:

```
$ poetry build
```

## Local installation with pip

The `csm-api-client` library may also be installed with pip:

```
$ pip install python-csm-api-client/
```

# Copying

(C) Copyright 2019-2022 Hewlett Packard Enterprise Development LP.

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
