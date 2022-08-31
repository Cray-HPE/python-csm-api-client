#
# MIT License
#
# (C) Copyright 2022 Hewlett Packard Enterprise Development LP
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
"""
Utility functions for interacting with Kubernetes.
"""

import warnings

from kubernetes.client import CoreV1Api
from kubernetes.config import load_kube_config
from kubernetes.config.config_exception import ConfigException
# YAMLLoadWarning is not defined in types-PyYAML.
from yaml import YAMLLoadWarning  # type: ignore[attr-defined]


def load_kube_api() -> CoreV1Api:
    """Get a Kubernetes CoreV1Api object.

    This helper function loads the kube config and then instantiates
    an API object.

    Returns:
        The API object from the kubernetes library.

    Raises:
        kubernetes.config.config_exception.ConfigException: if failed to load
            kubernetes configuration.
    """
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=YAMLLoadWarning)
            load_kube_config()
    # Earlier versions: FileNotFoundError; later versions: ConfigException
    except (FileNotFoundError, ConfigException) as err:
        raise ConfigException(
            'Failed to load kubernetes config to get pod status: '
            '{}'.format(err)
        )

    return CoreV1Api()
