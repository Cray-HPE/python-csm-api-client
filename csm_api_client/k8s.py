#
# MIT License
#
# (C) Copyright 2022-2023 Hewlett Packard Enterprise Development LP
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

import logging
from typing import Any, Type, TypeVar

from kubernetes.client import CoreV1Api
from kubernetes.config.config_exception import ConfigException
from kubernetes.config.kube_config import load_kube_config
from kubernetes.config.incluster_config import load_incluster_config


LOGGER = logging.getLogger(__name__)


T = TypeVar("T")


def load_kube_api(api_cls: Type[T] = CoreV1Api, **kwargs: Any) -> T:
    """Get a Kubernetes CoreV1Api object.

    This helper function loads the kubeconfig and then instantiates
    an API object. It will first attempt to load the configuration
    from the in-cluster config, if available, and will fall back to
    the config file if that doesn't work for any reason. Note that this
    order is the reverse of that in the `kubernetes.config.load_config()`
    helper function.

    Args:
        api_cls: the type of the API object to construct.

    Returns:
        The API object from the kubernetes library.

    Raises:
        kubernetes.config.config_exception.ConfigException: if failed to load
            kubernetes configuration.
    """
    try:
        load_incluster_config()
    except ConfigException as exc:
        LOGGER.debug("Couldn't load the in-cluster config: %s "
                     "(proceeding under the assumption that the config "
                     "should be loaded from the kubeconfig file)",
                     exc)
        try:
            load_kube_config()
        # Earlier versions: FileNotFoundError; later versions: ConfigException
        except (FileNotFoundError, ConfigException) as err:
            raise ConfigException(
                'Failed to load kubernetes config: {}'.format(err)
            ) from err

    return api_cls(**kwargs)
