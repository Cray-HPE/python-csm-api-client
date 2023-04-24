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
Tests for kubernetes utility functions.
"""

import unittest
from unittest.mock import patch, MagicMock

from kubernetes.config import ConfigException

from csm_api_client.k8s import load_kube_api


class TestLoadKubeApi(unittest.TestCase):
    """Tests for the load_kube_api() helper function"""

    def setUp(self) -> None:
        self.mock_k8s_incluster_config = \
            patch('csm_api_client.k8s.load_incluster_config').start()

        self.mock_k8s_config_from_disk = \
            patch('csm_api_client.k8s.load_kube_config').start()

        self.mock_k8s_api_cls = MagicMock()

    def tearDown(self) -> None:
        patch.stopall()

    def test_load_incluster_config(self):
        """Test loading the config inside the k8s cluster"""
        for file_exc_type in [FileNotFoundError, ConfigException]:
            self.mock_k8s_config_from_disk.side_effect = file_exc_type
            load_kube_api(api_cls=self.mock_k8s_api_cls)

            self.mock_k8s_api_cls.assert_called()
            self.mock_k8s_incluster_config.assert_called()
            self.mock_k8s_config_from_disk.assert_not_called()

    def test_load_config_from_disk(self):
        """Test loading the config outside the cluster (i.e. from disk)"""
        self.mock_k8s_incluster_config.side_effect = ConfigException
        load_kube_api(api_cls=self.mock_k8s_api_cls)
        self.mock_k8s_api_cls.assert_called_once()
        self.mock_k8s_config_from_disk.assert_called_once()

    def test_incluster_higher_precedence(self):
        """Test that the in-cluster is loaded before the config file"""
        load_kube_api(api_cls=self.mock_k8s_api_cls)
        self.mock_k8s_api_cls.assert_called_once()
        self.mock_k8s_incluster_config.assert_called_once()
        self.mock_k8s_config_from_disk.assert_not_called()

    def test_exception_when_cant_load(self):
        """Test that a ConfigException is raised when the config can't be loaded"""
        for exc_type in [FileNotFoundError, ConfigException]:
            self.mock_k8s_config_from_disk.side_effect = exc_type
            self.mock_k8s_incluster_config.side_effect = ConfigException
            with self.assertRaises(ConfigException):
                load_kube_api(api_cls=self.mock_k8s_api_cls)
            self.mock_k8s_api_cls.assert_not_called()
