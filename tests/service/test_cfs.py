#
# MIT License
#
# (C) Copyright 2021-2024 Hewlett Packard Enterprise Development LP
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
"""
Tests for the CFS client library.
"""
import io
import json
import logging
from copy import deepcopy
import datetime
from typing import List
import unittest
from unittest.mock import Mock, call, patch, MagicMock

from cray_product_catalog.query import ProductCatalogError

from csm_api_client.service.cfs import (
    CFSClient,
    CFSConfiguration,
    CFSConfigurationError,
    CFSConfigurationLayer,
    CFSImageConfigurationSession,
    LayerState,
    MAX_BRANCH_NAME_WIDTH,
)
from csm_api_client.service.gateway import APIError
from csm_api_client.service.vcs import VCSError


class TestCFSConfigurationLayer(unittest.TestCase):
    """Tests for the CFSConfigurationLayer class."""

    def setUp(self):
        self.product = 'sat'
        self.repo_path = f'/vcs/cray/{self.product}-config-management.git'
        self.clone_url = f'http://api-gw-service-nmn.local{self.repo_path}'
        self.name = 'sat'
        self.playbook = 'sat-ncn.yml'
        self.commit = 'abcd1234'
        self.cfs_config_layer = CFSConfigurationLayer(self.clone_url, name=self.name,
                                                      playbook=self.playbook, commit=self.commit)

        self.mock_vcs_repo_cls = patch('csm_api_client.service.cfs.VCSRepo').start()
        self.mock_vcs_repo = self.mock_vcs_repo_cls.return_value

        self.mock_datetime = patch('csm_api_client.service.cfs.datetime', wraps=datetime.datetime).start()
        self.static_datetime = datetime.datetime(2022, 2, 19, 23, 12, 48)
        self.mock_datetime.now.return_value = self.static_datetime
        self.expected_timestamp = '20220219T231248'

    def tearDown(self):
        patch.stopall()

    def test_construct_cfs_config_layer(self):
        """Test creating a new CFSConfigurationLayer."""
        self.assertEqual(self.clone_url, self.cfs_config_layer.clone_url)
        self.assertEqual(self.name, self.cfs_config_layer.name)
        self.assertEqual(self.playbook, self.cfs_config_layer.playbook)
        self.assertEqual(self.commit, self.cfs_config_layer.commit)
        self.assertIsNone(self.cfs_config_layer.branch)

    def test_construct_cfs_configuration_layer_no_branch_commit(self):
        """Test creating a new CFSConfigurationLayer requires a git commit or branch."""
        with self.assertRaisesRegex(ValueError, 'Either commit or branch is required'):
            CFSConfigurationLayer(self.clone_url)

    def test_repo_path(self):
        """Test the repo_path property of CFSConfigurationLayer."""
        self.assertEqual(self.repo_path, self.cfs_config_layer.repo_path)

    def assert_matches(self, layer):
        """Assert the given layer matches `self.cfs_config_layer`.

        Args:
            layer (CFSConfigurationLayer): the other layer
        """
        # Test that matches method is symmetric
        self.assertTrue(self.cfs_config_layer.matches(layer))
        self.assertTrue(layer.matches(self.cfs_config_layer))

    def assert_does_not_match(self, layer):
        """Assert the given layer does not match `self.cfs_config_layer`.

        Args:
            layer: the other layer
        """
        self.assertFalse(self.cfs_config_layer.matches(layer))
        self.assertFalse(layer.matches(self.cfs_config_layer))

    def test_matches_matching_layer(self):
        """Test matches method against matching CFSConfigurationLayer."""
        matching_layer = CFSConfigurationLayer(self.clone_url, name='some-other-name',
                                               playbook=self.playbook, branch='foo')
        self.assert_matches(matching_layer)

    def test_matches_matching_layer_different_vcs_hostname(self):
        """Test matches method against matching CFSConfigurationLayer with different host in clone URL."""
        matching_layer = CFSConfigurationLayer(f'http://api-gw-service.nmn{self.repo_path}',
                                               name='matching-layer', playbook=self.playbook,
                                               commit='a1b2c3d')
        self.assert_matches(matching_layer)

    def test_matches_mismatch_different_playbook(self):
        """Test matches method against mismatched CFSConfigurationLayer with different playbook."""
        mismatched_layer = CFSConfigurationLayer(self.clone_url, name=self.name,
                                                 playbook='special.yml', commit=self.commit)
        self.assert_does_not_match(mismatched_layer)

    def test_matches_mismatch_different_repo_path(self):
        """Test matches method against mismatched CFSConfigurationLayer with different repo path."""
        mismatched_layer = CFSConfigurationLayer('http://api-gw-service-nmn.local/vcs/cray/mismatch.git',
                                                 name=self.name, playbook=self.playbook,
                                                 commit=self.commit)
        self.assert_does_not_match(mismatched_layer)

    def test_matches_mismatch_wrong_type(self):
        """Test matches method against an incorrect type of object."""
        self.assertFalse(self.cfs_config_layer.matches('not correct type'))

    def test_get_updated_values(self):
        """Test get_updated_values against a changed layer."""
        new_clone_url = f'http://api-gw-service.nmn{self.repo_path}'
        new_name = 'sat-2.3.3'
        new_commit = 'abcdef1'
        new_layer = CFSConfigurationLayer(new_clone_url, name=new_name,
                                          playbook=self.playbook, commit=new_commit)
        updated_values = self.cfs_config_layer.get_updated_values(new_layer)
        self.assertEqual({
            'cloneUrl': (self.clone_url, new_clone_url),
            'name': (self.name, new_name),
            'commit': (self.commit, new_commit)
        }, updated_values)

    def test_resolve_branch_no_branch(self):
        """Test resolve_branch_to_commit_hash when no branch is specified."""
        self.cfs_config_layer.resolve_branch_to_commit_hash()
        self.assertIsNone(self.cfs_config_layer.branch)
        self.assertEqual(self.commit, self.cfs_config_layer.commit)
        self.mock_vcs_repo_cls.assert_not_called()

    def test_resolve_branch_only_branch(self):
        """Test resolve_branch_to_commit_hash when only branch is specified."""
        branch = 'integration'
        cfs_config_layer = CFSConfigurationLayer(self.clone_url, name=self.name,
                                                 playbook=self.playbook, branch=branch)

        cfs_config_layer.resolve_branch_to_commit_hash()

        self.mock_vcs_repo_cls.assert_called_once_with(self.clone_url)
        self.mock_vcs_repo.get_commit_hash_for_branch.assert_called_once_with(branch)
        self.assertEqual(self.mock_vcs_repo.get_commit_hash_for_branch.return_value,
                         cfs_config_layer.commit)
        self.assertIsNone(cfs_config_layer.branch)

    def test_resolve_branch_with_commit(self):
        """Test resolve_branch_to_commit_hash when branch and commit are specified."""
        branch = 'integration'
        cfs_config_layer = CFSConfigurationLayer(self.clone_url, name=self.name,
                                                 playbook=self.playbook, commit=self.commit,
                                                 branch=branch)

        with self.assertLogs(level=logging.INFO) as logs_cm:
            cfs_config_layer.resolve_branch_to_commit_hash()

        self.assertIn(f'already specifies a commit hash ({self.commit}) and branch ({branch})',
                      logs_cm.records[0].message)
        self.mock_vcs_repo_cls.assert_called_once_with(self.clone_url)
        self.mock_vcs_repo.get_commit_hash_for_branch.assert_called_once_with(branch)
        self.assertEqual(self.mock_vcs_repo.get_commit_hash_for_branch.return_value,
                         cfs_config_layer.commit)
        self.assertIsNone(cfs_config_layer.branch)

    def test_resolve_branch_does_not_exist(self):
        """Test resolve_branch_to_commit_hash when branch does not exist."""
        branch = 'integration'
        cfs_config_layer = CFSConfigurationLayer(self.clone_url, name=self.name,
                                                 playbook=self.playbook, branch=branch)
        self.mock_vcs_repo.get_commit_hash_for_branch.return_value = None
        err_regex = f'Failed to resolve branch {branch} to commit hash. No such branch.'

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            cfs_config_layer.resolve_branch_to_commit_hash()

        self.mock_vcs_repo_cls.assert_called_once_with(self.clone_url)
        self.mock_vcs_repo.get_commit_hash_for_branch.assert_called_once_with(branch)

        # Commit and branch should remain unaltered
        self.assertIsNone(cfs_config_layer.commit)
        self.assertEqual(branch, cfs_config_layer.branch)

    def test_resolve_branch_vcs_error(self):
        """Test resolve_branch_to_commit_hash when there is an error in VCSRepo."""
        branch = 'integration'
        cfs_config_layer = CFSConfigurationLayer(self.clone_url, name=self.name,
                                                 playbook=self.playbook, branch=branch)
        vcs_err_msg = 'Error access VCS'
        self.mock_vcs_repo.get_commit_hash_for_branch.side_effect = VCSError(vcs_err_msg)
        err_regex = f'Failed to resolve branch {branch} to commit hash: {vcs_err_msg}'

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            cfs_config_layer.resolve_branch_to_commit_hash()

        self.mock_vcs_repo_cls.assert_called_once_with(self.clone_url)
        self.mock_vcs_repo.get_commit_hash_for_branch.assert_called_once_with(branch)

        # Commit and branch should remain unaltered
        self.assertIsNone(cfs_config_layer.commit)
        self.assertEqual(branch, cfs_config_layer.branch)

    def test_req_payload_commit_only(self):
        """Test req_payload property when only a commit hash is specified."""
        expected_payload = {
            'cloneUrl': self.clone_url,
            'commit': self.commit,
            'name': self.name,
            'playbook': self.playbook
        }
        self.assertEqual(expected_payload, self.cfs_config_layer.req_payload)

    def test_req_payload_branch_only(self):
        """Test req_payload property when only a branch is specified."""
        branch = 'integration'
        cfs_config_layer = CFSConfigurationLayer(self.clone_url, name=self.name,
                                                 playbook=self.playbook, branch=branch)
        expected_payload = {
            'cloneUrl': self.clone_url,
            'branch': branch,
            'name': self.name,
            'playbook': self.playbook
        }
        self.assertEqual(expected_payload, cfs_config_layer.req_payload)

    def test_req_payload_branch_and_commit(self):
        """Test req_payload when branch and commit are specified."""
        branch = 'integration'
        cfs_config_layer = CFSConfigurationLayer(self.clone_url, name=self.name,
                                                 playbook=self.playbook, branch=branch,
                                                 commit=self.commit)
        expected_payload = {
            'cloneUrl': self.clone_url,
            'branch': branch,
            'commit': self.commit,
            'name': self.name,
            'playbook': self.playbook
        }
        self.assertEqual(expected_payload, cfs_config_layer.req_payload)

    def test_req_payload_only_clone_url_and_commit(self):
        """Test req_payload when only clone URL and commit are specified."""
        cfs_config_layer = CFSConfigurationLayer(self.clone_url, commit=self.commit)
        expected_payload = {
            'cloneUrl': self.clone_url,
            'commit': self.commit
        }
        self.assertEqual(expected_payload, cfs_config_layer.req_payload)

    def test_str_with_playbook(self):
        """Test __str__ method with a playbook specified."""
        self.assertEqual(f'layer with repo path {self.repo_path} and playbook {self.playbook}',
                         str(self.cfs_config_layer))

    def test_str_with_no_playbook(self):
        """Test __str__ method with no playbook specified."""
        cfs_config_layer = CFSConfigurationLayer(self.clone_url, commit=self.commit)
        self.assertEqual(f'layer with repo path {self.repo_path} and default playbook',
                         str(cfs_config_layer))

    def test_construct_name_playbook_and_branch(self):
        """Test construct_name static method with a playbook and branch."""
        branch_name = 'integration'
        name = CFSConfigurationLayer.construct_name(self.product, playbook=self.playbook,
                                                    branch=branch_name)
        expected_name = (f'{self.product}-{self.playbook[:-4]}-'
                         f'{branch_name[:MAX_BRANCH_NAME_WIDTH]}-{self.expected_timestamp}')
        self.assertEqual(expected_name, name)

    def test_construct_name_no_playbook(self):
        """Test construct_name static method with no playbook and a commit."""
        name = CFSConfigurationLayer.construct_name(self.product, commit='1234567abcdef')
        self.assertEqual(f'{self.product}-site-1234567-{self.expected_timestamp}',
                         name)


class TestCFSConfigurationLayerFromProduct(unittest.TestCase):
    """Tests for CFSConfigurationLayer.from_product_catalog class method."""

    def setUp(self):
        self.product_name = 'sat'
        self.product_version = '2.3.3'
        self.clone_url = f'https://vcs.system.domain/vcs/cray/{self.product_name}-config-management.git'
        self.product_commit = 'abcdef7654321'

        self.api_gw_host = 'api-gw-host.local'
        self.expected_clone_url = f'https://{self.api_gw_host}/vcs/cray/{self.product_name}-config-management.git'

        self.mock_product_catalog_cls = patch('csm_api_client.service.cfs.ProductCatalog').start()
        self.mock_product_catalog = self.mock_product_catalog_cls.return_value
        self.mock_product = self.mock_product_catalog.get_product.return_value
        self.mock_product.clone_url = self.clone_url
        self.mock_product.commit = self.product_commit

        self.mock_construct_name = patch.object(CFSConfigurationLayer, 'construct_name').start()

    def tearDown(self):
        patch.stopall()

    def test_product_defaults(self):
        """Test from_product_catalog with defaults from product entry."""
        layer = CFSConfigurationLayer.from_product_catalog(self.product_name, self.api_gw_host,
                                                           product_version=self.product_version)
        self.mock_product_catalog.get_product.assert_called_once_with(self.product_name,
                                                                      self.product_version)
        self.assertEqual(self.expected_clone_url, layer.clone_url)
        self.assertEqual(self.product_commit, layer.commit)
        self.assertIsNone(layer.branch)
        self.assertEqual(self.mock_construct_name.return_value, layer.name)

    def test_product_no_version_with_commit_hash(self):
        """Test from_product_catalog when a commit hash is specified."""
        alternate_commit = 'a1b2c3d'
        layer = CFSConfigurationLayer.from_product_catalog(self.product_name, self.api_gw_host, commit=alternate_commit)
        self.mock_product_catalog.get_product.assert_called_once_with(self.product_name, None)
        self.assertEqual(self.expected_clone_url, layer.clone_url)
        self.assertEqual(alternate_commit, layer.commit)
        self.assertIsNone(layer.branch)
        self.assertEqual(self.mock_construct_name.return_value, layer.name)

    def test_product_with_branch(self):
        """Test from_product_catalog when a branch is specified."""
        branch = 'integration'
        layer = CFSConfigurationLayer.from_product_catalog(self.product_name, self.api_gw_host,
                                                           product_version=self.product_version,
                                                           branch=branch)
        self.mock_product_catalog.get_product.assert_called_once_with(self.product_name,
                                                                      self.product_version)
        self.assertEqual(self.expected_clone_url, layer.clone_url)
        self.assertEqual(branch, layer.branch)
        self.assertIsNone(layer.commit)
        self.assertEqual(self.mock_construct_name.return_value, layer.name)

    def test_product_custom_layer_name(self):
        """Test from_product_catalog with a custom layer name."""
        custom_layer_name = f'custom-{self.product_name}'

        layer = CFSConfigurationLayer.from_product_catalog(self.product_name, self.api_gw_host,
                                                           name=custom_layer_name)
        self.mock_construct_name.assert_not_called()
        self.assertEqual(self.expected_clone_url, layer.clone_url)
        self.assertEqual(self.product_commit, layer.commit)
        self.assertIsNone(layer.branch)
        self.assertEqual(custom_layer_name, layer.name)

    def test_product_catalog_failure(self):
        """Test from_product_catalog when product catalog data can't be loaded."""
        pc_err_msg = 'k8s down'
        self.mock_product_catalog_cls.side_effect = ProductCatalogError(pc_err_msg)
        err_regex = (f'Failed to create CFS configuration layer for product '
                     f'{self.product_name}: {pc_err_msg}')

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            CFSConfigurationLayer.from_product_catalog(self.product_name, self.api_gw_host)

    def test_product_unknown_version(self):
        """Test from_product_catalog when unable to find the requested version of the product."""
        pc_err_msg = 'unable to find that version'
        self.mock_product_catalog.get_product.side_effect = ProductCatalogError(pc_err_msg)
        err_regex = (f'Failed to create CFS configuration layer for version {self.product_version} '
                     f'of product {self.product_name}: {pc_err_msg}')

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            CFSConfigurationLayer.from_product_catalog(self.product_name, self.api_gw_host,
                                                       product_version=self.product_version)

    def test_product_missing_commit(self):
        """Test from_product_catalog when unable to find a commit hash for the product."""
        self.mock_product.commit = None
        err_regex = (f'Failed to create CFS configuration layer for product '
                     f'{self.product_name}: .* has no commit hash')

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            CFSConfigurationLayer.from_product_catalog(self.product_name, self.api_gw_host)

    def test_product_missing_clone_url(self):
        """Test from_product_catalog when unable to find a clone URL for the product."""
        self.mock_product.clone_url = None
        err_regex = (f'Failed to create CFS configuration layer for product '
                     f'{self.product_name}: .* has no clone URL')

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            CFSConfigurationLayer.from_product_catalog(self.product_name, self.api_gw_host)

    def test_api_gw_host_with_url_scheme(self):
        """Test from_product_catalog when a URL scheme is added to the URL."""
        layer = CFSConfigurationLayer.from_product_catalog(self.product_name, f'https://{self.api_gw_host}')
        self.assertEqual(layer.clone_url, self.expected_clone_url)


class TestCFSConfigurationLayerFromCloneUrl(unittest.TestCase):
    """Tests for CFSConfigurationLayer.from_clone_url class method."""

    def setUp(self):
        self.mock_construct_name = patch.object(CFSConfigurationLayer, 'construct_name').start()

    def tearDown(self):
        patch.stopall()

    def test_from_clone_url(self):
        """Test from_clone_url."""
        short_name = 'cray'
        clone_url = f'http://api-gw/vcs/{short_name}-config-management.git'
        playbook = 'cray.yml'
        branch = 'main'
        layer = CFSConfigurationLayer.from_clone_url(clone_url, playbook=playbook, branch=branch)

        self.mock_construct_name.assert_called_once_with(short_name, playbook=playbook,
                                                         commit=None, branch=branch)
        self.assertEqual(self.mock_construct_name.return_value, layer.name)
        self.assertEqual(branch, layer.branch)
        self.assertEqual(clone_url, layer.clone_url)
        self.assertIsNone(layer.commit)

    def test_from_clone_url_custom_layer_name(self):
        """Test from_clone_url with custom layer name."""
        clone_url = f'http://api-gw/vcs/hpe-config-management.git'
        playbook = 'hpe.yml'
        branch = 'main'
        custom_layer_name = 'hpe'
        layer = CFSConfigurationLayer.from_clone_url(clone_url, name=custom_layer_name,
                                                     playbook=playbook, branch=branch)

        self.mock_construct_name.assert_not_called()
        self.assertEqual(custom_layer_name, layer.name)
        self.assertEqual(branch, layer.branch)
        self.assertEqual(clone_url, layer.clone_url)
        self.assertIsNone(layer.commit)


class TestCFSConfigurationLayerFromCFS(unittest.TestCase):
    """Tests for CFSConfigurationLayer.from_cfs class method."""

    def test_from_cfs(self):
        """Test CFSConfigurationLayer.from_cfs class method."""
        clone_url = 'https://api-gw-service-nmn.local/vcs/cray/cos-config-management.git'
        commit = '88cc1c1245882c84f94fd632447986757fe5c045'
        name = 'cos-integration-2.2.100'
        playbook = 'ncn.yml'
        data = {
            "cloneUrl": clone_url,
            "commit": commit,
            "name": name,
            "playbook": playbook
        }
        layer = CFSConfigurationLayer.from_cfs(data)
        self.assertEqual(clone_url, layer.clone_url)
        self.assertEqual(commit, layer.commit)
        self.assertEqual(name, layer.name)
        self.assertEqual(playbook, layer.playbook)
        self.assertIsNone(layer.branch)


class MockFile(io.StringIO):
    """An in-memory file object which is not automatically closed"""
    def __exit__(self, *_):
        pass


class TestCFSConfiguration(unittest.TestCase):
    """Tests for the CFSConfiguration class."""
    def setUp(self):
        self.mock_cfs_client_cls = patch('csm_api_client.service.cfs.CFSClient').start()
        self.mock_cfs_client = self.mock_cfs_client_cls.return_value
        self.mock_get_json = self.mock_cfs_client.get.return_value.json

        self.example_layer_data = {
            "cloneUrl": "https://api-gw-service-nmn.local/vcs/cray/example-config-management.git",
            "commit": "123456789abcdef",
            "name": "example-config",
            "playbook": "example-config.yml"
        }
        self.example_layer = CFSConfigurationLayer.from_cfs(self.example_layer_data)

        self.new_layer_data = {
            "cloneUrl": "https://api-gw-service-nmn.local/vcs/cray/new-config-management.git",
            "commit": "fedcba987654321",
            "name": "new-config",
            "playbook": "new-config.yml"
        }
        self.new_layer = CFSConfigurationLayer.from_cfs(self.new_layer_data)

        self.single_layer_config_data = {
            "lastUpdated": "2021-10-20T21:26:04Z",
            "layers": [
                deepcopy(self.example_layer_data)
            ],
            "name": "single-layer-config"
        }
        self.single_layer_config = CFSConfiguration(self.mock_cfs_client,
                                                    self.single_layer_config_data)

        self.duplicate_layer_config_data = {
            "lastUpdated": "2021-10-20T21:26:04Z",
            "layers": [
                deepcopy(self.example_layer_data),
                deepcopy(self.example_layer_data)
            ],
            "name": "duplicate-layer-config"
        }
        self.duplicate_layer_config = CFSConfiguration(self.mock_cfs_client,
                                                       self.duplicate_layer_config_data)

        self.single_layer_file_contents = json.dumps(self.single_layer_config_data)
        self.single_layer_file_obj = MockFile(self.single_layer_file_contents)
        self.empty_file_obj = MockFile()

        def mock_open(_, mode):
            if mode == 'x':
                raise FileExistsError
            elif mode == 'w':
                return self.empty_file_obj
            else:
                return self.single_layer_file_obj

        self.mock_open = patch('builtins.open', mock_open).start()
        self.file_path = 'some_file.json'

    def tearDown(self):
        patch.stopall()

    @patch('csm_api_client.service.cfs.CFSConfigurationLayer.from_cfs')
    def test_construct_cfs_configuration(self, mock_from_cfs):
        """Test the CFSConfiguration constructor."""
        cfs_config = CFSConfiguration(self.mock_cfs_client, self.single_layer_config_data)
        self.assertEqual('single-layer-config', cfs_config.name)
        self.assertEqual([mock_from_cfs.return_value], cfs_config.layers)

    def test_construct_empty_configuration(self):
        """Test creating an empty configuration"""
        self.assertEqual(CFSConfiguration.empty(self.mock_cfs_client).layers, [])

    def test_req_payload(self):
        """Test the req_payload method of CFSConfiguration."""
        self.assertEqual({'layers': [self.example_layer_data]},
                         self.single_layer_config.req_payload)

    def test_save_to_cfs(self):
        """Test that save_to_cfs properly calls the CFS API."""
        config_name = self.single_layer_config_data['name']
        layers = self.single_layer_config_data['layers']

        with patch('csm_api_client.service.cfs.CFSConfiguration') as mock_cfs_config_cls:
            updated_config = self.single_layer_config.save_to_cfs()

        self.mock_cfs_client.put.assert_called_once_with(
            'configurations', config_name, json={'layers': layers}
        )
        mock_cfs_config_cls.assert_called_once_with(
            self.mock_cfs_client, self.mock_cfs_client.put.return_value.json.return_value)
        self.assertEqual(mock_cfs_config_cls.return_value, updated_config)

    def test_save_to_cfs_new_name(self):
        """Test that save_to_cfs saves to a new name."""
        new_name = 'new-config-name'
        layers = self.single_layer_config_data['layers']

        with patch('csm_api_client.service.cfs.CFSConfiguration') as mock_cfs_config_cls:
            updated_config = self.single_layer_config.save_to_cfs(new_name)

        self.mock_cfs_client.put.assert_called_once_with(
            'configurations', new_name, json={'layers': layers}
        )
        mock_cfs_config_cls.assert_called_once_with(
            self.mock_cfs_client, self.mock_cfs_client.put.return_value.json.return_value)
        self.assertEqual(mock_cfs_config_cls.return_value, updated_config)

    def test_save_to_cfs_no_overwrite(self):
        """Test preventing overwriting an existing CFS configuration"""
        self.mock_cfs_client.get.return_value.status_code = 200
        config_name = self.single_layer_config_data['name']
        with self.assertRaisesRegex(CFSConfigurationError, 'already exists'):
            self.single_layer_config.save_to_cfs(config_name, overwrite=False)

    def test_save_to_cfs_api_failure(self):
        """Test that save_to_cfs raises an exception if CFS API request fails."""
        self.mock_cfs_client.put.side_effect = APIError('cfs problem')

        err_regex = f'Failed to update CFS configuration "single-layer-config": cfs problem'

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            self.single_layer_config.save_to_cfs()

    def test_save_to_cfs_bad_json_response(self):
        """Test that save_to_cfs raises an exception if the response from CFS contains bad JSON."""
        self.mock_cfs_client.put.return_value.json.side_effect = ValueError('bad json')

        err_regex = (f'Failed to decode JSON response from updating CFS configuration '
                     f'"single-layer-config": bad json')

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            self.single_layer_config.save_to_cfs()

    def test_save_to_cfs_backup_when_existing(self):
        """Test save_to_cfs creates a backup when the configuration already exists."""
        self.mock_cfs_client.get.return_value.status_code = 200
        config_name = self.single_layer_config_data['name']
        layers = self.single_layer_config_data['layers']
        backup_suffix = '.backup'

        # Set up some Mock config objects as CFSConfiguration return values
        # The first one is the backup copy of the CFSConfiguration
        backup_config = Mock()
        # The second one is the new configuration after it's saved to CFS
        new_config = Mock()
        mock_cfs_configs = [backup_config, new_config]

        with patch('csm_api_client.service.cfs.CFSConfiguration') as mock_cfs_config_cls:
            mock_cfs_config_cls.side_effect = mock_cfs_configs
            updated_config = self.single_layer_config.save_to_cfs(config_name, backup_suffix=backup_suffix)

        self.mock_cfs_client.put.assert_called_once_with(
            'configurations', config_name, json={'layers': layers}
        )

        mock_cfs_config_cls.assert_has_calls([
            call(self.mock_cfs_client, self.mock_cfs_client.get.return_value.json.return_value),
            call(self.mock_cfs_client, self.mock_cfs_client.put.return_value.json.return_value)
        ])
        backup_config.save_to_cfs.assert_called_once_with(name=f'{config_name}{backup_suffix}')
        self.assertEqual(new_config, updated_config)

    def test_save_to_cfs_backup_when_not_existing(self):
        """Test save_to_cfs creates a backup when the configuration does not exist"""
        self.mock_cfs_client.get.return_value.ok = False
        self.mock_cfs_client.get.return_value.status_code = 404
        layers = self.single_layer_config_data['layers']
        new_name = 'some-new-name'

        with patch('csm_api_client.service.cfs.CFSConfiguration') as mock_cfs_config_cls:
            saved_config = self.single_layer_config.save_to_cfs(new_name, backup_suffix='.backup')

        self.mock_cfs_client.put.assert_called_once_with(
            'configurations', new_name, json={'layers': layers}
        )
        mock_cfs_config_cls.assert_called_once_with(
            self.mock_cfs_client, self.mock_cfs_client.put.return_value.json.return_value)
        self.assertEqual(mock_cfs_config_cls.return_value, saved_config)

    def test_save_to_file(self):
        """Test that saving configuration to a file works"""
        self.single_layer_config.save_to_file(self.file_path)

        self.empty_file_obj.seek(0)
        dumped_data = json.load(self.empty_file_obj)

        for key, value in dumped_data.items():
            self.assertEqual(self.single_layer_config_data[key], value)

    def test_save_to_file_no_overwrite(self):
        """Test that overwriting a file fails when overwrite disabled"""
        with self.assertRaisesRegex(CFSConfigurationError, 'already exists'):
            self.single_layer_config.save_to_file(self.file_path, overwrite=False)

    def test_add_new_layer(self):
        """Test adding a new, totally different, layer to a CFSConfiguration"""
        self.single_layer_config.ensure_layer(self.new_layer, state=LayerState.PRESENT)

        self.assertEqual(2, len(self.single_layer_config.layers))
        self.assertEqual(self.new_layer, self.single_layer_config.layers[1])

    def test_update_existing_layer(self):
        """Test updating an existing layer in a CFSConfiguration"""
        new_commit_hash = 'fedcba987654321'
        updated_layer_data = deepcopy(self.example_layer_data)
        updated_layer_data['commit'] = new_commit_hash
        updated_layer = CFSConfigurationLayer.from_cfs(updated_layer_data)

        self.single_layer_config.ensure_layer(updated_layer, state=LayerState.PRESENT)

        self.assertEqual([updated_layer], self.single_layer_config.layers)

    def test_update_existing_layers(self):
        """Test updating two matching layers of a CFSConfiguration"""
        new_commit_hash = 'fedcba987654321'
        updated_layer_data = deepcopy(self.example_layer_data)
        updated_layer_data['commit'] = new_commit_hash
        updated_layer = CFSConfigurationLayer.from_cfs(updated_layer_data)

        self.duplicate_layer_config.ensure_layer(updated_layer, state=LayerState.PRESENT)

        self.assertEqual([updated_layer] * 2, self.duplicate_layer_config.layers)

    def test_update_layer_no_changes(self):
        """Test updating a layer of the CFSConfiguration when nothing has changed."""
        same_layer = CFSConfigurationLayer.from_cfs(deepcopy(self.example_layer_data))

        self.single_layer_config.ensure_layer(same_layer, state=LayerState.PRESENT)

        self.assertEqual([same_layer], self.single_layer_config.layers)
        self.assertFalse(self.single_layer_config.changed)

    def test_remove_existing_layer(self):
        """Test removing a matching layer from a CFSConfiguration."""
        same_layer = CFSConfigurationLayer.from_cfs(deepcopy(self.example_layer_data))

        self.single_layer_config.ensure_layer(same_layer, state=LayerState.ABSENT)

        self.assertEqual([], self.single_layer_config.layers)

    def test_remove_existing_layers(self):
        """Test removing multiple matching layers from a CFSConfiguration."""
        same_layer = CFSConfigurationLayer.from_cfs(deepcopy(self.example_layer_data))

        self.duplicate_layer_config.ensure_layer(same_layer, state=LayerState.ABSENT)

        self.assertEqual([], self.duplicate_layer_config.layers)

    def test_remove_non_existent_layer(self):
        """Test removing a layer that doesn't exist from a CFSConfiguration."""
        layers_before = self.single_layer_config.layers

        self.single_layer_config.ensure_layer(self.new_layer, state=LayerState.ABSENT)

        self.assertEqual(layers_before, self.single_layer_config.layers)
        self.assertFalse(self.single_layer_config.changed)


class TestCFSDebugCommand(unittest.TestCase):
    """Test getting the kubectl log command to debug a failing CFS job"""

    def setUp(self):
        self.failed_container = CFSImageConfigurationSession.get_first_failed_container
        self.update_status = CFSImageConfigurationSession._update_container_status

    def test_get_failing_init_container(self):
        """Check that init containers are always selected first"""
        for containers in [[], ['some', 'other', 'containers'], ['onecontainer']]:
            self.assertEqual(self.failed_container(['init'], containers), 'init')

    def test_get_failing_inventory_container(self):
        """Check that the inventory container is selected first"""
        for containers in [['inventory', 'ansible-1', 'teardown'], ['teardown', 'inventory', 'ansible']]:
            self.assertEqual(self.failed_container([], containers), 'inventory')

    def test_get_failing_ansible_container(self):
        """Check that the first ansible container is selected first"""
        self.assertEqual(self.failed_container([], ['ansible-3', 'ansible-1', 'ansible-2', 'teardown']), 'ansible-1')
        self.assertEqual(self.failed_container([], ['ansible', 'teardown']), 'ansible')

    def test_get_failing_teardown_container(self):
        """Check that the teardown container is selected when failing"""
        self.assertEqual(self.failed_container([], ['teardown']), 'teardown')

    def test_no_message_when_no_failing_containers(self):
        """Check that no debug message is given if there are no failing containers"""
        self.assertIsNone(self.failed_container([], []))

    def test_update_container_status_with_none_pod(self):
        """Check that update_container_status returns None when pod is None"""
        session = CFSImageConfigurationSession({}, MagicMock(), 'test_image')
        session.pod = None
        self.assertIsNone(self.update_status(session))

    def test_update_container_status_with_none_init_and_container_statuses(self):
        """Check that update_container_status returns None when both init and container statuses are None"""
        session = CFSImageConfigurationSession({}, MagicMock(), 'test_image')
        session.pod = MagicMock()
        session.pod.status.init_container_statuses = None
        session.pod.status.container_statuses = None
        self.assertIsNone(self.update_status(session))

    def test_update_container_status_with_none_init_statuses(self):
        """Check that update_container_status logs a message when init_container_statuses is None"""
        session = CFSImageConfigurationSession({}, MagicMock(), 'test_image')
        session.pod = MagicMock()
        session.pod.status.init_container_statuses = [None,MagicMock()]
        session.pod.status.container_statuses = [MagicMock()]
        with self.assertLogs() as log:
            self.update_status(session)
        self.assertIn('Found a None container', log.output[0])


class TestCFSClient(unittest.TestCase):
    """Tests for the CFSClient class"""

    @staticmethod
    def get_components(component_ids: List[str]) -> List[dict]:
        """Get a fake list of components"""
        components = []
        for component_id in component_ids:
            components.append({
                'configurationStatus': 'configured',
                'desiredConfig': 'management-23.11',
                'enabled': True,
                'errorCount': 0,
                'id': component_id,
                'state': [],
                'tags': {}
            })
        return components

    def test_get_component_ids_using_config(self):
        """Test get_component_ids_using_config"""
        component_ids = ['x3000c0s1b0n0', 'x3000c0s3b0n0', 'x3000c0s5b0n0']
        components = self.get_components(component_ids)
        # For the test, it doesn't have to match, but be consistent to avoid confusion
        config_name = components[0]['desiredConfig']
        cfs_client = CFSClient(Mock())

        with patch.object(cfs_client, 'get') as mock_get:
            mock_get.return_value.json.return_value = components
            result = cfs_client.get_component_ids_using_config(config_name)

        mock_get.assert_called_once_with('components', params={'configName': config_name})
        self.assertEqual(component_ids, result)

    def test_get_component_ids_using_config_failure(self):
        """Test get_component_ids_using_config when the request fails"""
        cfs_client = CFSClient(Mock())
        err_msg = 'Service unavailable'
        config_name = 'some-config-name'

        with patch.object(cfs_client, 'get', side_effect=APIError(err_msg)) as mock_get:
            with self.assertRaisesRegex(APIError, 'Failed to get components '):
                cfs_client.get_component_ids_using_config(config_name)

        mock_get.assert_called_once_with('components', params={'configName': config_name})

    def test_update_component_no_changes(self):
        """Test update_component with no changes requested"""
        cfs_client = CFSClient(Mock())
        component_id = 'x1000c0s0b0n0'

        with patch.object(cfs_client, 'patch') as mock_patch:
            with self.assertLogs(level=logging.WARNING) as logs_cm:
                cfs_client.update_component(component_id)

        mock_patch.assert_not_called()
        self.assertRegex(logs_cm.records[0].message,
                         'No property changes were requested')

    def test_update_component_only_desired_config(self):
        """Test update_component with only a desired_config"""
        cfs_client = CFSClient(Mock())
        component_id = 'x3000c0s1b0n0'
        desired_config = 'my-config'

        with patch.object(cfs_client, 'patch') as mock_patch:
            cfs_client.update_component(component_id, desired_config=desired_config)

        mock_patch.assert_called_once_with('components', component_id,
                                           json={'desiredConfig': desired_config})

    def test_update_component_all_properties(self):
        """Test update_component with all properties updated"""
        cfs_client = CFSClient(Mock())
        component_id = 'x3000c0s1b0n0'
        desired_config = 'my-config'

        with patch.object(cfs_client, 'patch') as mock_patch:
            cfs_client.update_component(component_id, desired_config=desired_config,
                                        enabled=True, clear_state=True, clear_error=True)

        expected_json_payload = {
            'desiredConfig': desired_config,
            'enabled': True,
            'state': [],
            'errorCount': 0
        }

        mock_patch.assert_called_once_with('components', component_id,
                                           json=expected_json_payload)
