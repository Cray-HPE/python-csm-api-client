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
    CFSClientBase,
    CFSV2Client,
    CFSV3Client,
    CFSV2Configuration,
    CFSV3Configuration,
    CFSConfigurationError,
    CFSLayerBase,
    CFSV2AdditionalInventoryLayer,
    CFSV3AdditionalInventoryLayer,
    CFSV2ConfigurationLayer,
    CFSV3ConfigurationLayer,
    CFSImageConfigurationSession,
    LayerState
)
from csm_api_client.service.gateway import APIError
from csm_api_client.service.vcs import VCSError


class TestCFSLayerBase(unittest.TestCase):
    """Tests for the CFSLayerBase class."""

    def setUp(self):
        self.product = 'sat'
        self.repo_path = f'/vcs/cray/{self.product}-config-management.git'
        self.clone_url = f'http://api-gw-service-nmn.local{self.repo_path}'
        self.name = 'sat'
        self.commit = 'abcd1234'
        self.additional_data = {
            'special_parameters': {
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }

        # Note that it is possible to create an instance of this abstract base class
        # only because it doesn't have any abstract methods. If one is added in the future,
        # this test will need to create a concrete subclass to test with.
        self.cfs_layer_cls = CFSLayerBase
        self.cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url,
                                            name=self.name, commit=self.commit,
                                            additional_data=self.additional_data)

        self.mock_vcs_repo_cls = patch('csm_api_client.service.cfs.VCSRepo').start()
        self.mock_vcs_repo = self.mock_vcs_repo_cls.return_value

        self.mock_datetime = patch('csm_api_client.service.cfs.datetime', wraps=datetime.datetime).start()
        self.static_datetime = datetime.datetime(2022, 2, 19, 23, 12, 48)
        self.mock_datetime.now.return_value = self.static_datetime
        self.expected_timestamp = '20220219T231248'

    def tearDown(self):
        patch.stopall()

    def test_init_subclass(self):
        """Test that the __init_subclass__ method properly sets up ATTRS_TO_CFS_PROPS"""

        # Make a concrete subclass of CFSLayerBase
        class ConcreteCFSLayer(CFSLayerBase):
            # Add another property like the actual subclasses do
            CFS_PROPS_TO_ATTRS = CFSLayerBase.CFS_PROPS_TO_ATTRS.copy()
            CFS_PROPS_TO_ATTRS['cloneUrl'] = 'clone_url'

        self.assertEqual({
            'clone_url': 'cloneUrl',
            'commit': 'commit',
            'branch': 'branch',
            'name': 'name'
        }, ConcreteCFSLayer.ATTRS_TO_CFS_PROPS)

        # Ensure the base class is not modified
        self.assertEqual({
            'commit': 'commit',
            'branch': 'branch',
            'name': 'name'
        }, CFSLayerBase.ATTRS_TO_CFS_PROPS)

    def test_construct_cfs_layer(self):
        """Test creating a new CFSConfigurationLayerBase."""
        self.assertEqual(self.clone_url, self.cfs_layer.clone_url)
        self.assertEqual(self.name, self.cfs_layer.name)
        self.assertEqual(self.commit, self.cfs_layer.commit)
        self.assertIsNone(self.cfs_layer.branch)
        self.assertEqual(self.additional_data, self.cfs_layer.additional_data)

    def test_construct_cfs_layer_branch_only(self):
        """Test creating a new CFSConfigurationLayerBase with only a branch."""
        cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=self.name,
                                       branch='integration')
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertIsNone(cfs_layer.commit)
        self.assertEqual('integration', cfs_layer.branch)
        self.assertEqual({}, cfs_layer.additional_data)

    def test_construct_cfs_layer_no_branch_commit(self):
        """Test creating a new CFSConfigurationLayerBase requires a git commit or branch."""
        with self.assertRaisesRegex(ValueError, 'Either commit or branch is required'):
            self.cfs_layer_cls(clone_url=self.clone_url)

    def test_generated_name_with_commit(self):
        """Test CFSConfigurationLayerBase.name when name is omitted and commit is specified."""
        cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url, commit='1234567abcdef')
        self.assertEqual(f'{self.product}-1234567-{self.expected_timestamp}', cfs_layer.name)

    def test_generated_name_with_branch(self):
        """Test CFSConfigurationLayerBase.name when name is omitted and branch is specified."""
        cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url, branch='integration')
        self.assertEqual(f'{self.product}-integra-{self.expected_timestamp}', cfs_layer.name)

    def test_repo_path(self):
        """Test the repo_path property of CFSConfigurationLayerBase."""
        self.assertEqual(self.repo_path, self.cfs_layer.repo_path)

    def test_repo_short_name(self):
        """Test the repo_short_name property of CFSConfigurationLayerBase."""
        self.assertEqual(self.product, self.cfs_layer.repo_short_name)

    def test_str(self):
        """Test __str__ method of CFSConfigurationLayerBase."""
        self.assertEqual(f'{self.cfs_layer_cls.__name__} with repo path {self.repo_path}',
                         str(self.cfs_layer))

    def test_get_updated_values_all_updated(self):
        """Test get_updated_values with a layer where all attributes changed."""
        new_name = 'sat-2.3.3'
        new_commit = 'abcdef1'
        new_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=new_name, commit=new_commit)
        updated_values = self.cfs_layer.get_updated_values(new_layer)
        self.assertEqual({
            'name': (self.name, new_name),
            'commit': (self.commit, new_commit)
        }, updated_values)

    def test_get_updated_values_commit_updated(self):
        """Test get_updated_values with a layer where just the commit is updated."""
        new_commit = '123abcd'
        new_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=self.name, commit=new_commit)
        updated_values = self.cfs_layer.get_updated_values(new_layer)
        self.assertEqual({
            'commit': (self.commit, new_commit)
        }, updated_values)

    def test_resolve_branch_no_branch(self):
        """Test resolve_branch_to_commit_hash when no branch is specified."""
        self.cfs_layer.resolve_branch_to_commit_hash()
        self.assertIsNone(self.cfs_layer.branch)
        self.assertEqual(self.commit, self.cfs_layer.commit)
        self.mock_vcs_repo_cls.assert_not_called()

    def test_resolve_branch_only_branch(self):
        """Test resolve_branch_to_commit_hash when only branch is specified."""
        branch = 'integration'
        cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=self.name,
                                       branch=branch)

        cfs_layer.resolve_branch_to_commit_hash()

        self.mock_vcs_repo_cls.assert_called_once_with(self.clone_url)
        self.mock_vcs_repo.get_commit_hash_for_branch.assert_called_once_with(branch)
        self.assertEqual(self.mock_vcs_repo.get_commit_hash_for_branch.return_value,
                         cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)

    def test_resolve_branch_with_commit(self):
        """Test resolve_branch_to_commit_hash when branch and commit are specified."""
        branch = 'integration'
        cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=self.name,
                                       commit=self.commit, branch=branch)

        with self.assertLogs(level=logging.INFO) as logs_cm:
            cfs_layer.resolve_branch_to_commit_hash()

        self.assertIn(f'already specifies a commit hash ({self.commit}) and branch ({branch})',
                      logs_cm.records[0].message)
        self.mock_vcs_repo_cls.assert_called_once_with(self.clone_url)
        self.mock_vcs_repo.get_commit_hash_for_branch.assert_called_once_with(branch)
        self.assertEqual(self.mock_vcs_repo.get_commit_hash_for_branch.return_value,
                         cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)

    def test_resolve_branch_does_not_exist(self):
        """Test resolve_branch_to_commit_hash when branch does not exist."""
        branch = 'integration'
        cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=self.name,
                                       branch=branch)
        self.mock_vcs_repo.get_commit_hash_for_branch.return_value = None
        err_regex = f'Failed to resolve branch {branch} to commit hash. No such branch.'

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            cfs_layer.resolve_branch_to_commit_hash()

        self.mock_vcs_repo_cls.assert_called_once_with(self.clone_url)
        self.mock_vcs_repo.get_commit_hash_for_branch.assert_called_once_with(branch)

        # Commit and branch should remain unaltered
        self.assertIsNone(cfs_layer.commit)
        self.assertEqual(branch, cfs_layer.branch)

    def test_resolve_branch_vcs_error(self):
        """Test resolve_branch_to_commit_hash when there is an error in VCSRepo."""
        branch = 'integration'
        cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=self.name, branch=branch)
        vcs_err_msg = 'Error accessing VCS'
        self.mock_vcs_repo.get_commit_hash_for_branch.side_effect = VCSError(vcs_err_msg)
        err_regex = f'Failed to resolve branch {branch} to commit hash: {vcs_err_msg}'

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            cfs_layer.resolve_branch_to_commit_hash()

        self.mock_vcs_repo_cls.assert_called_once_with(self.clone_url)
        self.mock_vcs_repo.get_commit_hash_for_branch.assert_called_once_with(branch)

        # Commit and branch should remain unaltered
        self.assertIsNone(cfs_layer.commit)
        self.assertEqual(branch, cfs_layer.branch)

    def test_req_payload_commit_only(self):
        """Test req_payload property when only a commit hash is specified."""
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
        }
        expected_payload.update(self.additional_data)
        self.assertEqual(expected_payload, self.cfs_layer.req_payload)

    def test_req_payload_branch_only(self):
        """Test req_payload property when only a branch is specified."""
        branch = 'integration'
        cfs_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=self.name, branch=branch)
        expected_payload = {
            'branch': branch,
            'name': self.name
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_req_payload_branch_and_commit(self):
        """Test req_payload when branch and commit are specified."""
        branch = 'integration'
        cfs_config_layer = self.cfs_layer_cls(clone_url=self.clone_url, name=self.name,
                                              branch=branch, commit=self.commit)
        expected_payload = {
            'branch': branch,
            'commit': self.commit,
            'name': self.name
        }
        self.assertEqual(expected_payload, cfs_config_layer.req_payload)


class TestCFSV2AdditionalInventoryLayer(unittest.TestCase):
    """Tests for the CFSV2AdditionalInventoryLayer class."""

    def setUp(self):
        self.product = 'sat'
        self.repo_path = f'/vcs/cray/{self.product}-config-management.git'
        self.clone_url = f'http://api-gw-service-nmn.local{self.repo_path}'
        self.name = 'sat'
        self.commit = 'abcd1234'
        self.additional_data = {
            'special_parameters': {
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }

    def test_construct_cfs_layer(self):
        """Test creating a new CFSV2AdditionalInventoryLayer."""
        cfs_layer = CFSV2AdditionalInventoryLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, additional_data=self.additional_data
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)
        self.assertEqual(self.additional_data, cfs_layer.additional_data)

    def test_construct_cfs_layer_branch_only(self):
        """Test creating a new CFSV2AdditionalInventoryLayer with only a branch."""
        branch = 'integration'
        cfs_layer = CFSV2AdditionalInventoryLayer(
            clone_url=self.clone_url, name=self.name, branch=branch
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertIsNone(cfs_layer.commit)
        self.assertEqual(branch, cfs_layer.branch)
        self.assertEqual({}, cfs_layer.additional_data)

    def test_construct_cfs_layer_no_branch_commit(self):
        """Test creating a new CFSV2AdditionalInventoryLayer requires a git commit or branch."""
        with self.assertRaisesRegex(ValueError, 'Either commit or branch is required'):
            CFSV2AdditionalInventoryLayer(clone_url=self.clone_url)

    def test_construct_cfs_layer_no_clone_url(self):
        """Test creating a new CFSV2AdditionalInventoryLayer requires a clone URL."""
        with self.assertRaisesRegex(ValueError, 'clone_url is required'):
            CFSV2AdditionalInventoryLayer(name=self.name, commit=self.commit)

    def test_req_payload(self):
        """Test req_payload property of CFSV2AdditionalInventoryLayer."""
        cfs_layer = CFSV2ConfigurationLayer(clone_url=self.clone_url, name=self.name,
                                            commit=self.commit, additional_data=self.additional_data)
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
            'cloneUrl': self.clone_url,
            'special_parameters': {
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_from_cfs(self):
        """Test from_cfs class method of CFSV2AdditionalInventoryLayer."""
        cfs_layer_data = {
            'cloneUrl': self.clone_url,
            'commit': self.commit,
            'name': self.name
        }
        cfs_layer = CFSV2AdditionalInventoryLayer.from_cfs(cfs_layer_data)
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertEqual(self.name, cfs_layer.name)


class TestCFSV3AdditionalInventoryLayer(unittest.TestCase):
    """Tests for the CFSV3AdditionalInventoryLayer class."""
    def setUp(self):
        self.product = 'sat'
        self.repo_path = f'/vcs/cray/{self.product}-config-management.git'
        self.clone_url = f'http://api-gw-service-nmn.local{self.repo_path}'
        self.name = 'sat'
        self.commit = 'abcd1234'
        self.additional_data = {
            'special_parameters': {
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }

    def test_construct_cfs_layer(self):
        """Test creating a new CFSV3AdditionalInventoryLayer."""
        cfs_layer = CFSV3AdditionalInventoryLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, additional_data=self.additional_data
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertIsNone(cfs_layer.source)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)
        self.assertEqual(self.additional_data, cfs_layer.additional_data)

    def test_construct_cfs_layer_branch_only(self):
        """Test creating a new CFSV3AdditionalInventoryLayer with only a branch."""
        branch = 'integration'
        cfs_layer = CFSV3AdditionalInventoryLayer(
            clone_url=self.clone_url, name=self.name, branch=branch
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertIsNone(cfs_layer.source)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertIsNone(cfs_layer.commit)
        self.assertEqual(branch, cfs_layer.branch)
        self.assertEqual({}, cfs_layer.additional_data)

    def test_construct_cfs_layer_source(self):
        """Test creating a new CFSV3AdditionalInventoryLayer with a source name."""
        cfs_layer = CFSV3AdditionalInventoryLayer(source='my-source', name=self.name,
                                                  commit=self.commit)
        self.assertEqual('my-source', cfs_layer.source)
        self.assertIsNone(cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)
        self.assertEqual({}, cfs_layer.additional_data)

    def test_construct_cfs_layer_no_branch_commit(self):
        """Test creating a new CFSV3AdditionalInventoryLayer requires a git commit or branch"""
        with self.assertRaisesRegex(ValueError, 'Either commit or branch is required'):
            CFSV3AdditionalInventoryLayer(clone_url=self.clone_url)

    def test_construct_cfs_layer_no_clone_url_or_source(self):
        """Test creating a new CFSV3AdditionalInventoryLayer requires a clone URL or source name"""
        with self.assertRaisesRegex(ValueError, 'source or clone_url is required'):
            CFSV3AdditionalInventoryLayer(name=self.name, commit=self.commit)

    def test_repo_short_name_with_clone_url(self):
        """Test the repo_short_name property of CFSV3AdditionalInventoryLayer with clone_url specified"""
        cfs_layer = CFSV3AdditionalInventoryLayer(clone_url=self.clone_url, name=self.name,
                                                  commit=self.commit)
        self.assertEqual(self.product, cfs_layer.repo_short_name)

    def test_repo_short_name_with_source(self):
        """Test the repo_short_name property of CFSV3AdditionalInventoryLayer with source specified"""
        cfs_layer = CFSV3AdditionalInventoryLayer(source='my-source', name=self.name,
                                                  commit=self.commit)
        self.assertEqual('my-source', cfs_layer.repo_short_name)

    def test_str_with_clone_url(self):
        """Test the __str__ method with clone_url specified"""
        cfs_layer = CFSV3AdditionalInventoryLayer(clone_url=self.clone_url, name=self.name,
                                                  commit=self.commit)
        self.assertEqual(f'CFSV3AdditionalInventoryLayer with repo path {self.repo_path}', str(cfs_layer))

    def test_str_with_source(self):
        """Test the __str__ method with source specified"""
        cfs_layer = CFSV3AdditionalInventoryLayer(source='my-source', name=self.name,
                                                  commit=self.commit)
        self.assertEqual('CFSV3AdditionalInventoryLayer with source my-source', str(cfs_layer))

    def test_req_payload(self):
        """Test req_payload property of CFSV3AdditionalInventoryLayer."""
        cfs_layer = CFSV3AdditionalInventoryLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, additional_data=self.additional_data
        )
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
            'clone_url': self.clone_url,
            'special_parameters': {
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_from_cfs(self):
        """Test from_cfs class method of CFSV3AdditionalInventoryLayer."""
        cfs_layer_data = {
            'clone_url': self.clone_url,
            'commit': self.commit,
            'name': self.name
        }
        cfs_layer = CFSV3AdditionalInventoryLayer.from_cfs(cfs_layer_data)
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertEqual(self.name, cfs_layer.name)


class TestCFSV2ConfigurationLayer(unittest.TestCase):
    """Tests for CFSV2ConfigurationLayer class."""

    def setUp(self):
        self.product = 'sat'
        self.repo_path = f'/vcs/cray/{self.product}-config-management.git'
        self.clone_url = f'http://api-gw-service-nmn.local{self.repo_path}'
        self.name = 'sat'
        self.commit = 'abcd1234'
        self.playbook = 'do-things.yml'
        self.additional_data = {
            'specialParameters': {
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }
        self.mock_datetime = patch('csm_api_client.service.cfs.datetime', wraps=datetime.datetime).start()
        self.static_datetime = datetime.datetime(2024, 8, 25, 11, 11, 11)
        self.mock_datetime.now.return_value = self.static_datetime
        self.expected_timestamp = '20240825T111111'

    def test_construct_cfs_layer(self):
        """Test creating a new CFSV2ConfigurationLayer."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            additional_data=self.additional_data
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)
        self.assertEqual(self.playbook, cfs_layer.playbook)
        self.assertIsNone(cfs_layer.ims_require_dkms)
        self.assertEqual(self.additional_data, cfs_layer.additional_data)

    def test_construct_cfs_layer_ims_require_dkms(self):
        """Test creating a new CFSV2ConfigurationLayer with ims_require_dkms specified."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            ims_require_dkms=True,
            additional_data=self.additional_data
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)
        self.assertEqual(self.playbook, cfs_layer.playbook)
        self.assertTrue(cfs_layer.ims_require_dkms)
        self.assertEqual(self.additional_data, cfs_layer.additional_data)

    def test_construct_cfs_layer_no_playbook(self):
        """Test creating a new CFSV2ConfigurationLayer without a playbook."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit,
            ims_require_dkms=True,
            additional_data=self.additional_data
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)
        self.assertIsNone(cfs_layer.playbook)
        self.assertTrue(cfs_layer.ims_require_dkms)
        self.assertEqual(self.additional_data, cfs_layer.additional_data)

    def test_str_with_playbook(self):
        """Test __str__ method of CFSV2ConfigurationLayer with playbook specified."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook
        )
        self.assertEqual(f'CFSV2ConfigurationLayer with repo path {self.repo_path} '
                         f'and playbook {self.playbook}', str(cfs_layer))

    def test_str_no_playbook(self):
        """Test __str__ method of CFSV2ConfigurationLayer without playbook specified."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit
        )
        self.assertEqual(f'CFSV2ConfigurationLayer with repo path {self.repo_path} '
                         f'and default playbook', str(cfs_layer))

    def test_name_when_specified(self):
        """Test name property of CFSV2ConfigurationLayer when name is specified."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook
        )
        self.assertEqual(self.name, cfs_layer.name)

    def test_name_with_commit_and_playbook(self):
        """Test name property of CFSV2ConfigurationLayer with commit and playbook specified."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url,
            commit=self.commit, playbook=self.playbook
        )
        self.assertEqual(f'{self.product}-do-things-{self.commit[:7]}-{self.expected_timestamp}',
                         cfs_layer.name)

    def test_name_with_branch_and_playbook(self):
        """Test name property of CFSV2ConfigurationLayer with branch and playbook specified."""
        branch = 'integration'
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, branch=branch,
            playbook=self.playbook
        )
        self.assertEqual(f'{self.product}-do-things-{branch[:7]}-{self.expected_timestamp}',
                         cfs_layer.name)

    def test_name_no_playbook(self):
        """Test name property of CFSV2ConfigurationLayer with no playbook specified."""
        cfs_layer = CFSV2ConfigurationLayer(clone_url=self.clone_url, commit=self.commit)
        self.assertEqual(f'{self.product}-site-{self.commit[:7]}-{self.expected_timestamp}',
                         cfs_layer.name)

    def test_req_payload(self):
        """Test req_payload property of CFSV2ConfigurationLayer."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            additional_data={'foo': 'bar', 'baz': {'bat': 'qux'}}
        )
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
            'cloneUrl': self.clone_url,
            'playbook': self.playbook,
            'foo': 'bar',
            'baz': {'bat': 'qux'}
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_payload_with_ims_require_dkms(self):
        """Test req_payload property of CFSV2ConfigurationLayer with ims_require_dkms set to True."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            ims_require_dkms=True, additional_data=self.additional_data
        )
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
            'cloneUrl': self.clone_url,
            'playbook': self.playbook,
            'specialParameters': {
                'imsRequireDkms': True,
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_payload_with_ims_require_dkms_false(self):
        """Test req_payload property of CFSV2ConfigurationLayer with ims_require_dkms set to False."""
        cfs_layer = CFSV2ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            ims_require_dkms=False, additional_data=self.additional_data
        )
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
            'cloneUrl': self.clone_url,
            'playbook': self.playbook,
            'specialParameters': {
                'imsRequireDkms': False,
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_from_cfs(self):
        """Test from_cfs class method of CFSV2ConfigurationLayer."""
        cfs_layer_data = {
            'cloneUrl': self.clone_url,
            'commit': self.commit,
            'name': self.name,
            'playbook': self.playbook,
            'specialParameters': { 'imsRequireDkms': True }
        }
        cfs_layer = CFSV2ConfigurationLayer.from_cfs(cfs_layer_data)
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.playbook, cfs_layer.playbook)
        self.assertTrue(cfs_layer.ims_require_dkms)


# Create a test class for CFSV3ConfigurationLayer just like the tests for CFSV2ConfigurationLayer
class TestCFSV3ConfigurationLayer(unittest.TestCase):
    """Tests for the CFSV3ConfigurationLayer class."""

    def setUp(self):
        self.product = 'sat'
        self.repo_path = f'/vcs/cray/{self.product}-config-management.git'
        self.clone_url = f'http://api-gw-service-nmn.local{self.repo_path}'
        self.name = 'sat'
        self.commit = 'abcd1234'
        self.playbook = 'do-things.yml'
        self.additional_data = {
            'special_parameters': {
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }
        self.mock_datetime = patch('csm_api_client.service.cfs.datetime', wraps=datetime.datetime).start()
        self.static_datetime = datetime.datetime(2024, 8, 25, 11, 11, 11)
        self.mock_datetime.now.return_value = self.static_datetime
        self.expected_timestamp = '20240825T111111'

    def test_construct_cfs_layer(self):
        """Test creating a new CFSV3ConfigurationLayer."""
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            additional_data=self.additional_data
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)
        self.assertEqual(self.playbook, cfs_layer.playbook)
        self.assertIsNone(cfs_layer.ims_require_dkms)
        self.assertEqual(self.additional_data, cfs_layer.additional_data)

    def test_construct_cfs_layer_ims_require_dkms(self):
        """Test creating a new CFSV3ConfigurationLayer with ims_require_dkms specified."""
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            ims_require_dkms=True,
            additional_data=self.additional_data
        )
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertIsNone(cfs_layer.branch)
        self.assertEqual(self.playbook, cfs_layer.playbook)
        self.assertTrue(cfs_layer.ims_require_dkms)
        self.assertEqual(self.additional_data, cfs_layer.additional_data)

    def test_construct_cfs_layer_no_playbook(self):
        """Test creating a new CFSV3ConfigurationLayer without a playbook."""
        with self.assertRaisesRegex(TypeError, "missing 1 required positional argument: 'playbook'"):
            CFSV3ConfigurationLayer(
                clone_url=self.clone_url, name=self.name,
                commit=self.commit, additional_data=self.additional_data
            )

    def test_str_with_clone_url(self):
        """Test __str__ method of CFSV3ConfigurationLayer with clone_url specified."""
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook
        )
        self.assertEqual(f'CFSV3ConfigurationLayer with repo path {self.repo_path} '
                         f'and playbook {self.playbook}', str(cfs_layer))

    def test_str_with_source(self):
        """Test __str__ method of CFSV3ConfigurationLayer with source specified."""
        cfs_layer = CFSV3ConfigurationLayer(
            source='my-source', name=self.name,
            commit=self.commit, playbook=self.playbook
        )
        self.assertEqual('CFSV3ConfigurationLayer with source my-source '
                         f'and playbook {self.playbook}', str(cfs_layer))

    def test_name_when_specified(self):
        """Test name property of CFSV3ConfigurationLayer when name is specified."""
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook
        )
        self.assertEqual(self.name, cfs_layer.name)

    def test_name_with_commit_and_playbook(self):
        """Test name property of CFSV3ConfigurationLayer with commit and playbook specified."""
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url,
            commit=self.commit, playbook=self.playbook
        )
        self.assertEqual(f'{self.product}-do-things-{self.commit[:7]}-{self.expected_timestamp}',
                         cfs_layer.name)

    def test_name_with_branch_and_playbook(self):
        """Test name property of CFSV3ConfigurationLayer with branch and playbook specified."""
        branch = 'integration'
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url, branch=branch,
            playbook=self.playbook
        )
        self.assertEqual(f'{self.product}-do-things-{branch[:7]}-{self.expected_timestamp}',
                         cfs_layer.name)

    def test_req_payload(self):
        """Test req_payload property of CFSV2ConfigurationLayer."""
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            additional_data={'foo': 'bar', 'baz': {'bat': 'qux'}}
        )
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
            'clone_url': self.clone_url,
            'playbook': self.playbook,
            'foo': 'bar',
            'baz': {'bat': 'qux'}
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_payload_with_ims_require_dkms(self):
        """Test req_payload property of CFSV3ConfigurationLayer with ims_require_dkms set to True"""
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            ims_require_dkms=True, additional_data=self.additional_data
        )
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
            'clone_url': self.clone_url,
            'playbook': self.playbook,
            'special_parameters': {
                'ims_require_dkms': True,
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_payload_with_ims_require_dkms_false(self):
        """Test req_payload property of CFSV3ConfigurationLayer with ims_require_dkms set to False"""
        cfs_layer = CFSV3ConfigurationLayer(
            clone_url=self.clone_url, name=self.name,
            commit=self.commit, playbook=self.playbook,
            ims_require_dkms=False, additional_data=self.additional_data
        )
        expected_payload = {
            'commit': self.commit,
            'name': self.name,
            'clone_url': self.clone_url,
            'playbook': self.playbook,
            'special_parameters': {
                'ims_require_dkms': False,
                'future_cfs_layer_special_parameter': 'special_value'
            }
        }
        self.assertEqual(expected_payload, cfs_layer.req_payload)

    def test_from_cfs(self):
        """Test from_cfs class method of CFSV3ConfigurationLayer."""
        cfs_layer_data = {
            'clone_url': self.clone_url,
            'commit': self.commit,
            'name': self.name,
            'playbook': self.playbook,
            'special_parameters': { 'ims_require_dkms': True }
        }
        cfs_layer = CFSV3ConfigurationLayer.from_cfs(cfs_layer_data)
        self.assertEqual(self.clone_url, cfs_layer.clone_url)
        self.assertEqual(self.commit, cfs_layer.commit)
        self.assertEqual(self.name, cfs_layer.name)
        self.assertEqual(self.playbook, cfs_layer.playbook)
        self.assertTrue(cfs_layer.ims_require_dkms)


class TestCFSLayersMatch(unittest.TestCase):
    """Tests for the matches method of CFSLayerBase subclasses."""

    def setUp(self):
        self.clone_url = 'http://api-gw-service-nmn.local/vcs/cray/sat-config-management.git'
        self.clone_url_diff_host = 'http://api-gw-service.nmn.local/vcs/cray/sat-config-management.git'
        self.other_clone_url = 'http://api-gw-service.nmn.local/vcs/cray/other-config-management.git'
        self.playbook = 'do-things.yml'
        self.other_playbook = 'do-other-things.yml'

    def assert_matches(self, layer_1, layer_2):
        """Assert the given layer matches `self.cfs_config_layer`.

        Args:
            layer_1: the first layer
            layer_2: the second layer
        """
        # Check both ways to ensure symmetric relationship
        self.assertTrue(layer_1.matches(layer_2))
        self.assertTrue(layer_2.matches(layer_1))

    def assert_does_not_match(self, layer_1, layer_2):
        """Assert the given layer does not match `self.cfs_config_layer`.

        Args:
            layer_1: the first layer
            layer_2: the second layer
        """
        # Check both ways to ensure symmetric relationship
        self.assertFalse(layer_1.matches(layer_2))
        self.assertFalse(layer_2.matches(layer_1))

    def test_additional_inventory_layers_match(self):
        """Test matches method against matching additional inventory layers"""
        for layer_cls in [CFSV2AdditionalInventoryLayer, CFSV3AdditionalInventoryLayer]:
            # Two layers with the same clone_url and name should match
            layer_1 = layer_cls(clone_url=self.clone_url, name='sat', commit='abcd1234')
            layer_2 = layer_cls(clone_url=self.clone_url_diff_host, name='sat', commit='5678aef')
            self.assert_matches(layer_1, layer_2)

    def test_additional_inventory_layers_do_not_match(self):
        """Test matches method against non-matching additional inventory layers"""
        for layer_cls in [CFSV2AdditionalInventoryLayer, CFSV3AdditionalInventoryLayer]:
            non_matching_pairs = (
                (layer_cls(clone_url=self.clone_url, name='first', commit='abcd1234'),
                 layer_cls(clone_url=self.other_clone_url, name='second', commit='abcd1234')),
            )
            for layer_1, layer_2 in non_matching_pairs:
                self.assert_does_not_match(layer_1, layer_2)

    def test_additional_inventory_layers_v3_with_source_matches(self):
        """Test matches against CFSV3AdditionalInventoryLayer with source name."""
        layer_1 = CFSV3AdditionalInventoryLayer(source='my-source', name='sat', commit='abcd1234')
        layer_2 = CFSV3AdditionalInventoryLayer(source='my-source', name='sat', commit='5678aef')
        self.assert_matches(layer_1, layer_2)

    def test_additional_inventory_layers_v3_with_source_does_not_match(self):
        layer_1 = CFSV3AdditionalInventoryLayer(source='my-source', name='sat', commit='abcd1234')
        layer_2 = CFSV3AdditionalInventoryLayer(source='other-source', name='sat', commit='abcd1234')
        self.assert_does_not_match(layer_1, layer_2)

    def test_configuration_layers_match(self):
        """Test matches method against matching configuration layers"""
        for layer_cls in [CFSV2ConfigurationLayer, CFSV3ConfigurationLayer]:
            # Two layers with the same clone_url and playbook path should match,
            # even if the clone_url differs slightly
            layer_1 = layer_cls(clone_url=self.clone_url, name='sat',
                                playbook=self.playbook, commit='abcd1234')
            layer_2 = layer_cls(clone_url=self.clone_url_diff_host, name='other-name',
                                playbook=self.playbook, commit='5678aef')
            self.assert_matches(layer_1, layer_2)

    def test_configuration_layers_do_not_match(self):
        """Test matches method against non-matching layers"""
        for layer_cls in [CFSV2ConfigurationLayer, CFSV3ConfigurationLayer]:
            non_matching_pairs = (
                (
                    layer_cls(clone_url=self.clone_url, name='first',
                              playbook=self.playbook, commit='abcd1234'),
                    layer_cls(clone_url=self.other_clone_url, name='second',
                              playbook=self.playbook, commit='abcd1234')
                ),
                (
                    layer_cls(clone_url=self.clone_url, name='first',
                              playbook=self.playbook, commit='abcd1234'),
                    layer_cls(clone_url=self.clone_url, name='second',
                              playbook=self.other_playbook, commit='abcd1234')
                ),
                (
                    layer_cls(clone_url=self.clone_url, name='first',
                              playbook=self.playbook, commit='abcd1234',
                              ims_require_dkms=True),
                    layer_cls(clone_url=self.clone_url, name='second',
                              playbook=self.playbook, commit='5678aef',
                              ims_require_dkms=False)
                ),
            )
            for layer_1, layer_2 in non_matching_pairs:
                self.assert_does_not_match(layer_1, layer_2)

    def test_configuration_layers_v3_with_source_matches(self):
        """Test matches against CFSV3ConfigurationLayer with source name."""
        layer_1 = CFSV3ConfigurationLayer(source='my-source', name='sat',
                                          playbook=self.playbook, commit='abcd1234',
                                          ims_require_dkms=True)
        layer_2 = CFSV3ConfigurationLayer(source='my-source', name='sat',
                                          playbook=self.playbook, commit='5678aef',
                                          ims_require_dkms=True)
        self.assert_matches(layer_1, layer_2)

    def test_configuration_layers_v3_with_source_does_not_match(self):
        layer_1 = CFSV3ConfigurationLayer(source='my-source', name='sat',
                                          playbook=self.playbook, commit='abcd1234')
        layer_2 = CFSV3ConfigurationLayer(source='other-source', name='sat',
                                          playbook=self.playbook, commit='abcd1234')
        self.assert_does_not_match(layer_1, layer_2)

    def test_matches_mismatch_wrong_types(self):
        """Test matches method against an incorrect type pairings."""
        non_matching_pairs = (
            (
                CFSV2ConfigurationLayer(clone_url=self.clone_url, name='sat',
                                        playbook=self.playbook, commit='abcd1234'),
                CFSV2AdditionalInventoryLayer(clone_url=self.clone_url, name='sat', commit='abcd1234')
            ),
            (
                CFSV3ConfigurationLayer(clone_url=self.clone_url, name='sat',
                                        playbook=self.playbook, commit='abcd1234'),
                CFSV3AdditionalInventoryLayer(clone_url=self.clone_url, name='sat', commit='abcd1234')
            ),
            (
                CFSV2ConfigurationLayer(clone_url=self.clone_url, name='sat',
                                        playbook=self.playbook, commit='abcd1234'),
                CFSV3ConfigurationLayer(clone_url=self.clone_url, name='sat',
                                        playbook=self.playbook, commit='abcd1234')
            ),
            (
                CFSV2AdditionalInventoryLayer(clone_url=self.clone_url, name='sat', commit='abcd1234'),
                CFSV3AdditionalInventoryLayer(clone_url=self.clone_url, name='sat', commit='abcd1234')
            ),
        )
        for layer_1, layer2 in non_matching_pairs:
            self.assert_does_not_match(layer_1, layer2)


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

    def tearDown(self):
        patch.stopall()

    def test_product_defaults_with_product_catalog(self):
        """Test from_product_catalog with defaults from product entry."""
        layer = CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host,
                                                  product_version=self.product_version,
                                                  product_catalog=self.mock_product_catalog)
        # The method should not create a new ProductCatalog instance since it was given one
        self.mock_product_catalog_cls.assert_not_called()
        self.mock_product_catalog.get_product.assert_called_once_with(self.product_name,
                                                                      self.product_version)
        self.assertEqual(self.expected_clone_url, layer.clone_url)
        self.assertEqual(self.product_commit, layer.commit)
        self.assertIsNone(layer.branch)

    def test_product_default_no_product_catalog(self):
        """Test from_product_catalog with defaults from product entry when not given a product catalog."""
        layer = CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host,
                                                  product_version=self.product_version)
        # The method should create a new ProductCatalog instance since it was not given one
        self.mock_product_catalog_cls.assert_called_once_with()
        self.mock_product_catalog.get_product.assert_called_once_with(self.product_name,
                                                                      self.product_version)
        self.assertEqual(self.expected_clone_url, layer.clone_url)
        self.assertEqual(self.product_commit, layer.commit)
        self.assertIsNone(layer.branch)

    def test_product_no_version_with_commit_hash(self):
        """Test from_product_catalog when a commit hash is specified."""
        alternate_commit = 'a1b2c3d'
        layer = CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host,
                                                  commit=alternate_commit)
        self.mock_product_catalog_cls.assert_called_once_with()
        self.mock_product_catalog.get_product.assert_called_once_with(self.product_name, None)
        self.assertEqual(self.expected_clone_url, layer.clone_url)
        self.assertEqual(alternate_commit, layer.commit)
        self.assertIsNone(layer.branch)

    def test_product_with_branch(self):
        """Test from_product_catalog when a branch is specified."""
        branch = 'integration'
        layer = CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host,
                                                  product_version=self.product_version,
                                                  branch=branch)
        self.mock_product_catalog_cls.assert_called_once_with()
        self.mock_product_catalog.get_product.assert_called_once_with(self.product_name,
                                                                      self.product_version)
        self.assertEqual(self.expected_clone_url, layer.clone_url)
        self.assertEqual(branch, layer.branch)
        self.assertIsNone(layer.commit)

    def test_product_custom_layer_name(self):
        """Test from_product_catalog with a custom layer name."""
        custom_layer_name = f'custom-{self.product_name}'

        layer = CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host,
                                                  name=custom_layer_name)
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
            CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host)

    def test_product_unknown_version(self):
        """Test from_product_catalog when unable to find the requested version of the product."""
        pc_err_msg = 'unable to find that version'
        self.mock_product_catalog.get_product.side_effect = ProductCatalogError(pc_err_msg)
        err_regex = (f'Failed to create CFS configuration layer for version {self.product_version} '
                     f'of product {self.product_name}: {pc_err_msg}')

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host,
                                              product_version=self.product_version)

    def test_product_missing_commit(self):
        """Test from_product_catalog when unable to find a commit hash for the product."""
        self.mock_product.commit = None
        err_regex = (f'Failed to create CFS configuration layer for product '
                     f'{self.product_name}: .* has no commit hash')

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host)

    def test_product_missing_clone_url(self):
        """Test from_product_catalog when unable to find a clone URL for the product."""
        self.mock_product.clone_url = None
        err_regex = (f'Failed to create CFS configuration layer for product '
                     f'{self.product_name}: .* has no clone URL')

        with self.assertRaisesRegex(CFSConfigurationError, err_regex):
            CFSLayerBase.from_product_catalog(self.product_name, self.api_gw_host)

    def test_api_gw_host_with_url_scheme(self):
        """Test from_product_catalog when a URL scheme is added to the URL."""
        layer = CFSLayerBase.from_product_catalog(self.product_name, f'https://{self.api_gw_host}')
        self.assertEqual(layer.clone_url, self.expected_clone_url)


class TestCFSConfigurationLayerFromCloneUrl(unittest.TestCase):
    """Tests for CFSConfigurationLayer.from_clone_url class method."""

    def test_from_clone_url(self):
        """Test from_clone_url."""
        for cls in (CFSV2ConfigurationLayer, CFSV3ConfigurationLayer):
            short_name = 'cray'
            clone_url = f'http://api-gw/vcs/{short_name}-config-management.git'
            playbook = 'cray.yml'
            branch = 'main'
            layer = cls.from_clone_url(clone_url, playbook=playbook, branch=branch)
            self.assertEqual(branch, layer.branch)
            self.assertEqual(clone_url, layer.clone_url)
            self.assertIsNone(layer.commit)

    def test_from_clone_url_custom_layer_name(self):
        """Test from_clone_url with custom layer name."""
        for cls in (CFSV2ConfigurationLayer, CFSV3ConfigurationLayer):
            clone_url = 'http://api-gw/vcs/hpe-config-management.git'
            playbook = 'hpe.yml'
            branch = 'main'
            custom_layer_name = 'hpe'
            layer = cls.from_clone_url(clone_url, name=custom_layer_name,
                                       playbook=playbook, branch=branch)

            self.assertEqual(custom_layer_name, layer.name)
            self.assertEqual(branch, layer.branch)
            self.assertEqual(clone_url, layer.clone_url)
            self.assertIsNone(layer.commit)


class MockFile(io.StringIO):
    """An in-memory file object which is not automatically closed"""
    def __exit__(self, *_):
        pass


class TestCFSV2Configuration(unittest.TestCase):
    """Tests for the CFSV2Configuration class."""
    def setUp(self):
        self.mock_cfs_client = Mock(spec=CFSV2Client)

        self.example_layer_data = {
            "cloneUrl": "https://api-gw-service-nmn.local/vcs/cray/example-config-management.git",
            "commit": "123456789abcdef",
            "name": "example-config",
            "playbook": "example-config.yml"
        }
        self.example_layer = CFSV2ConfigurationLayer.from_cfs(self.example_layer_data)

        self.new_layer_data = {
            "cloneUrl": "https://api-gw-service-nmn.local/vcs/cray/new-config-management.git",
            "commit": "fedcba987654321",
            "name": "new-config",
            "playbook": "new-config.yml"
        }
        self.new_layer = CFSV2ConfigurationLayer.from_cfs(self.new_layer_data)

        self.dkms_layer_data = {
            "cloneUrl": "https://api-gw-service-nmn.local/vcs/cray/cos-config-management.git",
            "commit": "abcdef123456789",
            "name": "cos-config",
            "playbook": "cos-config.yml",
            "specialParameters": {
                "imsRequireDkms": True
            }
        }
        self.dkms_layer = CFSV2ConfigurationLayer.from_cfs(self.dkms_layer_data)

        self.single_layer_config_data = {
            "lastUpdated": "2021-10-20T21:26:04Z",
            "layers": [
                deepcopy(self.example_layer_data)
            ],
            "name": "single-layer-config"
        }
        self.single_layer_config = CFSV2Configuration(self.mock_cfs_client,
                                                      self.single_layer_config_data)

        self.duplicate_layer_config_data = {
            "lastUpdated": "2021-10-20T21:26:04Z",
            "layers": [
                deepcopy(self.example_layer_data),
                deepcopy(self.example_layer_data)
            ],
            "name": "duplicate-layer-config"
        }
        self.duplicate_layer_config = CFSV2Configuration(self.mock_cfs_client,
                                                         self.duplicate_layer_config_data)

        # A configuration with multiple layers
        self.multiple_layer_config_data = {
            "lastUpdated": "2021-10-20T21:26:04Z",
            "layers": [
                deepcopy(self.example_layer_data),
                deepcopy(self.dkms_layer_data)
            ],
            "name": "multiple-layer-config",
        }
        self.multiple_layer_config = CFSV2Configuration(self.mock_cfs_client,
                                                        self.multiple_layer_config_data)

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

    @staticmethod
    def get_put_configuration_call(config_name, layers):
        """Return the expected call to put_configuration with the given parameters."""
        return call(config_name, {'layers': layers}, request_params=None)

    def assert_put_configuration_called(self, config_name, layers):
        """Assert that put_configuration was called with the given parameters."""
        self.mock_cfs_client.put_configuration.assert_called_once_with(
            config_name, {'layers': layers}, request_params=None
        )

    @patch('csm_api_client.service.cfs.CFSLayerBase.from_cfs')
    def test_construct_cfs_configuration(self, mock_from_cfs):
        """Test the CFSConfiguration constructor."""
        cfs_config = CFSV2Configuration(self.mock_cfs_client, self.single_layer_config_data)
        self.assertEqual('single-layer-config', cfs_config.name)
        self.assertEqual([mock_from_cfs.return_value], cfs_config.layers)
        self.assertIsNone(cfs_config.additional_inventory)

    @patch('csm_api_client.service.cfs.CFSLayerBase.from_cfs')
    def test_cfs_configuration_preserves_additional_data(self, mock_from_cfs):
        """Test that CFSConfiguration preserves additional data."""
        config_data = deepcopy(self.single_layer_config_data)
        config_data['newProperty'] = 'newValue'
        config_data['specialParameters'] = {'foo': 'bar'}

        cfs_config = CFSV2Configuration(self.mock_cfs_client, config_data)

        self.assertEqual('single-layer-config', cfs_config.name)
        self.assertEqual([mock_from_cfs.return_value], cfs_config.layers)
        self.assertEqual('newValue', cfs_config.passthrough_data['newProperty'])
        self.assertEqual({'foo': 'bar'}, cfs_config.passthrough_data['specialParameters'])

        req_payload = cfs_config.req_payload
        self.assertEqual('newValue', req_payload['newProperty'])
        self.assertEqual({'foo': 'bar'}, req_payload['specialParameters'])

    def test_cfs_configuration_with_additional_inventory(self):
        """Test that CFSConfiguration can handle additional inventory layer."""
        config_data = deepcopy(self.single_layer_config_data)
        config_data['additional_inventory'] = {
            "cloneUrl": "https://api-gw-service-nmn.local/vcs/cray/inventory.git",
            "commit": "987654321abcdef",
            "name": "inventory",
        }

        with patch('csm_api_client.service.cfs.CFSV2AdditionalInventoryLayer.from_cfs') as mock_from_cfs:
            cfs_config = CFSV2Configuration(self.mock_cfs_client, config_data)

        self.assertEqual('single-layer-config', cfs_config.name)
        self.assertEqual(1, len(cfs_config.layers))
        self.assertEqual(mock_from_cfs.return_value, cfs_config.additional_inventory)

    def test_construct_empty_configuration(self):
        """Test creating an empty configuration"""
        self.assertEqual(CFSV2Configuration.empty(self.mock_cfs_client).layers, [])

    def test_req_payload(self):
        """Test the req_payload method of CFSConfiguration."""
        self.assertEqual({'layers': [self.example_layer_data]},
                         self.single_layer_config.req_payload)

    @patch('csm_api_client.service.cfs.CFSLayerBase.from_cfs')
    def test_save_to_cfs(self, mock_from_cfs):
        """Test that save_to_cfs properly calls the CFS API."""
        config_name = self.single_layer_config_data['name']
        layers = self.single_layer_config_data['layers']
        self.mock_cfs_client.put_configuration.return_value = self.single_layer_config_data

        saved_config = self.single_layer_config.save_to_cfs()

        self.assert_put_configuration_called(config_name, layers)
        self.assertIsInstance(saved_config, CFSV2Configuration)
        self.assertEqual(saved_config.name, config_name)
        self.assertEqual([mock_from_cfs.return_value], saved_config.layers)

    @patch('csm_api_client.service.cfs.CFSLayerBase.from_cfs')
    def test_save_to_cfs_new_name(self, mock_from_cfs):
        """Test that save_to_cfs saves to a new name."""
        new_name = 'new-config-name'
        layers = self.single_layer_config_data['layers']

        expected_saved_config_data = deepcopy(self.single_layer_config_data)
        expected_saved_config_data['name'] = new_name
        self.mock_cfs_client.put_configuration.return_value = expected_saved_config_data

        saved_config = self.single_layer_config.save_to_cfs(new_name)

        self.assert_put_configuration_called(new_name, layers)
        self.assertIsInstance(saved_config, CFSV2Configuration)
        self.assertEqual(saved_config.name, new_name)
        self.assertEqual([mock_from_cfs.return_value], saved_config.layers)

    def test_save_to_cfs_no_overwrite(self):
        """Test preventing overwriting an existing CFS configuration"""
        self.mock_cfs_client.get.return_value.status_code = 200
        config_name = self.single_layer_config_data['name']
        with self.assertRaisesRegex(CFSConfigurationError, 'already exists'):
            self.single_layer_config.save_to_cfs(config_name, overwrite=False)

    def test_save_to_cfs_api_failure(self):
        """Test that save_to_cfs raises an exception if CFS API request fails."""
        api_err_msg = 'cfs problem'
        self.mock_cfs_client.put_configuration.side_effect = APIError(api_err_msg)

        with self.assertRaisesRegex(CFSConfigurationError, api_err_msg):
            self.single_layer_config.save_to_cfs()

    @patch('csm_api_client.service.cfs.CFSLayerBase.from_cfs')
    def test_save_to_cfs_backup_when_existing(self, mock_from_cfs):
        """Test save_to_cfs creates a backup when the configuration already exists."""
        # Mock get to return an existing configuration; use deepcopy because it gets modified with pop
        self.mock_cfs_client.get.return_value.ok = True
        self.mock_cfs_client.get.return_value.json.return_value = deepcopy(self.single_layer_config_data)

        config_name = self.single_layer_config_data['name']
        layers = self.single_layer_config_data['layers']
        backup_suffix = '.backup'
        expected_backup_config_data = deepcopy(self.single_layer_config_data)
        expected_backup_config_data['name'] = f'{config_name}{backup_suffix}'

        # Mock put_configuration to return the backup configuration and then the updated configuration
        self.mock_cfs_client.put_configuration.side_effect = [
            expected_backup_config_data,
            self.single_layer_config_data
        ]

        saved_config = self.single_layer_config.save_to_cfs(config_name, backup_suffix=backup_suffix)

        # The first call to put should be to save the backup copy with no additional request parameters
        # The next call is to save the requested configuration with the additional request parameters
        self.mock_cfs_client.put_configuration.assert_has_calls([
            call(f'{config_name}{backup_suffix}', {'layers': layers}),
            self.get_put_configuration_call(config_name, layers)
        ])

        self.assertIsInstance(saved_config, CFSV2Configuration)
        self.assertEqual(saved_config.name, config_name)
        self.assertEqual([mock_from_cfs.return_value], saved_config.layers)

    @patch('csm_api_client.service.cfs.CFSLayerBase.from_cfs')
    def test_save_to_cfs_backup_when_not_existing(self, mock_from_cfs):
        """Test save_to_cfs does not create a backup when the configuration does not exist"""
        self.mock_cfs_client.get.return_value.ok = False
        self.mock_cfs_client.get.return_value.status_code = 404
        layers = self.single_layer_config_data['layers']
        new_name = 'some-new-name'
        expected_saved_config_data = deepcopy(self.single_layer_config_data)
        expected_saved_config_data['name'] = new_name
        self.mock_cfs_client.put_configuration.return_value = expected_saved_config_data

        saved_config = self.single_layer_config.save_to_cfs(new_name, backup_suffix='.backup')

        self.assert_put_configuration_called(new_name, layers)
        self.assertIsInstance(saved_config, CFSV2Configuration)
        self.assertEqual(saved_config.name, new_name)
        self.assertEqual([mock_from_cfs.return_value], saved_config.layers)

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

    def test_add_new_layer_complicated_config(self):
        """"Test adding a new layer to a more complicated CFSConfiguration"""
        self.multiple_layer_config.ensure_layer(self.new_layer, state=LayerState.PRESENT)

        self.assertEqual(3, len(self.multiple_layer_config.layers))
        self.assertEqual(self.new_layer, self.multiple_layer_config.layers[2])

    def test_update_layer_with_dkms_property(self):
        """Test updating a layer with the imsRequireDkms special parameter."""
        dkms_config = CFSV2Configuration(
            self.mock_cfs_client,
            {
                'name': 'dkms-layer-config',
                'layers': [self.dkms_layer_data],
                'lastUpdated': '2021-10-20T21:26:04Z'
            }
        )
        new_commit_hash = 'fedcba987654321'
        updated_layer_data = deepcopy(self.dkms_layer_data)
        updated_layer_data['commit'] = new_commit_hash
        updated_layer = CFSV2ConfigurationLayer.from_cfs(updated_layer_data)
        dkms_config.ensure_layer(updated_layer, state=LayerState.PRESENT)

        self.assertEqual([updated_layer], dkms_config.layers)

    def test_add_new_layer_when_dkms_differs(self):
        """Test adding a new layer when the imsRequireDkms property differs."""
        non_dkms_layer_data = deepcopy(self.dkms_layer_data)
        del non_dkms_layer_data['specialParameters']
        non_dkms_layer = CFSV2ConfigurationLayer.from_cfs(non_dkms_layer_data)

        # Since this layer now differs in that it does not require dkms, it should be added
        # instead of modifying the similar layer
        self.multiple_layer_config.ensure_layer(non_dkms_layer, state=LayerState.PRESENT)

        self.assertEqual(3, len(self.multiple_layer_config.layers))
        self.assertEqual(non_dkms_layer, self.multiple_layer_config.layers[2])

    def test_update_existing_layer(self):
        """Test updating an existing layer in a CFSConfiguration"""
        new_commit_hash = 'fedcba987654321'
        updated_layer_data = deepcopy(self.example_layer_data)
        updated_layer_data['commit'] = new_commit_hash
        updated_layer = CFSV2ConfigurationLayer.from_cfs(updated_layer_data)

        self.single_layer_config.ensure_layer(updated_layer, state=LayerState.PRESENT)

        self.assertEqual([updated_layer], self.single_layer_config.layers)

    def test_update_existing_layers(self):
        """Test updating two matching layers of a CFSConfiguration"""
        new_commit_hash = 'fedcba987654321'
        updated_layer_data = deepcopy(self.example_layer_data)
        updated_layer_data['commit'] = new_commit_hash
        updated_layer = CFSV2ConfigurationLayer.from_cfs(updated_layer_data)

        self.duplicate_layer_config.ensure_layer(updated_layer, state=LayerState.PRESENT)

        self.assertEqual([updated_layer] * 2, self.duplicate_layer_config.layers)

    def test_update_layer_no_changes(self):
        """Test updating a layer of the CFSConfiguration when nothing has changed."""
        same_layer = CFSV2ConfigurationLayer.from_cfs(deepcopy(self.example_layer_data))

        self.single_layer_config.ensure_layer(same_layer, state=LayerState.PRESENT)

        self.assertEqual([same_layer], self.single_layer_config.layers)
        self.assertFalse(self.single_layer_config.changed)

    def test_remove_existing_layer(self):
        """Test removing a matching layer from a CFSConfiguration."""
        same_layer = CFSV2ConfigurationLayer.from_cfs(deepcopy(self.example_layer_data))

        self.single_layer_config.ensure_layer(same_layer, state=LayerState.ABSENT)

        self.assertEqual([], self.single_layer_config.layers)

    def test_remove_existing_layers(self):
        """Test removing multiple matching layers from a CFSConfiguration."""
        same_layer = CFSV2ConfigurationLayer.from_cfs(deepcopy(self.example_layer_data))

        self.duplicate_layer_config.ensure_layer(same_layer, state=LayerState.ABSENT)

        self.assertEqual([], self.duplicate_layer_config.layers)

    def test_remove_non_existent_layer(self):
        """Test removing a layer that doesn't exist from a CFSConfiguration."""
        layers_before = self.single_layer_config.layers

        self.single_layer_config.ensure_layer(self.new_layer, state=LayerState.ABSENT)

        self.assertEqual(layers_before, self.single_layer_config.layers)
        self.assertFalse(self.single_layer_config.changed)


class TestCFSV3Configuration(unittest.TestCase):
    """Tests for the CFSV3Configuration class.

    Note that the implementation of CFSV3Configuration is identical to CFSV2Configuration, so
    tests here can be minimal just to ensure that the class is correctly defined.
    """

    def setUp(self):
        self.mock_cfs_client = Mock(spec=CFSV3Client)
        self.mock_get_json = self.mock_cfs_client.get.return_value.json

        self.example_layer_data = {
            "clone_url": "https://api-gw-service-nmn.local/vcs/cray/example-config-management.git",
            "commit": "123456789abcdef",
            "name": "example-config",
            "playbook": "example-config.yml"
        }

        self.dkms_layer_data = {
            "clone_url": "https://api-gw-service-nmn.local/vcs/cray/cos-config-management.git",
            "commit": "abcdef123456789",
            "name": "cos-config",
            "playbook": "cos-config.yml",
            "special_parameters": {
                "ims_require_dkms": True
            }
        }

        additional_inventory_data = {
            "clone_url": "https://api-gw-service-nmn.local/vcs/cray/inventory.git",
            "commit": "987654321abcdef",
            "name": "inventory",
        }

        # A configuration with multiple layers
        self.multiple_layer_config_data = {
            "last_updated": "2021-10-20T21:26:04Z",
            "layers": [
                deepcopy(self.example_layer_data),
                deepcopy(self.dkms_layer_data)
            ],
            "additional_inventory": additional_inventory_data,
            "name": "multiple-layer-config",
        }
        self.multiple_layer_config = CFSV3Configuration(self.mock_cfs_client,
                                                        self.multiple_layer_config_data)

    @staticmethod
    def get_put_configuration_call(config_name, layers):
        """Return the expected call to put_configuration with the given parameters."""
        return call(config_name, {'layers': layers}, request_params=None)

    def assert_put_configuration_called(self, config_name, layers):
        """Assert that put_configuration was called with the given parameters."""
        self.mock_cfs_client.put_configuration.assert_called_once_with(
            config_name, {'layers': layers}, request_params=None
        )

    @patch('csm_api_client.service.cfs.CFSV3ConfigurationLayer.from_cfs')
    @patch('csm_api_client.service.cfs.CFSV3AdditionalInventoryLayer.from_cfs')
    def test_complete_configuration(self, mock_inv_layer, mock_config_layer):
        """Test creating a CFSV3Configuration with layers and additional_inventory"""
        config_data = deepcopy(self.multiple_layer_config_data)
        # Add some additional properties to ensure they're preserved
        config_data['new_property'] = 'new_value'
        config_data['special_parameters'] = {'foo': 'bar'}

        cfs_config = CFSV3Configuration(self.mock_cfs_client, config_data)

        self.assertEqual('multiple-layer-config', cfs_config.name)
        self.assertEqual([mock_config_layer.return_value] * 2, cfs_config.layers)
        self.assertEqual(mock_inv_layer.return_value, cfs_config.additional_inventory)
        self.assertEqual('new_value', cfs_config.passthrough_data['new_property'])
        self.assertEqual({'foo': 'bar'}, cfs_config.passthrough_data['special_parameters'])

        req_payload = cfs_config.req_payload
        self.assertEqual('new_value', req_payload['new_property'])
        self.assertEqual({'foo': 'bar'}, req_payload['special_parameters'])
        self.assertEqual([mock_config_layer.return_value.req_payload] * 2, req_payload['layers'])
        self.assertEqual(mock_inv_layer.return_value.req_payload, req_payload['additional_inventory'])


class TestCFSDebugCommand(unittest.TestCase):
    """Test getting the kubectl log command to debug a failing CFS job"""

    def setUp(self):
        self.failed_container = CFSImageConfigurationSession.get_first_failed_container

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


class TestCFSUpdateContainerStatus(unittest.TestCase):
    """Tests for the _update_container_status method of CFSImageConfigurationSession"""

    def setUp(self):
        """Create a CFSImageConfigurationSession to use in the tests"""
        self.session_name = 'test_session'
        self.image_name = 'test_image'
        self.session = CFSImageConfigurationSession({'name': self.session_name},
                                                    MagicMock(spec=CFSV2Client),
                                                    self.image_name)

        # Mock out a single init_container_status element
        self.init_container_status = MagicMock()
        self.init_container_status.name = 'git-clone'
        self.init_container_status.state.running = None
        self.init_container_status.state.terminated = MagicMock()
        self.init_container_status.state.terminated.exit_code = 0
        self.init_container_statuses = [self.init_container_status]

        # Mock out two container_status elements
        self.inventory_container_status = MagicMock()
        self.inventory_container_status.name = 'inventory'
        self.inventory_container_status.state.running = True
        self.ansible_container_status = MagicMock()
        self.ansible_container_status.name = 'ansible'
        self.ansible_container_status.state.running = True
        self.container_statuses = [self.inventory_container_status, self.ansible_container_status]

        self.session.pod = MagicMock()
        self.session.pod.status.init_container_statuses = self.init_container_statuses
        self.session.pod.status.container_statuses = self.container_statuses

    def test_update_container_status(self):
        """Test _update_container_status when containers are successfully found"""
        with self.assertLogs(level=logging.INFO) as logs_cm:
            self.assertIsNone(self.session._update_container_status())

        # One message is logged describing the session, and one for each container reporting status
        # for the first time
        self.assertEqual(4, len(logs_cm.records))
        self.assertRegexpMatches(logs_cm.records[0].message,
                                 f'CFS session: {self.session_name} *Image: {self.image_name}')
        self.assertRegexpMatches(logs_cm.records[1].message,
                                 'Container git-clone *transitioned to succeeded')
        self.assertRegexpMatches(logs_cm.records[2].message,
                                 'Container inventory *transitioned to running')
        self.assertRegexpMatches(logs_cm.records[3].message,
                                 'Container ansible *transitioned to running')

        # Now simulate completion of the inventory and ansible container
        self.inventory_container_status.state.running = None
        self.inventory_container_status.state.terminated = MagicMock()
        self.inventory_container_status.state.terminated.exit_code = 0
        self.ansible_container_status.state.running = None
        self.ansible_container_status.state.terminated = MagicMock()
        self.ansible_container_status.state.terminated.exit_code = 0
        # Update container status again
        with self.assertLogs(level=logging.INFO) as logs_cm:
            self.assertIsNone(self.session._update_container_status())

        # One message is logged describing the session, and one for each container reporting new status
        self.assertEqual(3, len(logs_cm.records))
        self.assertRegexpMatches(logs_cm.records[0].message,
                                 f'CFS session: {self.session_name} *Image: {self.image_name}')
        self.assertRegexpMatches(logs_cm.records[1].message,
                                 'Container inventory *transitioned to succeeded from running')
        self.assertRegexpMatches(logs_cm.records[2].message,
                                 'Container ansible *transitioned to succeeded from running')

    def test_update_container_status_with_none_pod(self):
        """Check that update_container_status returns None when pod is None"""
        self.session.pod = None
        self.assertIsNone(self.session._update_container_status())

    def test_update_container_status_with_none_init_and_container_statuses(self):
        """Check that update_container_status returns None when both init and container statuses are None"""
        self.session.pod.status.init_container_statuses = None
        self.session.pod.status.container_statuses = None

        with self.assertLogs(level=logging.INFO) as logs_cm:
            self.assertIsNone(self.session._update_container_status())

        self.assertEqual(2, len(logs_cm.records))
        self.assertRegexpMatches(logs_cm.records[0].message,
                                 f'CFS session: {self.session_name} *Image: {self.image_name}')
        self.assertRegexpMatches(logs_cm.records[1].message,
                                 'Waiting for container statuses in pod: init_container_statuses is None')

    def test_update_container_status_with_none_init_statuses(self):
        """Check that update_container_status logs a message one of init_container_statuses is None"""
        self.session.pod.status.init_container_statuses = [None, self.init_container_status]

        with self.assertLogs(level=logging.DEBUG) as logs_cm:
            self.assertIsNone(self.session._update_container_status())

        for log_record in logs_cm.records:
            if log_record.levelno == logging.DEBUG:
                self.assertIn('Found a None container in init_container_status',
                              logs_cm.records[0].message)


class TestCFSClientBase(unittest.TestCase):
    """Tests for the CFSClientBase class."""

    def setUp(self):
        self.mock_session = Mock()
        self.mock_cfs_v2_client = patch('csm_api_client.service.cfs.CFSV2Client').start()
        self.mock_cfs_v3_client = patch('csm_api_client.service.cfs.CFSV3Client').start()

    def tearDown(self):
        patch.stopall()

    def test_get_client_v2(self):
        """Test get_client with version 2"""
        cfs_client = CFSClientBase.get_cfs_client(self.mock_session, 'v2')
        self.assertEqual(cfs_client, self.mock_cfs_v2_client.return_value)

    def test_get_client_v3(self):
        """Test get_client with version 3"""
        cfs_client = CFSClientBase.get_cfs_client(self.mock_session, 'v3')
        self.assertEqual(cfs_client, self.mock_cfs_v3_client.return_value)

    def test_get_client_invalid_version(self):
        """Test get_client with an invalid version"""
        with self.assertRaisesRegex(ValueError, 'Invalid CFS API version'):
            CFSClientBase.get_cfs_client(self.mock_session, 'v4')

class TestCFSV2Client(unittest.TestCase):
    """Tests for the CFSV2Client class"""

    @staticmethod
    def get_fake_components(component_ids: List[str]) -> List[dict]:
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

    def test_join_words(self):
        """Test the join_words static method of CFSV2Client"""
        words_and_results = (
            (('desired', 'config'), 'desiredConfig'),
            (('DESIRED', 'CONFIG'), 'desiredConfig'),
            (('deSiReD', 'cOnFiG'), 'desiredConfig'),
            (('one',), 'one'),
            (('three', 'word', 'name'), 'threeWordName')
        )
        for words, expected_result in words_and_results:
            self.assertEqual(expected_result,
                             CFSV2Client.join_words(*words))

    def test_get_component_ids_using_config(self):
        """Test get_component_ids_using_config"""
        component_ids = ['x3000c0s1b0n0', 'x3000c0s3b0n0', 'x3000c0s5b0n0']
        components = self.get_fake_components(component_ids)
        # For the test, it doesn't have to match, but be consistent to avoid confusion
        config_name = components[0]['desiredConfig']
        cfs_client = CFSV2Client(Mock())

        with patch.object(cfs_client, 'get') as mock_get:
            mock_get.return_value.json.return_value = components
            result = cfs_client.get_component_ids_using_config(config_name)

        mock_get.assert_called_once_with('components', params={'configName': config_name})
        self.assertEqual(component_ids, result)

    def test_get_component_ids_using_config_failure(self):
        """Test get_component_ids_using_config when the request fails"""
        cfs_client = CFSV2Client(Mock())
        err_msg = 'Service unavailable'
        config_name = 'some-config-name'

        with patch.object(cfs_client, 'get', side_effect=APIError(err_msg)) as mock_get:
            with self.assertLogs(level=logging.WARNING) as log:
                cfs_client.get_component_ids_using_config(config_name)

            self.assertIn("Failed to get CFS components:",
                          log.output[0])

        mock_get.assert_called_once_with('components', params={'configName': config_name})

    def test_update_component_no_changes(self):
        """Test update_component with no changes requested"""
        cfs_client = CFSV2Client(Mock())
        component_id = 'x1000c0s0b0n0'

        with patch.object(cfs_client, 'patch') as mock_patch:
            with self.assertLogs(level=logging.WARNING) as logs_cm:
                cfs_client.update_component(component_id)

        mock_patch.assert_not_called()
        self.assertRegex(logs_cm.records[0].message,
                         'No property changes were requested')

    def test_update_component_only_desired_config(self):
        """Test update_component with only a desired_config"""
        cfs_client = CFSV2Client(Mock())
        component_id = 'x3000c0s1b0n0'
        desired_config = 'my-config'

        with patch.object(cfs_client, 'patch') as mock_patch:
            cfs_client.update_component(component_id, desired_config=desired_config)

        mock_patch.assert_called_once_with('components', component_id,
                                           json={'desiredConfig': desired_config})

    def test_update_component_all_properties(self):
        """Test update_component with all properties updated"""
        cfs_client = CFSV2Client(Mock())
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


class TestCFSV3Client(unittest.TestCase):
    """Tests for the CFSV3Client"""

    def test_join_words(self):
        """Test the join_words static method of CFSV2Client"""
        words_and_results = (
            (('desired', 'config'), 'desired_config'),
            (('DESIRED', 'CONFIG'), 'desired_config'),
            (('deSiReD', 'cOnFiG'), 'desired_config'),
            (('one',), 'one'),
            (('three', 'word', 'name'), 'three_word_name')
        )
        for words, expected_result in words_and_results:
            self.assertEqual(expected_result,
                             CFSV3Client.join_words(*words))

    def test_get_components_paged(self):
        """Test get_components method of CFSV3Client with paged results"""
        cfs_client = CFSV3Client(Mock())
        components = [
            {'id': 'x1000c0s0b0n0'},
            {'id': 'x1000c0s1b0n0'},
            {'id': 'x1000c0s2b0n0'},
            {'id': 'x1000c0s3b0n0'},
            {'id': 'x1000c0s4b0n0'},
        ]

        base_params = {'desired_config': 'my-config'}
        with patch.object(cfs_client, 'get') as mock_get:
            mock_get.return_value.json.side_effect = [
                {'components': components[:2], 'next': {'limit': 2, 'after_id': 'x1000c0s1b0n0',
                                                        'desired_config': 'my-config'}},
                {'components': components[2:4], 'next': {'limit': 2, 'after_id': 'x1000c0s3b0n0',
                                                         'desired_config': 'my-config'}},
                {'components': [components[4]], 'next': None}
            ]

            result = list(cfs_client.get_components(params=base_params))

        self.assertEqual(components, result)
        mock_get.assert_has_calls([
            call('components', params=base_params),
            call().json(),
            call('components', params={'limit': 2, 'after_id': 'x1000c0s1b0n0',
                                       'desired_config': 'my-config'}),
            call().json(),
            call('components', params={'limit': 2, 'after_id': 'x1000c0s3b0n0',
                                       'desired_config': 'my-config'}),
            call().json()
        ])

    def test_get_components_unpaged(self):
        """Test get_components method of CFSV3Client when results are not paged"""
        cfs_client = CFSV3Client(Mock())
        components = [
            {'id': 'x1000c0s0b0n0'},
            {'id': 'x1000c0s1b0n0'},
            {'id': 'x1000c0s2b0n0'},
            {'id': 'x1000c0s3b0n0'},
            {'id': 'x1000c0s4b0n0'},
        ]

        with patch.object(cfs_client, 'get') as mock_get:
            mock_get.return_value.json.side_effect = [
                {'components': components, 'next': None}
            ]

            result = list(cfs_client.get_components())

        self.assertEqual(components, result)
        mock_get.assert_has_calls([
            call('components', params=None),
            call().json()
        ])

    def test_get_configurations_paged(self):
        """Test get_configurations method of CFSV3Client with paged results"""
        cfs_client = CFSV3Client(Mock())
        configurations = [
            {'name': 'config-1'},
            {'name': 'config-2'},
            {'name': 'config-3'},
            {'name': 'config-4'},
            {'name': 'config-5'},
        ]

        base_params = {'limit': 2}
        with patch.object(cfs_client, 'get') as mock_get:
            mock_get.return_value.json.side_effect = [
                {'configurations': configurations[:2], 'next': {'limit': 2, 'after': 'config-2'}},
                {'configurations': configurations[2:4], 'next': {'limit': 2, 'after': 'config-4'}},
                {'configurations': [configurations[4]], 'next': None}
            ]

            result = list(cfs_client.get_configurations(params=base_params))

        self.assertEqual(configurations, result)
        mock_get.assert_has_calls([
            call('configurations', params=base_params),
            call().json(),
            call('configurations', params={'limit': 2, 'after': 'config-2'}),
            call().json(),
            call('configurations', params={'limit': 2, 'after': 'config-4'}),
            call().json()
        ])

    def test_get_configurations_unpaged(self):
        """Test get_configurations method of CFSV3Client when results are not paged"""
        cfs_client = CFSV3Client(Mock())
        configurations = [
            {'name': 'config-1'},
            {'name': 'config-2'},
            {'name': 'config-3'},
            {'name': 'config-4'},
            {'name': 'config-5'},
        ]

        with patch.object(cfs_client, 'get') as mock_get:
            mock_get.return_value.json.side_effect = [
                {'configurations': configurations, 'next': None}
            ]

            result = list(cfs_client.get_configurations())

        self.assertEqual(configurations, result)
        mock_get.assert_has_calls([
            call('configurations', params=None),
            call().json()
        ])

    def test_get_sessions_paged(self):
        """Test get_sessions method of CFSV3Client with paged results"""
        cfs_client = CFSV3Client(Mock())
        sessions = [
            {'name': 'session-1'},
            {'name': 'session-2'},
            {'name': 'session-3'},
            {'name': 'session-4'},
            {'name': 'session-5'},
        ]

        base_params = {'limit': 2}
        with patch.object(cfs_client, 'get') as mock_get:
            mock_get.return_value.json.side_effect = [
                {'sessions': sessions[:2], 'next': {'limit': 2, 'after': 'session-2'}},
                {'sessions': sessions[2:4], 'next': {'limit': 2, 'after': 'session-4'}},
                {'sessions': [sessions[4]], 'next': None}
            ]

            result = list(cfs_client.get_sessions(params=base_params))

        self.assertEqual(sessions, result)
        mock_get.assert_has_calls([
            call('sessions', params=base_params),
            call().json(),
            call('sessions', params={'limit': 2, 'after': 'session-2'}),
            call().json(),
            call('sessions', params={'limit': 2, 'after': 'session-4'}),
            call().json()
        ])

    def test_get_sessions_unpaged(self):
        """Test get_sessions method of CFSV3Client when results are not paged"""
        cfs_client = CFSV3Client(Mock())
        sessions = [
            {'name': 'session-1'},
            {'name': 'session-2'},
            {'name': 'session-3'},
            {'name': 'session-4'},
            {'name': 'session-5'},
        ]

        with patch.object(cfs_client, 'get') as mock_get:
            mock_get.return_value.json.side_effect = [
                {'sessions': sessions, 'next': None}
            ]

            result = list(cfs_client.get_sessions())

        self.assertEqual(sessions, result)
        mock_get.assert_has_calls([
            call('sessions', params=None),
            call().json()
        ])