#
# MIT License
#
# (C) Copyright 2021-2025 Hewlett Packard Enterprise Development LP
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
Basic client library for CFS.
"""
from abc import ABC, abstractmethod
import os.path
from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum
from functools import cached_property
from itertools import chain
import json
import logging
import re
import shutil
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Type,
    Union
)
from urllib.parse import urlparse, urlunparse
import uuid

from cray_product_catalog.query import ProductCatalog, ProductCatalogError
# ApiException is not found in the kubernetes.client library
from kubernetes.client import (
    ApiException,
    CoreV1Api,
    V1ContainerStatus,
    V1Pod,
)
from semver import VersionInfo

from csm_api_client.service.gateway import APIError, APIGatewayClient
from csm_api_client.service.hsm import HSMClient
from csm_api_client.service.vcs import VCSError, VCSRepo
from csm_api_client.session import Session
from csm_api_client.util import get_val_by_path, pop_val_by_path, set_val_by_path, strip_suffix

LOGGER = logging.getLogger(__name__)
MAX_BRANCH_NAME_WIDTH = 7


class LayerState(Enum):
    """Desired state of a layer in a CFSConfiguration."""
    PRESENT = 'present'
    ABSENT = 'absent'

    def __str__(self) -> str:
        """Use just the value as the string method.

        This allows its use as the value of the `choices` parameter in the
        add_argument method of argparse.ArgumentParser while still providing
        clear help text to the user.
        """
        return self.value


class CFSConfigurationError(Exception):
    """Represents an error that occurred while modifying CFS."""


class CFSLayerBase(ABC):
    """A common base for layers in a CFS configuration.

    This is an abstract base class not specific to any version of CFS. It must
    be subclassed to support either CFS v2 or CFS v3. It includes common
    functionality for both versions of CFS.

    This class is the parent class for both the layers and the additional
    inventory that can be included in a CFS configuration.
    """

    # Default mapping from CFS properties to attributes of this class
    CFS_PROPS_TO_ATTRS: Dict[str, str] = {
        'commit': 'commit',
        'branch': 'branch',
        'name': 'name',
    }
    # This gets automatically overwritten in the __init_subclass__ method. It is
    # the reverse mapping from attributes of the class to CFS properties.
    ATTRS_TO_CFS_PROPS: Dict[str, str] = {value: key for key, value in CFS_PROPS_TO_ATTRS.items()}
    # These are the attributes that must match for a layer to be considered the same
    # as another layer (apart from the version). Subclasses can add to or override this.
    MATCHING_ATTRS = ['repo_path']

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Sets up class attribute mapping from attributes to CFS properties"""
        super().__init_subclass__(**kwargs)
        # Create the reverse mapping from attributes to CFS properties automatically
        cls.ATTRS_TO_CFS_PROPS = {val: key for key, val in cls.CFS_PROPS_TO_ATTRS.items()}

    def __init__(self,
                 clone_url: Optional[str] = None,
                 name: Optional[str] = None,
                 commit: Optional[str] = None,
                 branch: Optional[str] = None,
                 additional_data: Optional[dict] = None) -> None:
        """Create a new layer.

        Args:
            clone_url: the git repository clone URL
            name: the name of the CFS configuration layer. The
                name is optional.
            commit: the commit hash to use. Either `commit` or
                `branch` is required.
            branch: the git branch to use. Either `commit` or
                `branch` is required.
            additional_data: any extra data included in a layer that is not
                explicitly understood by this class.

        Raises:
            ValueError: if neither `commit` nor `branch` is specified.
        """
        self.clone_url = clone_url
        self._name = name
        if not (commit or branch):
            raise ValueError('Either commit or branch is required to create '
                             'a CFS configuration layer.')
        self.commit = commit
        self.branch = branch
        self.additional_data = additional_data or {}

    @property
    def name(self) -> str:
        """the name of the CFS configuration layer"""
        if self._name is not None:
            return self._name

        branch_or_commit = self.branch or self.commit
        # This shouldn't happen given that __init__ checks that one is specified,
        # but program defensively and satisfy mypy type-checking.
        if not branch_or_commit:
            branch_or_commit = 'default'

        name_components = [
            self.repo_short_name,
            branch_or_commit[:MAX_BRANCH_NAME_WIDTH],
            datetime.now().strftime('%Y%m%dT%H%M%S')
        ]
        return '-'.join(name_components)

    @property
    def repo_path(self) -> str:
        """the path portion of the clone URL, e.g. /vcs/cray/sat-config-management.git"""
        if self.clone_url:
            return urlparse(self.clone_url).path
        else:
            return ''

    @property
    def repo_short_name(self) -> str:
        """a shortened version of the repo name, e.g. 'sat-config-management' becomes 'sat'"""
        repo_name = os.path.basename(self.repo_path)
        # Strip off the '.git' suffix then strip off '-config-management' if present
        return strip_suffix(strip_suffix(repo_name, '.git'), '-config-management')

    def matches(self, other_layer: 'CFSLayerBase') -> bool:
        """Determine whether this layer matches another layer.

        A layer matches if it has the same content, but perhaps at a different
        version. This is useful for identifying which layers in a configuration
        must be updated when updating the version of a product used in a CFS
        configuration.

        Args:
            other_layer: the other layer to compare

        Returns:
            True if the layers match, False otherwise.
        """
        # Layers must be of the same exact type to match. This means they must
        # both be additional inventory layers or configuration layers, and they
        # must be using the same version of CFS.
        if type(self) is not type(other_layer):
            return False

        return all(getattr(self, attr) == getattr(other_layer, attr)
                   for attr in self.MATCHING_ATTRS)

    def get_updated_values(self, new_layer: 'CFSLayerBase') -> Dict:
        """Get the values which have been updated by the new version of this layer.

        Args:
            new_layer: the new layer to compare this one to

        Returns:
            dict: A dict mapping from the names of updated properties to a tuple
                which contains the old and new values.
        """
        updated_values = {}
        for cfs_prop, attr in self.CFS_PROPS_TO_ATTRS.items():
            old_value = getattr(self, attr)
            new_value = getattr(new_layer, attr)
            if old_value != new_value:
                updated_values[cfs_prop] = (old_value, new_value)
        return updated_values

    def resolve_branch_to_commit_hash(self) -> None:
        """Resolve a branch to a commit hash and specify only the commit hash.

        CFS v3 can emulate this behavior in which branch names are resolved to
        commit hashes and only the commit hash is stored in the configuration
        if the 'drop_branches' request parameter is used when creating/updating
        the configuration.

        Returns:
            None. Modifies `self.branch` and `self.commit`.

        Raises:
            CFSConfigurationError: if there is a failure to resolve the branch
                to a commit hash.
        """
        if not self.branch:
            # No branch to translate to commit
            return

        if self.clone_url is None:
            # This should only be the case if this is called in CFS v3 for a layer
            # that specifies a "source" instead of a "clone_url"
            raise CFSConfigurationError(f'Cannot resolve branch {self.branch} to commit hash '
                                        'because clone URL is not specified.')

        vcs_repo = VCSRepo(self.clone_url)
        if self.commit:
            LOGGER.info("%s already specifies a commit hash (%s) and branch (%s); "
                        "overwriting commit hash with latest from branch",
                        self, self.commit, self.branch)
        try:
            self.commit = vcs_repo.get_commit_hash_for_branch(self.branch)
        except VCSError as err:
            raise CFSConfigurationError(f'Failed to resolve branch {self.branch} '
                                        f'to commit hash: {err}')

        if not self.commit:
            raise CFSConfigurationError(f'Failed to resolve branch {self.branch} '
                                        f'to commit hash. No such branch.')

        # Clear out the branch so only commit hash is passed to CFS
        self.branch = None

    @property
    def req_payload(self) -> Dict:
        """Get the request payload to send to CFS for this layer.

        Returns:
            the data for this layer in the format expected by the CFS API
        """
        # Add the additional data to the payload first
        req_payload = {**self.additional_data}
        for cfs_prop, attr in self.CFS_PROPS_TO_ATTRS.items():
            value = getattr(self, attr, None)
            if value is not None:
                set_val_by_path(req_payload, cfs_prop, value)
        return req_payload

    def __str__(self) -> str:
        return f'{self.__class__.__name__} with repo path {self.repo_path}'

    @classmethod
    def from_clone_url(cls, clone_url: str,
                       name: Optional[str] = None,
                       commit: Optional[str] = None,
                       branch: Optional[str] = None,
                       **kwargs: Any) -> 'CFSLayerBase':
        """Create a new CFSConfigurationLayer from an explicit clone URL.

        This method is deprecated and preserved for backwards compatibility. Use
        the constructor directly instead.

        Args:
            clone_url: the git repository clone URL
            name: an optional name override
            commit: an optional commit override
            branch: an optional branch override
            **kwargs: additional arguments to pass along to __init__

        Returns:
            the layer constructed from the clone URL
        """
        return cls(clone_url=clone_url, name=name, commit=commit, branch=branch, **kwargs)

    @classmethod
    def from_cfs(cls, data: Dict) -> 'CFSLayerBase':
        """Create a new CFSLayerBase from data in a response from CFS.

        Args:
            data: the data for the layer from CFS.

        Returns:
            the CFS configuration layer retrieved from CFS configuration data
        """
        data_copy = deepcopy(data)
        kwargs = {
            attr: pop_val_by_path(data_copy, cfs_prop)
            for cfs_prop, attr in cls.CFS_PROPS_TO_ATTRS.items()
        }
        return cls(**kwargs, additional_data=data_copy)

    @classmethod
    def from_product_catalog(cls, product_name: str, api_gw_host: str,
                             product_version: Optional[str] = None,
                             commit: Optional[str] = None,
                             branch: Optional[str] = None,
                             product_catalog: Optional[ProductCatalog] = None,
                             **kwargs: Any) -> 'CFSLayerBase':
        """Create a new CFSConfigurationLayer from product catalog data.

        Args:
            product_name: the name of the product in the product catalog
            api_gw_host: the URL of the API gateway
            product_version: the version of the product in the
                product catalog. If omitted, the latest version is used.
            commit: an optional commit override
            branch: an optional branch override
            product_catalog: the product catalog to use. If omitted, the product
                catalog is loaded from Kubernetes
            **kwargs: additional keyword arguments to pass to the constructor

        Returns:
            the layer constructed from the product

        Raises:
            CFSConfigurationError: if there is a problem getting required info
                from the product catalog to construct the layer.
        """
        fail_msg = (
            f'Failed to create CFS configuration layer for '
            f'{f"version {product_version} of " if product_version else ""}'
            f'product {product_name}'
        )

        if product_catalog is None:
            try:
                product_catalog = ProductCatalog()
            except ProductCatalogError as err:
                raise CFSConfigurationError(f'{fail_msg}: {err}')

        try:
            product = product_catalog.get_product(product_name, product_version)
        except ProductCatalogError as err:
            raise CFSConfigurationError(f'{fail_msg}: {err}')

        if not product.clone_url:
            raise CFSConfigurationError(f'{fail_msg}: {product} has no clone URL.')

        api_gw_parsed = urlparse(api_gw_host)
        if api_gw_parsed.netloc:
            api_gw_host = api_gw_parsed.netloc
        clone_url = urlunparse(
            urlparse(product.clone_url)._replace(
                netloc=api_gw_host
            )
        )

        if not (commit or branch):
            if not product.commit:
                raise CFSConfigurationError(f'{fail_msg}: {product} has no commit hash.')
            commit = product.commit

        return cls(clone_url=clone_url, commit=commit, branch=branch, **kwargs)


class CFSV2AdditionalInventoryLayer(CFSLayerBase):
    """A layer of additional inventory in a CFS v2 configuration."""

    CFS_PROPS_TO_ATTRS = CFSLayerBase.CFS_PROPS_TO_ATTRS.copy()
    CFS_PROPS_TO_ATTRS['cloneUrl'] = 'clone_url'

    def __init__(self, **kwargs: Any) -> None:
        """Create a new CFSV2AdditionalInventoryLayer.

        Raises:
            ValueError: if clone_url is not specified, or branch or commit is
                not specified
        """
        super().__init__(**kwargs)
        if not self.clone_url:
            raise ValueError('clone_url is required for layers in CFS v2')


class CFSV3AdditionalInventoryLayer(CFSLayerBase):
    """A layer of additional inventory in a CFS v3 configuration."""

    CFS_PROPS_TO_ATTRS = CFSLayerBase.CFS_PROPS_TO_ATTRS.copy()
    CFS_PROPS_TO_ATTRS['clone_url'] = 'clone_url'
    CFS_PROPS_TO_ATTRS['source'] = 'source'
    MATCHING_ATTRS = CFSLayerBase.MATCHING_ATTRS + ['source']

    def __init__(self, source: str = None, **kwargs: Any) -> None:
        """Create a new CFSV3AdditionalInventoryLayer.

        Args:
            source: the name of the CFS source to use for the layer, if not
                using clone_url

        Raises:
            ValueError: if clone_url or source is not specified, or branch or
                commit is not specified
        """
        super().__init__(**kwargs)
        self.source = source
        if not (self.source or self.clone_url):
            raise ValueError('Either source or clone_url is required for layers in CFS v3')

    @property
    def repo_short_name(self) -> str:
        """a shortened version of the repo name, or the source name"""
        if self.source:
            return self.source
        else:
            return super().repo_short_name

    def __str__(self) -> str:
        if self.clone_url:
            return f'{self.__class__.__name__} with repo path {self.repo_path}'
        else:
            return f'{self.__class__.__name__} with source {self.source}'


class CFSV2ConfigurationLayer(CFSV2AdditionalInventoryLayer):
    """A layer in a CFS v2 configuration"""

    CFS_PROPS_TO_ATTRS = CFSV2AdditionalInventoryLayer.CFS_PROPS_TO_ATTRS.copy()
    CFS_PROPS_TO_ATTRS['playbook'] = 'playbook'
    CFS_PROPS_TO_ATTRS['specialParameters.imsRequireDkms'] = 'ims_require_dkms'
    MATCHING_ATTRS = CFSV2AdditionalInventoryLayer.MATCHING_ATTRS + ['playbook', 'ims_require_dkms']

    def __init__(self, playbook: str = None, ims_require_dkms: bool = None, **kwargs: Any) -> None:
        """Create a new CFSV2ConfigurationLayer

        Note that CFS v2 configuration layers do not require a playbook. If a
        playbook is not specified, they use the globally configured default
        playbook.

        Args:
            playbook: the name of the playbook to use for the layer
            ims_require_dkms: a special parameter for IMS that indicates whether
                DKMS is required for the layer
            **kwargs: additional arguments to pass to the constructor

        Raises:
            ValueError: if clone_url is not specified, or branch or commit is
                not specified
        """
        super().__init__(**kwargs)
        self.playbook = playbook
        self.ims_require_dkms = ims_require_dkms

    def __str__(self) -> str:
        return (f'{self.__class__.__name__} with repo path {self.repo_path}'
                f' and {f"playbook {self.playbook}" if self.playbook else "default playbook"}')

    @property
    def name(self) -> str:
        """the name of the CFS configuration layer"""
        if self._name is not None:
            return self._name

        if self.playbook is not None:
            playbook_name = os.path.splitext(self.playbook)[0]
        else:
            playbook_name = 'site'

        branch_or_commit = self.branch or self.commit
        # This shouldn't happen given that __init__ checks that one is specified,
        # but program defensively and satisfy mypy type-checking.
        if not branch_or_commit:
            branch_or_commit = 'default'

        name_components = [
            self.repo_short_name,
            playbook_name,
            branch_or_commit[:MAX_BRANCH_NAME_WIDTH],
            datetime.now().strftime('%Y%m%dT%H%M%S')
        ]
        return '-'.join(name_components)


# This preserves backwards-compatibility with earlier versions of this library
# which did not distinguish between CFS API versions
CFSConfigurationLayer = CFSV2ConfigurationLayer


class CFSV3ConfigurationLayer(CFSV3AdditionalInventoryLayer):
    """A layer in a CFS v3 configuration."""

    CFS_PROPS_TO_ATTRS = CFSV3AdditionalInventoryLayer.CFS_PROPS_TO_ATTRS.copy()
    CFS_PROPS_TO_ATTRS['playbook'] = 'playbook'
    CFS_PROPS_TO_ATTRS['special_parameters.ims_require_dkms'] = 'ims_require_dkms'
    MATCHING_ATTRS = CFSV3AdditionalInventoryLayer.MATCHING_ATTRS + ['playbook', 'ims_require_dkms']

    def __init__(self, playbook: str, ims_require_dkms: bool = None, **kwargs: Any) -> None:
        """Create a new CFSV3ConfigurationLayer.

        Args:
            playbook: the name of the playbook to use for the layer
            ims_require_dkms: a special parameter for IMS that indicates whether
                DKMS is required for the layer

        Raises:
            ValueError: if clone_url or source is not specified, or branch or
                commit is not specified
        """
        super().__init__(**kwargs)
        self.playbook = playbook
        self.ims_require_dkms = ims_require_dkms

    def __str__(self) -> str:
        if self.clone_url:
            desc = f'{self.__class__.__name__} with repo path {self.repo_path}'
        else:
            desc = f'{self.__class__.__name__} with source {self.source}'
        return f'{desc} and playbook {self.playbook}'

    @property
    def name(self) -> str:
        """the name of the CFS configuration layer"""
        if self._name is not None:
            return self._name

        playbook_name = os.path.splitext(self.playbook)[0]
        branch_or_commit = self.branch or self.commit
        # This shouldn't happen given that __init__ checks that one is specified,
        # but program defensively and satisfy mypy type-checking.
        if not branch_or_commit:
            branch_or_commit = 'default'

        name_components = [
            self.repo_short_name,
            playbook_name,
            branch_or_commit[:MAX_BRANCH_NAME_WIDTH],
            datetime.now().strftime('%Y%m%dT%H%M%S')
        ]
        return '-'.join(name_components)


class CFSConfigurationBase(ABC):
    """Represents a single configuration in CFS.

    This abstract base class is subclassed to create the CFS v2- v3-specific
    classes.
    """

    # Subclasses must set these class variables to the appropriate classes
    cfs_config_layer_cls: type[CFSLayerBase]
    cfs_additional_inventory_cls: type[CFSLayerBase]

    def __init__(self, cfs_client: 'CFSClientBase', data: Dict) -> None:
        """Create a new CFSConfiguration.

        Args:
            cfs_client: the CFSClient instance to use for interacting with CFS
            data: the data for the configuration from CFS
        """
        self._cfs_client = cfs_client
        self.data = data
        self.layers = [self.cfs_config_layer_cls.from_cfs(layer_data)
                       for layer_data in self.data.get('layers', [])]
        self.additional_inventory: Optional[CFSLayerBase] = None
        if 'additional_inventory' in self.data:
            self.additional_inventory = self.cfs_additional_inventory_cls.from_cfs(
                self.data['additional_inventory'])
        self.changed = False

    @classmethod
    def empty(cls, cfs_client: 'CFSClientBase') -> 'CFSConfigurationBase':
        """Get a new empty CFSConfiguration with no layers.

        Returns:
            a new empty configuration
        """
        return cls(cfs_client, {})

    @property
    def name(self) -> Optional[str]:
        """the name of the CFS configuration"""
        return self.data.get('name')

    @property
    def passthrough_data(self) -> dict[str, Any]:
        """a dict containing any additional data to pass through to CFS

        This preserves any additional data that should be maintained when making
        a PUT request to update the configuration in CFS. Currently, the only
        field that will be saved here is "description", but this safeguards
        against any additional fields that may be added by CFS in the future.
        """
        # Note: both the CFS v2 and v3 versions of the lastUpdated field are ignored
        return {key: value for key, value in self.data.items()
                if key not in ('layers', 'additional_inventory', 'lastUpdated',
                               'last_updated', 'name')}

    @property
    def req_payload(self) -> Dict:
        """the request payload to provide in a PUT request to create/update the configuration"""
        payload: Dict = {
            'layers': [layer.req_payload for layer in self.layers],
        }
        if self.additional_inventory:
            payload['additional_inventory'] = self.additional_inventory.req_payload
        payload.update(self.passthrough_data)
        return payload

    def save_to_cfs(self, name: Optional[str] = None,
                    overwrite: bool = True, backup_suffix: Union[str, None] = None,
                    request_params: Dict = None) -> 'CFSConfigurationBase':
        """Save the configuration to CFS, optionally with a new name.

        Args:
            name: the name to save as. Required if this
                configuration does not yet have a name.
            overwrite: if True, silently overwrite an existing CFS
                configuration given by `name`. If False, raise a
                CFSConfigurationError if a configuration with `name` already
                exists
            backup_suffix: if specified, before saving the current version of
                the CFS configuration, if it already exists in CFS, save a
                backup with the given suffix.
            request_params: request parameters passed along to CFS when saving
                the new CFS configuration (e.g. drop_branches). Not passed when
                saving the backup.

        Returns:
            the new configuration that was saved to CFS

        Raises:
            CFSConfigurationError: if there is a failure saving the configuration
                to CFS, or if a CFS configuration exists and `overwrite` is False
        """
        if name is not None:
            cfs_name = name
        elif self.name is not None:
            cfs_name = self.name
        else:
            raise ValueError('A name must be specified for the CFS configuration.')

        existing_cfs_config_data = None
        # Check if a CFS configuration with the given name already exists when
        # overwrite is disallowed or when a backup is requested.
        if not overwrite or backup_suffix:
            try:
                response = self._cfs_client.get('configurations', cfs_name, raise_not_ok=False)

                # If response was OK, that indicates there's already a CFS configuration
                if response.ok:
                    try:
                        existing_cfs_config_data = response.json()
                    except ValueError as err:
                        raise CFSConfigurationError(f'Failed to decode JSON response from reading'
                                                    f'existing CFS configuration "{cfs_name}": {err}')

                elif response.status_code != 404:
                    # If there's a failure for any reason other than a missing
                    # configuration, throw the APIError and catch it below
                    self._cfs_client.raise_from_response(response)

            except APIError as err:
                raise CFSConfigurationError(f'Failed to check for existing CFS '
                                            f'configuration "{cfs_name}": {err}') from err

        if existing_cfs_config_data:
            if not overwrite:
                raise CFSConfigurationError(f'A configuration named {cfs_name} already exists '
                                            f'and will not be overwritten.')
            if backup_suffix:
                backup_config_name = f'{cfs_name}{backup_suffix}'
                # CFS does not allow certain read-only properties in a PUT request, so strip them
                for prop in ('name', 'lastUpdated', 'last_updated'):
                    existing_cfs_config_data.pop(prop, None)
                try:
                    self._cfs_client.put_configuration(backup_config_name, existing_cfs_config_data)
                except APIError as err:
                    raise CFSConfigurationError(f'Failed to back up CFS configuration "{cfs_name}" '
                                                f'to "{backup_config_name}": {err}') from err

        try:
            response_json = self._cfs_client.put_configuration(
                cfs_name, self.req_payload, request_params=request_params)
        except APIError as err:
            raise CFSConfigurationError(str(err)) from err

        LOGGER.info('Successfully saved CFS configuration "%s"', cfs_name)
        return self.__class__(self._cfs_client, response_json)

    def save_to_file(self, file_path: str, overwrite: bool = True,
                     backup_suffix: Union[str, None] = None) -> None:
        """Save the configuration to a file.

        Args:
            file_path: the path to the file where this config should be saved
            overwrite: if True, silently overwrite an existing file
                given by `file_path`. If False, raise a CFSConfigurationError if
                `file_path` already exists.
            backup_suffix: if specified, before saving the current version of
                the CFS configuration to a file, if the file already exists,
                save a backup file with the given suffix.

        Returns:
            None

        Raises:
            CFSConfigurationError: if there is a failure saving the configuration
                to the file, or if the file exists and overwrite is `False`.
        """
        # It only makes sense to make a backup copy if overwriting is requested
        if backup_suffix and overwrite:
            path_without_ext, ext = os.path.splitext(file_path)
            backup_file_path = f'{path_without_ext}{backup_suffix}{ext}'
            try:
                shutil.copyfile(file_path, backup_file_path)
            except FileNotFoundError:
                LOGGER.debug(f'File {file_path} does not exist so does not need to be backed up.')
            except OSError as err:
                raise CFSConfigurationError(f'Failed to copy {file_path} to {backup_file_path} '
                                            f'before overwriting: {err}') from err

        try:
            with open(file_path, 'w' if overwrite else 'x') as f:
                json.dump(self.req_payload, f, indent=2)
        except FileExistsError:
            raise CFSConfigurationError(f'Configuration at path {file_path} already exists '
                                        f'and will not be overwritten.')
        except OSError as err:
            raise CFSConfigurationError(f'Failed to write to file file_path: {err}')

    def ensure_layer(self, layer: CFSLayerBase,
                     state: LayerState = LayerState.PRESENT) -> None:
        """Ensure a layer exists or does not exist with the given parameters.

        Args:
            layer: the layer to ensure is present or
                absent
            state: whether to ensure the layer is present or absent

        Returns:
            None
        """
        action = ('Removing', 'Updating')[state is LayerState.PRESENT]

        new_layers = []
        found_match = False
        for existing_layer in self.layers:
            if layer.matches(existing_layer):
                found_match = True
                LOGGER.info('%s existing %s', action, existing_layer)
                if state is LayerState.ABSENT:
                    # Skip adding this layer to new_layers
                    self.changed = True
                    continue

                updated_props = existing_layer.get_updated_values(layer)
                if updated_props:
                    self.changed = True
                    for updated_prop, update in updated_props.items():
                        LOGGER.info('Property "%s" of %s updated from %s to %s',
                                    updated_prop, existing_layer, update[0], update[1])

                # Preserve any existing additional data in the layer
                layer.additional_data = deepcopy(existing_layer.additional_data)
                new_layers.append(layer)
            else:
                # This layer doesn't match, so leave it untouched
                new_layers.append(existing_layer)

        if not found_match:
            LOGGER.info('No %s found.', layer)
            if state is LayerState.PRESENT:
                LOGGER.info('Adding a %s to the end.', layer)
                self.changed = True
                new_layers.append(layer)

        self.layers = new_layers

        if not self.changed:
            LOGGER.info('No changes to configuration "%s" are necessary.', self.name)


class CFSV2Configuration(CFSConfigurationBase):
    """CFS V2 configuration"""

    cfs_config_layer_cls = CFSV2ConfigurationLayer
    cfs_additional_inventory_cls = CFSV2AdditionalInventoryLayer


# This preserves backwards-compatibility with earlier versions of this library
# which did not distinguish between CFS API versions
CFSConfiguration = CFSV2Configuration


class CFSV3Configuration(CFSConfigurationBase):
    """CFS V3 configuration"""

    cfs_config_layer_cls = CFSV3ConfigurationLayer
    cfs_additional_inventory_cls = CFSV3AdditionalInventoryLayer


class CFSImageConfigurationSession:
    """A class representing an image customization CFS configuration session

    The same class is used for both CFS v2 and v3 since the only difference here
    is the property name for the start time.

    Attributes:
        data: the data from the CFS API for this session
        cfs_client: the CFS API client used to create the session and to use to
            query the session status
        image_name: the name specified for the image in a bootprep input
            file. Currently only used for logging, but may be used when creating
            the session in the future if CASMCMS-7564 is resolved.
        pod: the pod associated with this session.
            It is set to None until the pod has been created and found by
            update_pod_status. It is updated by each call to update_pod_status.
        init_container_status_by_name: a mapping from init container name
            to status
        container_status_by_name: a mapping from container name to status
    """
    # Values reported by CFS in status.session.status
    COMPLETE_VALUE = 'complete'
    PENDING_VALUE = 'pending'
    # Value reported by CFS in status.session.succeeded when session has succeeded
    SUCCEEDED_VALUE = 'true'
    # Hardcode the namespace in which Kubernetes jobs are created since CFS
    # sessions do not record this information
    KUBE_NAMESPACE = 'services'

    # Hardcode the container status messages
    CONTAINER_RUNNING_VALUE = 'running'
    CONTAINER_WAITING_VALUE = 'waiting'
    CONTAINER_SUCCEEDED_VALUE = 'succeeded'
    CONTAINER_FAILED_VALUE = 'failed'

    def __init__(self, data: Dict, cfs_client: 'CFSClientBase', image_name: str):
        """Create a new CFSImageConfigurationSession

        Args:
            data: the data from the CFS API for this session
            cfs_client: the CFS API client used to
                create the session and to use to query the session status
            image_name: the name specified for the image in a bootprep
                input file.
        """
        self.data = data
        self.cfs_client = cfs_client
        self.image_name = image_name

        self.logged_job_wait_msg = False
        self.logged_pod_wait_msg = False

        # This is set to the V1Pod for this CFS session by update_pod_status
        self.pod: Optional[V1Pod] = None

        # We are assuming unique container names within the pod
        self.init_container_status_by_name: Dict[str, str] = {}
        self.container_status_by_name: Dict[str, str] = {}

    @property
    def name(self) -> str:
        """The name of the CFS session"""
        return self.data.get('name', '')

    @property
    def session_status(self) -> str:
        """The status of the session according to CFS

        This will be one of 'pending', 'running', or 'complete'
        """
        return str(get_val_by_path(self.data, 'status.session.status'))

    @property
    def complete(self) -> bool:
        """True if the configuration session is complete, False otherwise"""
        return self.session_status == self.COMPLETE_VALUE

    @property
    def succeeded(self) -> bool:
        """True if the configuration session succeeded, False otherwise"""
        return get_val_by_path(self.data, 'status.session.succeeded') == self.SUCCEEDED_VALUE

    @property
    def kube_job(self) -> str:
        """The name of the kubernetes job created by this CFS session"""
        return str(get_val_by_path(self.data, 'status.session.job'))

    @property
    def pod_name(self) -> str:
        """The name of the pod created by the K8s job for this CFS session"""
        # Handle access to this property before update_pod_status has found the pod
        if self.pod is None or self.pod.metadata is None:
            return ''
        return str(self.pod.metadata.name)

    @property
    def start_time(self) -> datetime:
        """The start time of this CFS session"""
        # In CFS v2, the property is startTime. In CFS v3, it's start_time
        start_time_str = get_val_by_path(self.data, 'status.session.startTime')
        if start_time_str is None:
            start_time_str = get_val_by_path(self.data, 'status.session.start_time')
        # Once we can rely on Python 3.7 or greater, can switch this to use fromisoformat
        return datetime.strptime(str(start_time_str), '%Y-%m-%dT%H:%M:%S')

    @property
    def time_since_start(self) -> timedelta:
        """The time that has elapsed since the start of the session"""
        return datetime.utcnow() - self.start_time

    @property
    def resultant_image_id(self) -> Optional[str]:
        """The ID of the resultant IMS image created by this session or None"""
        artifact_list = get_val_by_path(self.data, 'status.artifacts', [])
        if not artifact_list:
            return None
        # A CFS image customization session can produce more than one resulting
        # image if given multiple target groups referring to different images,
        # but it is not a common use case, so it is not currently supported by
        # CFSClient.create_image_customization_session, so just assume there is
        # only one resultant image here.
        return artifact_list[0].get('result_id', None)

    @staticmethod
    def get_failed_containers(status_by_name: Dict) -> List[str]:
        """Get the list of failed container names from the given status_by_name

        Args:
            status_by_name: a dictionary mapping from container name to
                status string

        Returns:
            The container names that have failed
        """
        return [container_name for container_name, status in status_by_name.items()
                if status == 'failed']

    @property
    def failed_containers(self) -> List[str]:
        """List of the failed containers in the pod for this CFS session"""
        return self.get_failed_containers(self.container_status_by_name)

    @property
    def failed_init_containers(self) -> List[str]:
        """List of failed init container names in the pod for this CFS session"""
        return self.get_failed_containers(self.init_container_status_by_name)

    def get_suggested_debug_command(self) -> Optional[str]:
        """Get the best guess at the command the admin should use to debug failure

        Generally if an init container failed, that is the one to debug first.

        Returns:
            The command that the admin should run to debug the failure,
                or None if no command is available or necessary.
        """

        # This shouldn't happen but protect against it anyway
        if self.pod is None or self.pod.metadata is None:
            return None

        kubectl_cmd = f'kubectl logs -n {self.KUBE_NAMESPACE} {self.pod.metadata.name}'
        failed_container = self.get_first_failed_container(
            self.failed_init_containers,
            self.failed_containers
        )
        if failed_container is None:
            return None

        return f'{kubectl_cmd} -c {failed_container}'

    @staticmethod
    def get_first_failed_container(
        failed_init_containers: List[str],
        failed_containers: List[str]
    ) -> Optional[str]:
        container_execution_order_prefixes = [
            'inventory',
            'ansible',
            'teardown'
        ]
        if failed_init_containers:
            # If any init containers fail, none of the (non-init) containers
            # will be run. Kubernetes init containers are executed in-order, so
            # the following line works on the assumption that nothing is
            # modifying the ordering of the containers.
            return failed_init_containers[0]
        elif failed_containers:
            for container_name_prefix in container_execution_order_prefixes:
                matching_failed_containers = [name for name in failed_containers
                                              if name.startswith(container_name_prefix)]
                if matching_failed_containers:
                    def extract_numeric_suffix(s: str) -> int:
                        match = re.search(r'([0-9]+)$', s)
                        if match is None:
                            return 0
                        return int(match.group(0))
                    return min(matching_failed_containers, key=extract_numeric_suffix)
            else:
                # Some container failed that is not listed in the execution
                # order above.
                return None
        else:
            return None

    def update_cfs_status(self) -> None:
        """Query the CFS API to update the status of this CFS session

        Returns:
            None

        Raises:
            APIError: if there is a failure to get session status from the CFS API
        """
        fail_msg = f'Failed to get updated session status for session {self.name}'

        try:
            session_details = self.cfs_client.get_session(self.name)
        except APIError as err:
            raise APIError(f'{fail_msg}: {err}')

        try:
            self.data['status'] = session_details['status']
        except KeyError as err:
            raise APIError(f'{fail_msg}: {err} key was missing in response from CFS.')

    def get_container_status_description(self, container_status: V1ContainerStatus) -> str:
        """Get a string representation of the container status

        Args:
            container_status: the status of the container

        Returns:
            One of 'running', 'waiting', 'succeeded', or 'failed'
        """
        # The Python Kubernetes client docs are misleading in that they
        # imply only one of these keys will be present, but in reality,
        # they are all present, and set to None if not the current state
        if not container_status.state:
            return self.CONTAINER_WAITING_VALUE

        if container_status.state.running:
            return self.CONTAINER_RUNNING_VALUE
        elif container_status.state.terminated:
            if container_status.state.terminated.exit_code == 0:
                return self.CONTAINER_SUCCEEDED_VALUE
            else:
                return self.CONTAINER_FAILED_VALUE
        else:
            return self.CONTAINER_WAITING_VALUE

    def get_container_status_change_msg(
        self,
        container_name: str,
        new_status: str,
        old_status: Optional[str] = None
    ) -> str:
        """Get a nicely formatted container status change message.

        Args:
            container_name: the name of the container that changed state
            new_status: the new status of the container
            old_status: the old status of the container, if known

        Returns:
            The description of the container status change
        """
        if not self.pod or not self.pod.status:
            container_name_width = len(container_name)
        else:
            container_name_width = max(
                len(container_status.name)
                for container_status in chain(
                    self.pod.status.init_container_statuses,
                    self.pod.status.container_statuses,
                ) if container_status is not None
            )
        status_width = max(len(status) for status in [
                self.CONTAINER_RUNNING_VALUE,
                self.CONTAINER_WAITING_VALUE,
                self.CONTAINER_SUCCEEDED_VALUE,
                self.CONTAINER_FAILED_VALUE
            ]
        )

        msg = (
            f'Container {container_name: <{container_name_width}} '
            f'transitioned to {new_status: <{status_width}}'
        )

        if old_status:
            msg += f' from {old_status: <{status_width}}'

        return msg

    def _update_container_status(self) -> None:
        """Update init_container and container status mappings

        Updates `init_container_status_by_name` and `container_status_by_name`
        attributes with latest status from `self.pod`.

        Returns:
            None
        """
        state_log_msgs = []

        # This shouldn't happen since this is only called by update_pod_status
        # after the pod is successfully found.
        if self.pod is None:
            return

        # Update both init_container_status_by_name and container_status_by_name
        for prefix in ('init_', ''):
            status_by_name = getattr(self, f'{prefix}container_status_by_name')
            container_statuses = getattr(self.pod.status, f'{prefix}container_statuses')
            if container_statuses is None:
                state_log_msgs.append(f'Waiting for container statuses in pod: {prefix}container_statuses is None.')
                break

            for container_status in container_statuses:
                if container_status is None:
                    LOGGER.debug(f'Found a None container in {prefix}container_status')
                    continue

                container_name = container_status.name

                status = self.get_container_status_description(container_status)

                old_status = status_by_name.get(container_status.name)

                # First time reporting status or status has changed
                if not old_status or (status != old_status):
                    state_log_msgs.append(self.get_container_status_change_msg(container_name,
                                                                               status, old_status))

                status_by_name[container_name] = status

        if state_log_msgs:
            LOGGER.info(f'CFS session: {self.name: <{CFSClientBase.MAX_SESSION_NAME_LENGTH}} '
                        f'Image: {self.image_name}:')
            for msg in state_log_msgs:
                LOGGER.info(f'    {msg}')

    def update_pod_status(self, kube_client: CoreV1Api) -> None:
        """Get the pod for the Kubernetes job.

        The CFS session starts a Kubernetes job which has the following spec:

            backoffLimit: 0
            completions: 1
            parallelism: 1

        As a result, it will only ever run one pod, regardless of whether it
        completes successfully or not.

        Args:
            kube_client: the Kubernetes API client

        Returns:
            None

        Raises:
            APIError: if there is a problem querying the Kubernetes API for
                status of the pod for the session.
        """
        try:
            pods = kube_client.list_namespaced_pod(self.KUBE_NAMESPACE,
                                                   label_selector=f'job-name={self.kube_job}')
        except ApiException as err:
            raise APIError(f'Failed to query Kubernetes for pod associated '
                           f'with CFS Kubernetes job {self.kube_job}: {err}')

        try:
            self.pod = pods.items[0]
        except IndexError:
            if not self.logged_pod_wait_msg:
                LOGGER.info(f'Waiting for creation of Kubernetes pod associated with session {self.name}.')
                self.logged_pod_wait_msg = True
        else:
            self._update_container_status()

    def update_status(self, kube_client: CoreV1Api) -> None:
        """Query the CFS and Kubernetes APIs to update the status of this CFS session

        Args:
            kube_client: the Kubernetes API client

        Returns:
            None. Updates the status stored in this object.
        """
        self.update_cfs_status()

        if self.session_status == self.PENDING_VALUE:
            if not self.logged_job_wait_msg:
                LOGGER.info(f'Waiting for CFS to create Kubernetes job associated with session {self.name}.')
                self.logged_job_wait_msg = True
        else:
            self.update_pod_status(kube_client)


class CFSClientBase(APIGatewayClient, ABC):
    MAX_SESSION_NAME_LENGTH = 45
    configuration_cls: Type[CFSConfigurationBase]

    @staticmethod
    @abstractmethod
    def join_words(*words: str) -> str:
        """Join words together according to the convention used by the CFS version.

        E.g. CFS v2 uses camelCase while CFS v3 uses snake_case

        Args:
            *words: the words to join together

        Returns:
            The words joined together according to the convention of the CFS API
            version.
        """
        pass

    @staticmethod
    def get_valid_session_name(prefix: str = 'sat') -> str:
        """Get a valid CFS session name.

        CFS session names are restricted by Kubernetes naming conventions, and
        they must be unique. This method gives an easy way to get a valid, unique
        name, with an optional prefix.

        Note that this uses uuid.uuid4 to generate a 36-character-long uuid and
        adds a hyphen after the prefix. Since CFS session names are limited to
        45 characters, the max prefix length is 45 - 36 - 1 = 8.

        No other validation of the prefix is performed.

        Args:
            prefix: the prefix to use for the CFS session name. This will
                be at the beginning of the session name followed by a hyphen.

        Returns:
            The session name
        """
        uuid_str = str(uuid.uuid4())

        # Subtract an additional 1 to account for "-" separating prefix from uuid
        prefix_max_len = CFSClientBase.MAX_SESSION_NAME_LENGTH - len(uuid_str) - 1
        if len(prefix) > prefix_max_len:
            LOGGER.warning(f'Given CFS session prefix is too long and will be '
                           f'truncated ({len(prefix)} > {prefix_max_len})')
            prefix = prefix[:prefix_max_len]

        return '-'.join([prefix, uuid_str])

    def get_api_version(self) -> Optional[VersionInfo]:
        """Get the API version from the CFS API.

        Returns:
            The version returned by the CFS API or None if the CFS API does not
            support the version endpoint and returned a 404.

        Raises:
            APIError: if there is an issue getting the API version, other
                than this endpoint not being supported, in which case, the method
                returns None.
        """
        # A call to get without args will just do a GET to the base_resource_path,
        # which returns the CFS semantic version.
        response = self.get(raise_not_ok=False)
        if not response.ok:
            if response.status_code == 404:
                # Versions of the CFS API prior to 1.12.0 did not provide this endpoint
                return None
            else:
                self.raise_from_response(response)

        try:
            version_dict = response.json()
        except ValueError as err:
            raise APIError(f'Failed to parse JSON in response from CFS when getting '
                           f'API version: {err}')

        try:
            return VersionInfo(version_dict['major'], version_dict['minor'], version_dict['patch'])
        except ValueError:
            raise APIError(f'CFS API returned invalid semantic version data: {version_dict}')
        except KeyError as err:
            raise APIError(f'CFS API returned invalid version dict with missing key {err}:'
                           f'{version_dict}')

    @cached_property
    def supports_customized_image_name(self) -> bool:
        """Whether the version of CFS API supports specifying the name of the customized image"""
        api_version = self.get_api_version()
        if api_version is None:
            return False
        else:
            return api_version >= VersionInfo(1, 12, 0)

    def put_configuration(self, config_name: str, request_body: Dict,
                          request_params: Optional[Dict] = None) -> Dict:
        """Create a new configuration or update an existing configuration

        Args:
            config_name: the name of the configuration to create/update
            request_body: the configuration data, which should have a
                'layers' key.
            request_params: the parameters to pass

        Returns:
            The details of the newly updated or created session
        """
        try:
            return self.put('configurations', config_name, json=request_body, req_param=request_params).json()
        except APIError as err:
            raise APIError(f'Failed to update CFS configuration {config_name}: {err}')
        except ValueError as err:
            raise APIError(f'Failed to parse JSON in response from CFS when updating '
                           f'CFS configuration {config_name}: {err}')

    def get_session(self, name: str) -> Dict:
        """Get details for a session.

        Args:
            name: the name of the session to get

        Returns:
            The details about the session
        """
        try:
            return self.get('sessions', name).json()
        except APIError as err:
            raise APIError(f'Failed to get CFS session {name}: {err}')
        except ValueError as err:
            raise APIError(f'Failed to parse JSON in response from CFS when getting '
                           f'CFS session {name}: {err}')

    def create_image_customization_session(
        self, session_name: str, config_name: str, image_id: str,
        target_groups: Iterable[str], image_name: str
    ) -> CFSImageConfigurationSession:
        """Create a new image customization session.

        The session name will be generated with self.get_valid_session_name.

        Args:
            session_name: the name of the session
            config_name: the name of the configuration to use
            image_id: the id of the IMS image to customize
            target_groups: the group names to target. Each group
                name specified here will be defined to point at the IMS image
                specified by `image_id`.
            image_name: the name of the image to create. If the version of the
                CFS API supports naming the customized image, then this will be
                the name of the resulting image. If not, then the name is just
                used during logging, but the actual name will differ.

        Returns:
            The created session
        """
        request_body: Dict = {
            'name': session_name,
            self.join_words('configuration', 'name'): config_name,
            'target': {
                'definition': 'image',
                'groups': [
                    {'name': target_group, 'members': [image_id]}
                    for target_group in target_groups
                ]
            }
        }

        if self.supports_customized_image_name:
            request_body['target']['image_map'] = [
                {'source_id': image_id, 'result_name': image_name}
            ]

        try:
            created_session = self.post('sessions', json=request_body).json()
        except APIError as err:
            raise APIError(f'Failed to create image customization session : {err}')
        except ValueError as err:
            raise APIError(f'Failed to parse JSON in response from CFS when '
                           f'creating CFS session: {err}')

        return CFSImageConfigurationSession(created_session, self, image_name)

    def get_configuration(self, name: str) -> CFSConfigurationBase:
        """Get a CFS configuration by name."""
        try:
            config_data = self.get('configurations', name).json()
        except APIError as err:
            raise APIError(f'Could not retrieve configuration "{name}" from CFS: {err}')
        except json.JSONDecodeError as err:
            raise APIError(f'Invalid JSON response received from CFS when getting '
                           f'configuration "{name}" from CFS: {err}')
        # Return an appropriate CFS configuration
        return self.configuration_cls(self, config_data)

    def get_configurations_for_components(self, hsm_client: HSMClient, **kwargs: str) -> List[CFSConfigurationBase]:
        """Get configurations for components matching the given params.

        Parameters passed into this function should be valid HSM component
        attributes.

        Args:
            hsm_client (HSMClient): the HSM client to use to query HSM for
                component IDs
            kwargs: parameters which are passed to HSM to filter queried components.

        Returns:
            The relevant configs marked as desired for the
                given components

        Raises:
            APIError: if there is an error accessing HSM or CFS APIs
        """
        target_xnames = hsm_client.get_component_xnames(params=kwargs or None)

        LOGGER.info('Querying CFS configurations for the following components: %s',
                    ', '.join(target_xnames))

        desired_config_names = set()
        for xname in target_xnames:
            try:
                desired_config_name = self.get('components', xname).json().get(
                    self.join_words('desired', 'config'))
                if desired_config_name:
                    LOGGER.info('Found configuration "%s" for component %s',
                                desired_config_name, xname)
                    desired_config_names.add(desired_config_name)
            except APIError as err:
                LOGGER.warning('Could not retrieve CFS configuration for component %s: %s',
                               xname, err)
            except json.JSONDecodeError as err:
                LOGGER.warning('CFS returned invalid JSON for component %s: %s',
                               xname, err)

        return [self.get_configuration(config_name) for config_name in desired_config_names]

    @abstractmethod
    def get_components(self, params: Dict = None) -> Generator[Dict, None, None]:
        """Get all the CFS components.

        This method must handle paging if necessary.

        Args:
            params: the parameters to pass to the GET on components

        Yields:
            The CFS components.
        """
        pass

    @abstractmethod
    def get_configurations(self, params: Dict = None) -> Generator[Dict, None, None]:
        """Get all the CFS configurations.

        This method must handle paging if necessary.

        Args:
            params: the parameters to pass to the GET on configurations

        Yields:
            The CFS configurations.
        """
        pass

    @abstractmethod
    def get_sessions(self, params: Dict = None) -> Generator[Dict, None, None]:
        """Get all the CFS sessions.

        This method must handle paging if necessary.

        Args:
            params: the parameters to pass to the GET on sessions

        Yields:
            The CFS sessions.
        """
        pass

    def get_component_ids_using_config(self, config_name: str) -> List[str]:
        """Get a list of CFS components using the given CFS configuration.

        Args:
            config_name: the name of the CFS configuration to search for
                components using it

        Returns:
            The list of component IDs of CFS components using the given CFS
            configuration as their desiredConfig.
        """
        filter_params = {self.join_words('config', 'name'): config_name}
        try:
            return [component['id'] for component in self.get_components(params=filter_params)]
        except (APIError, ValueError) as err:
            raise APIError(f'Failed to get components using configuration '
                           f'"{config_name}": {err}') from err
        except KeyError as err:
            raise APIError(f'Failed to get components using configuration "{config_name}: '
                           f'one or more components missing {err} property') from err

    def update_component(self, component_id: str, desired_config: Optional[str] = None,
                         clear_state: Optional[bool] = None, clear_error: Optional[bool] = None,
                         enabled: Optional[bool] = None) -> None:
        """Update a CFS component with the given parameters.

        Args:
            component_id: the id (xname) of the component to be updated
            desired_config: the desiredConfig of the component
            clear_state: if True, clear the state of the component
            clear_error: if True, clear the errorCount of the component
            enabled: if specified, set the enabled property of the component

        Returns: None

        Raises:
            APIError: if there is a failure to update the given component
        """
        patch_params: Dict = {}
        if desired_config is not None:
            patch_params[self.join_words('desired', 'config')] = desired_config
        if enabled is not None:
            patch_params['enabled'] = enabled
        if clear_state:
            patch_params['state'] = []
        if clear_error:
            patch_params[self.join_words('error', 'count')] = 0

        if patch_params:
            self.patch('components', component_id, json=patch_params)
        else:
            LOGGER.warning(f'No property changes were requested during update '
                           f'of CFS component with id {component_id}.')

    @staticmethod
    def get_cfs_client(session: Session, version: str, **kwargs: Any) -> 'CFSClientBase':
        """Instantiate a CFSVxClient for the given API version.

        Args:
            session (csm_api_client.Session): session object to pass through to the client
            version (Optional[str]): 'v2' or 'v3'

        Additional kwargs are passed through to the underlying CFSVxClient
        constructor.

        Returns:
            An instance of a subclass of `CFSClientBase`.

        Raises:
            ValueError: if the given version string is not valid
        """
        cfs_client_cls = {
            'v2': CFSV2Client,
            'v3': CFSV3Client,
        }.get(version)

        if cfs_client_cls is None:
            raise ValueError(f'Invalid CFS API version "{version}"')

        return cfs_client_cls(session, **kwargs)


class CFSV2Client(CFSClientBase):
    base_resource_path = 'cfs/v2'
    configuration_cls = CFSV2Configuration

    @staticmethod
    def join_words(*words: str) -> str:
        return ''.join([words[0].lower()] + [word.capitalize() for word in words[1:]])

    def get_resource(self, resource: str, params: Dict = None) -> Generator[Dict, None, None]:
        """Get a resource from the CFS API.

        Args:
            resource: the name of the resource to get (e.g. 'components')
            params: the parameters to pass to the GET on the resource
        """
        try:
            yield from self.get(resource, params=params).json()
        except APIError as err:
            raise APIError(f'Failed to get CFS {resource}: {err}')
        except ValueError as err:
            raise APIError(f'Failed to parse JSON in response from CFS when getting '
                           f'{resource}: {err}')

    def get_components(self, params: Dict = None) -> Generator[Dict, None, None]:
        yield from self.get_resource('components', params=params)

    def get_configurations(self, params: Dict = None) -> Generator[Dict, None, None]:
        yield from self.get_resource('configurations', params=params)

    def get_sessions(self, params: Dict = None) -> Generator[Dict, None, None]:
        yield from self.get_resource('sessions', params=params)


# Create an alias for CFSClient that points at CFSV2Client to preserve backwards compatibility
CFSClient = CFSV2Client


class CFSV3Client(CFSClientBase):
    base_resource_path = 'cfs/v3'
    configuration_cls = CFSV3Configuration

    @staticmethod
    def join_words(*words: str) -> str:
        return '_'.join([word.lower() for word in words])

    def put_configuration(self, config_name: str, request_body: Dict,
                          request_params: Optional[Dict] = None, drop_branches: bool = False) -> Dict:
        """Create a new configuration or update an existing configuration

        Args:
            config_name: the name of the configuration to create/update
            request_body: the configuration data, which should have a
                'layers' key.
            request_params: the parameters to pass
            drop_branches: whether to drop branches and use commit hashes only

        Returns:
            The details of the newly updated or created session
        """
        if request_params is None:
            request_params = {}
        request_params['drop_branches'] = drop_branches

        try:
            return self.put('configurations', config_name, json=request_body, req_param=request_params).json()
        except APIError as err:
            raise APIError(f'Failed to update CFS configuration {config_name}: {err}')
        except ValueError as err:
            raise APIError(f'Failed to parse JSON in response from CFS when updating '
                           f'CFS configuration {config_name}: {err}')

    def get_paged_resource(self, resource: str, params: Dict = None) -> Generator[Dict, None, None]:
        """Get a paged resource from the CFS API.

        Args:
            resource: the name of the resource to get (e.g. 'components')
            params: the parameters to pass to the GET on the resource
        """
        # On the first request, pass in user-specified parameters
        next_params = params
        try:
            while True:
                response = self.get(resource, params=next_params).json()
                yield from response[resource]
                next_params = response.get('next')
                if not next_params:
                    break
        except APIError as err:
            raise APIError(f'Failed to get CFS {resource}: {err}')
        except ValueError as err:
            raise APIError(f'Failed to parse JSON in response from CFS when getting '
                           f'{resource}: {err}')

    def get_components(self, params: Dict = None) -> Generator[Dict, None, None]:
        yield from self.get_paged_resource('components', params=params)

    def get_configurations(self, params: Dict = None) -> Generator[Dict, None, None]:
        yield from self.get_paged_resource('configurations', params=params)

    def get_sessions(self, params: Dict = None) -> Generator[Dict, None, None]:
        yield from self.get_paged_resource('sessions', params=params)
