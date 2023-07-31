#
# MIT License
#
# (C) Copyright 2021-2023 Hewlett Packard Enterprise Development LP
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
import os.path
from datetime import datetime, timedelta
from enum import Enum
from itertools import chain
import json
import logging
import re
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
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

from csm_api_client.service.gateway import APIError, APIGatewayClient
from csm_api_client.service.hsm import HSMClient
from csm_api_client.service.vcs import VCSError, VCSRepo
from csm_api_client.util import get_val_by_path

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


class CFSConfigurationLayer:
    """A layer in a CFS configuration."""

    # A mapping from the properties in CFS response data to attributes of this class
    CFS_PROPS_TO_ATTRS = {
        'cloneUrl': 'clone_url',
        'commit': 'commit',
        'branch': 'branch',
        'name': 'name',
        'playbook': 'playbook'
    }
    ATTRS_TO_CFS_PROPS = {val: key for key, val in CFS_PROPS_TO_ATTRS.items()}

    def __init__(self, clone_url: str,
                 name: Optional[str] = None,
                 playbook: Optional[str] = None,
                 commit: Optional[str] = None,
                 branch: Optional[str] = None) -> None:
        """Create a new CFSConfiguration.

        Args:
            clone_url: the git repository clone URL
            name (str, optional): the name of the CFS configuration layer. The
                name is optional.
            playbook: the name of the Ansible playbook. If
                omitted, the CFS-internal default is used.
            commit: the commit hash to use. Either `commit` or
                `branch` is required.
            branch: the git branch to use. Either `commit` or
                `branch` is required.

        Raises:
            ValueError: if neither `commit` nor `branch` is specified.
        """
        self.clone_url = clone_url
        self.name = name
        self.playbook = playbook
        if not (commit or branch):
            raise ValueError('Either commit or branch is required to create '
                             'a CFSConfigurationLayer.')
        self.commit = commit
        self.branch = branch

    @property
    def repo_path(self) -> str:
        """the path portion of the clone URL, e.g. /vcs/cray/sat-config-management.git"""
        return urlparse(self.clone_url).path

    def matches(self, other_layer: 'CFSConfigurationLayer') -> bool:
        """Determine whether this layer matches the given layer.

        Args:
            other_layer: the layer data

        Returns:
            True if the layer matches, False otherwise.
        """
        if not(isinstance(other_layer, CFSConfigurationLayer)):
            return False

        return self.repo_path == other_layer.repo_path and self.playbook == other_layer.playbook

    def get_updated_values(self, new_layer: 'CFSConfigurationLayer') -> Dict:
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

        Returns:
            None. Modifies `self.branch` and `self.commit`.

        Raises:
            CFSConfigurationError: if there is a failure to resolve the branch
                to a commit hash.
        """
        if not self.branch:
            # No branch to translate to commit
            return

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
        req_payload = {}
        for cfs_prop, attr in self.CFS_PROPS_TO_ATTRS.items():
            value = getattr(self, attr, None)
            if value:
                req_payload[cfs_prop] = value
        return req_payload

    def __str__(self) -> str:
        return (f'layer with repo path {self.repo_path} and '
                f'{f"playbook {self.playbook}" if self.playbook else "default playbook"}')

    @staticmethod
    def construct_name(product_or_repo: str,
                       playbook: Optional[str] = None,
                       commit: Optional[str] = None,
                       branch: Optional[str] = None) -> str:
        """Construct a name for the layer following a naming convention.

        Args:
            product_or_repo: the name of the product or repository
            playbook: the name of the playbook
            commit: the commit hash
            branch: the name of the branch. If both commit and branch
                are specified, branch is used in the name.

        Returns:
            the constructed layer name
        """
        playbook_name = os.path.splitext(playbook)[0] if playbook else 'site'
        branch_or_commit = branch or commit
        if not branch_or_commit:
            branch_or_commit = 'default'

        name_components = [
            product_or_repo,
            playbook_name,
            branch_or_commit[:MAX_BRANCH_NAME_WIDTH],
            datetime.now().strftime('%Y%m%dT%H%M%S')
        ]
        return '-'.join(name_components)

    @classmethod
    def from_product_catalog(cls, product_name: str, api_gw_host: str,
                             product_version: Optional[str] = None,
                             name: Optional[str] = None,
                             playbook: Optional[str] = None,
                             commit: Optional[str] = None,
                             branch: Optional[str] = None) -> 'CFSConfigurationLayer':
        """Create a new CFSConfigurationLayer from product catalog data.

        Args:
            product_name: the name of the product in the product catalog
            api_gw_host: the URL of the API gateway
            product_version: the version of the product in the
                product catalog. If omitted, the latest version is used.
            name: an optional name override
            playbook: the name of the Ansible playbook
            commit: an optional commit override
            branch: an optional branch override

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

        try:
            product = ProductCatalog().get_product(product_name, product_version)
        except ProductCatalogError as err:
            raise CFSConfigurationError(f'{fail_msg}: {err}')

        if not product.clone_url:
            raise CFSConfigurationError(f'{fail_msg}: {product} has no clone URL.')
        else:
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

        if not name:
            name = cls.construct_name(product_name, playbook=playbook,
                                      commit=commit, branch=branch)

        return CFSConfigurationLayer(clone_url, name=name, playbook=playbook,
                                     commit=commit, branch=branch)

    @classmethod
    def from_clone_url(cls, clone_url: str,
                       name: Optional[str] = None,
                       playbook: Optional[str] = None,
                       commit: Optional[str] = None,
                       branch: Optional[str] = None) -> 'CFSConfigurationLayer':
        """Create a new CFSConfigurationLayer from an explicit clone URL.

        Args:
            clone_url: the git repository clone URL
            name: an optional name override
            playbook: the name of the Ansible playbook
            commit: an optional commit override
            branch: an optional branch override

        Returns:
            the layer constructed from the product
        """
        def strip_suffix(s: str, suffix: str) -> str:
            if s.endswith(suffix):
                return s[:-len(suffix)]
            return s

        if not name:
            repo_name = os.path.basename(urlparse(clone_url).path)
            # Strip off the '.git' suffix then strip off '-config-management' if present
            short_repo_name = strip_suffix(strip_suffix(repo_name, '.git'), '-config-management')
            name = cls.construct_name(short_repo_name, playbook=playbook,
                                      commit=commit, branch=branch)

        return CFSConfigurationLayer(clone_url, name=name, playbook=playbook,
                                     commit=commit, branch=branch)

    @classmethod
    def from_cfs(cls, data: Dict) -> 'CFSConfigurationLayer':
        """Create a new CFSConfigurationLayer from data in a response from CFS.

        Args:
            data: the data for the layer from CFS.

        Returns:
            the CFS configuration layer retrieved from CFS configuration data
        """
        clone_url = data['cloneUrl']
        kwargs = {
            attr: data.get(cfs_prop)
            for cfs_prop, attr in cls.CFS_PROPS_TO_ATTRS.items()
            if cfs_prop != 'cloneUrl'
        }
        return cls(clone_url, **kwargs)


class CFSConfiguration:
    """Represents a single configuration in CFS."""

    def __init__(self, cfs_client: 'CFSClient', data: Dict) -> None:
        self._cfs_client = cfs_client
        self.data = data
        self.layers = [CFSConfigurationLayer.from_cfs(layer_data)
                       for layer_data in self.data.get('layers', [])]
        self.changed = False

    @classmethod
    def empty(cls, cfs_client: 'CFSClient') -> 'CFSConfiguration':
        """Get a new empty CFSConfiguration with no layers.

        Returns:
            a new empty configuration
        """
        return cls(cfs_client, {
            'layers': []
        })

    @property
    def name(self) -> Optional[str]:
        """the name of the CFS configuration"""
        return self.data.get('name')

    @property
    def req_payload(self) -> Dict:
        """a dict containing just the layers key used to update in requests"""
        return {'layers': [layer.req_payload for layer in self.layers]}

    def save_to_cfs(self, name: Optional[str] = None,
                    overwrite: bool = True) -> 'CFSConfiguration':
        """Save the configuration to CFS, optionally with a new name.

        Args:
            name: the name to save as. Required if this
                configuration does not yet have a name.
            overwrite: if True, silently overwrite an existing CFS
                configuration given by `name`. If False, raise a
                CFSConfigurationError if a configuration with `name` already
                exists

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

        # If overwriting is disabled, throw an error if we try to overwrite a
        # configuration
        if not overwrite:
            try:
                response = self._cfs_client.get('configurations', cfs_name, raise_not_ok=False)

                # If response was OK, that indicates there's already a CFS configuration
                if response.ok:
                    raise CFSConfigurationError(f'A configuration named {cfs_name} already exists '
                                                f'and will not be overwritten.')
                elif response.status_code != 404:
                    # If there's a failure for any reason other than a missing
                    # configuration, throw an APIError
                    self._cfs_client.raise_from_response(response)

            except APIError as err:
                raise CFSConfigurationError(f'Failed to retrieve CFS configuration "{cfs_name}": {err}')

        try:
            response_json = self._cfs_client.put('configurations', cfs_name,
                                                 json=self.req_payload).json()
        except APIError as err:
            raise CFSConfigurationError(f'Failed to update CFS configuration "{cfs_name}": {err}')
        except ValueError as err:
            raise CFSConfigurationError(f'Failed to decode JSON response from updating '
                                        f'CFS configuration "{cfs_name}": {err}')

        LOGGER.info('Successfully saved CFS configuration "%s"', cfs_name)
        return CFSConfiguration(self._cfs_client, response_json)

    def save_to_file(self, file_path: str, overwrite: bool = True) -> None:
        """Save the configuration to a file.

        Args:
            file_path: the path to the file where this config should be saved
            overwrite: if True, silently overwrite an existing file
            given by `file_path`. If False, raise a CFSConfigurationError if
            `file_path` already exists.

        Returns:
            None

        Raises:
            CFSConfigurationError: if there is a failure saving the configuration
                to the file, or if the file exists and overwrite is `False`.
        """
        try:
            with open(file_path, 'w' if overwrite else 'x') as f:
                json.dump(self.req_payload, f, indent=2)
        except FileExistsError:
            raise CFSConfigurationError(f'Configuration at path {file_path} already exists '
                                        f'and will not be overwritten.')
        except OSError as err:
            raise CFSConfigurationError(f'Failed to write to file file_path: {err}')

    def ensure_layer(self, layer: CFSConfigurationLayer, state: LayerState = LayerState.PRESENT) -> None:
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


class CFSImageConfigurationSession:
    """A class representing an image customization CFS configuration session

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
    # Hardcode the namespace in which kuberenetes jobs are created since CFS
    # sessions do not record this information
    KUBE_NAMESPACE = 'services'

    # Hardcode the container status messages
    CONTAINER_RUNNING_VALUE = 'running'
    CONTAINER_WAITING_VALUE = 'waiting'
    CONTAINER_SUCCEEDED_VALUE = 'succeeded'
    CONTAINER_FAILED_VALUE = 'failed'

    def __init__(self, data: Dict, cfs_client: 'CFSClient', image_name: str):
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
        start_time_str = get_val_by_path(self.data, 'status.session.startTime')
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
        # The Python Kuberenetes client docs are misleading in that they
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
                )
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
            for container_status in getattr(self.pod.status, f'{prefix}container_statuses'):
                container_name = container_status.name

                status = self.get_container_status_description(container_status)

                old_status = status_by_name.get(container_status.name)

                # First time reporting status or status has changed
                if not old_status or (status != old_status):
                    state_log_msgs.append(self.get_container_status_change_msg(container_name,
                                                                               status, old_status))

                status_by_name[container_name] = status

        if state_log_msgs:
            LOGGER.info(f'CFS session: {self.name: <{CFSClient.MAX_SESSION_NAME_LENGTH}} '
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


class CFSClient(APIGatewayClient):
    base_resource_path = 'cfs/v2/'
    MAX_SESSION_NAME_LENGTH = 45

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
        prefix_max_len = CFSClient.MAX_SESSION_NAME_LENGTH - len(uuid_str) - 1
        if len(prefix) > prefix_max_len:
            LOGGER.warning(f'Given CFS session prefix is too long and will be '
                           f'truncated ({len(prefix)} > {prefix_max_len})')
            prefix = prefix[:prefix_max_len]

        return '-'.join([prefix, uuid_str])

    def put_configuration(self, config_name: str, request_body: Dict) -> None:
        """Create a new configuration or update an existing configuration

        Args:
            config_name: the name of the configuration to create/update
            request_body: the configuration data, which should have a
                'layers' key.
        """
        self.put('configurations', config_name, json=request_body)

    def get_session(self, name: str) -> Dict:
        """Get details for a session.

        Args:
            name: the name of the session to get
k
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
        self, config_name: str, image_id: str,
        target_groups: Iterable[str], image_name: str
    ) -> CFSImageConfigurationSession:
        """Create a new image customization session.

        The session name will be generated with self.get_valid_session_name.

        Args:
            config_name: the name of the configuration to use
            image_id: the id of the IMS image to customize
            target_groups: the group names to target. Each group
                name specified here will be defined to point at the IMS image
                specified by `image_id`.
            image_name: the name of the image being created, used just
                for logging purposes

        Returns:
            The created session
        """
        request_body = {
            'name': self.get_valid_session_name(),
            'configurationName': config_name,
            'target': {
                'definition': 'image',
                'groups': [
                    {'name': target_group, 'members': [image_id]}
                    for target_group in target_groups
                ]
            }
        }
        try:
            created_session = self.post('sessions', json=request_body).json()
        except APIError as err:
            raise APIError(f'Failed to create image customization session : {err}')
        except ValueError as err:
            raise APIError(f'Failed to parse JSON in response from CFS when '
                           f'creating CFS session: {err}')

        return CFSImageConfigurationSession(created_session, self, image_name)

    def get_configuration(self, name: str) -> CFSConfiguration:
        """Get a CFS configuration by name."""
        try:
            config_data = self.get('configurations', name).json()
        except APIError as err:
            raise APIError(f'Could not retrieve configuration "{name}" from CFS: {err}')
        except json.JSONDecodeError as err:
            raise APIError(f'Invalid JSON response received from CFS when getting '
                           f'configuration "{name}" from CFS: {err}')
        return CFSConfiguration(self, config_data)

    def get_configurations_for_components(self, hsm_client: HSMClient, **kwargs: str) -> List[CFSConfiguration]:
        """Configurations for components matching the given params.

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

        LOGGER.info('Querying CFS configurations for the following NCNs: %s',
                    ', '.join(target_xnames))

        desired_config_names = set()
        for xname in target_xnames:
            try:
                desired_config_name = self.get('components', xname).json().get('desiredConfig')
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
