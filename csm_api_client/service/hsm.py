#
# MIT License
#
# (C) Copyright 2019-2022 Hewlett Packard Enterprise Development LP
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
Client for querying the Hardware State Manager (HSM) API
"""
import logging
from typing import (
    Dict,
    Iterable,
    Optional,
    Set,
    Tuple,
)

from csm_api_client.service.gateway import (
    APIError,
    APIGatewayClient,
    handle_api_errors,
)
from csm_api_client.xname import XName


BMC_TYPES = ('NodeBMC', 'RouterBMC', 'ChassisBMC')
LOGGER = logging.getLogger(__name__)

Components = Iterable[Dict[str, str]]


class HSMClient(APIGatewayClient):
    base_resource_path = 'smd/hsm/v2/'

    def get_bmcs_by_type(
        self,
        bmc_type: Optional[str] = None,
        check_keys: bool = True
    ) -> Iterable[Dict]:
        """Get a list of BMCs, optionally of a single type.

        Args:
            bmc_type: Any HSM BMC type: NodeBMC, RouterBMC or ChassisBMC.
            check_keys: Whether or not to filter data based on missing keys.

        Returns:
            A list of dictionaries where each dictionary describes a BMC.

        Raises:
            APIError: if the API query failed or returned an invalid response.
        """
        try:
            response = self.get(
                'Inventory', 'RedfishEndpoints', params={'type': bmc_type} if bmc_type else {}
            )
        except APIError as err:
            raise APIError(f'Failed to get BMCs from HSM API: {err}')

        try:
            redfish_endpoints = response.json()['RedfishEndpoints']
        except ValueError as err:
            raise APIError(f'API response could not be parsed as JSON: {err}')
        except KeyError as err:
            raise APIError(f'API response missing expected key: {err}')

        # Check that the returned data has expected keys, and exclude data without it.
        invalid_redfish_endpoint_xnames = []
        if check_keys:
            invalid_redfish_endpoint_xnames = [
                endpoint.get('ID') for endpoint in redfish_endpoints
                if any(required_key not in endpoint for required_key in ['ID', 'Enabled', 'DiscoveryInfo'])
                or 'LastDiscoveryStatus' not in endpoint['DiscoveryInfo']
            ]
        if invalid_redfish_endpoint_xnames:
            LOGGER.warning(
                'The following xnames were excluded due to incomplete information from HSM: %s',
                ', '.join(invalid_redfish_endpoint_xnames)
            )

        return [
            endpoint for endpoint in redfish_endpoints
            if endpoint.get('ID') not in invalid_redfish_endpoint_xnames
        ]

    def get_and_filter_bmcs(
        self,
        bmc_types: Tuple[str, ...] = BMC_TYPES,
        include_disabled: bool = False,
        include_failed_discovery: bool = False,
        xnames: Optional[Iterable[str]] = None
    ) -> Set[str]:
        """Get all BMCs of a given type, optionally filtering against a list of xnames.

        Args:
            bmc_types: Any combination of ('NodeBMC', 'RouterBMC', 'ChassisBMC')
            include_disabled: if True, include disabled nodes.
            include_failed_discovery: if True, include nodes which had discovery errors.
            xnames: A list of xnames to filter against the data from HSM.

        Returns:
            A set of xnames.

        Raises:
            APIError: if an API query failed or returned an invalid response.
        """
        if set(bmc_types) == set(BMC_TYPES):
            bmcs = self.get_bmcs_by_type()
        else:
            bmcs = []
            for bmc_type in bmc_types:
                bmcs.extend(self.get_bmcs_by_type(bmc_type))

        # Filter given xnames by type
        hsm_xnames = set(bmc['ID'] for bmc in bmcs)
        if xnames:
            type_excluded_xnames = set(xnames) - hsm_xnames
            xnames = set(xnames).intersection(hsm_xnames)
            if type_excluded_xnames:
                LOGGER.warning(
                    'The following xnames will be excluded as they are not type(s) %s: %s',
                    ', '.join(bmc_types), ', '.join(type_excluded_xnames)
                )
        else:
            xnames = hsm_xnames

        # Filter out disabled components
        if not include_disabled:
            disabled_xnames = set(bmc['ID'] for bmc in bmcs if not bmc['Enabled'])
            disabled_xnames_to_report = set(xnames).intersection(disabled_xnames)
            if disabled_xnames_to_report:
                LOGGER.warning(
                    'Excluding the following xnames which are disabled: %s',
                    ', '.join(disabled_xnames_to_report)
                )
            xnames = xnames - disabled_xnames

        # Filter out components for which discovery failed
        if not include_failed_discovery:
            failed_discovery_xnames = set(
                bmc['ID'] for bmc in bmcs if bmc['DiscoveryInfo']['LastDiscoveryStatus'] != 'DiscoverOK'
            )
            failed_discovery_xnames_to_report = set(xnames).intersection(failed_discovery_xnames)
            if failed_discovery_xnames_to_report:
                LOGGER.warning(
                    'Excluding the following xnames which have a LastDiscoveryStatus other than "DiscoverOK": %s',
                    ', '.join(failed_discovery_xnames_to_report)
                )
            xnames = xnames - failed_discovery_xnames

        return xnames

    def get_component_xnames(self, params: Optional[Dict[str, str]] = None, omit_empty: bool = True) -> Iterable[str]:
        """Get the xnames of components matching the given criteria.

        If any args are omitted, the results are not limited by that criteria.

        Args:
            params: the parameters to pass in the GET request to the
                '/State/Components' URL in HSM. E.g.:
                    {
                        'type': 'Node',
                        'role': 'Compute',
                        'class': 'Mountain'
                    }
            omit_empty: if True, omit the components with "State": "Empty"

        Returns:
            the xnames matching the given filters

        Raises:
            APIError: if there is a failure querying the HSM API or getting
                the required information from the response.
        """
        if params:
            params_string = f' with {", ".join(f"{key}={value}" for key, value in params.items())}'
        else:
            params_string = ''

        err_prefix = f'Failed to get components{params_string}.'

        try:
            components = self.get('State', 'Components', params=params).json()['Components']
        except APIError as err:
            raise APIError(f'{err_prefix}: {err}')
        except ValueError as err:
            raise APIError(f'{err_prefix} due to bad JSON in response: {err}')
        except KeyError as err:
            raise APIError(f'{err_prefix} due to missing {err} key in response.')

        try:
            if omit_empty:
                return [component['ID'] for component in components
                        if component['State'] != 'Empty']
            else:
                return [component['ID'] for component in components]
        except KeyError as err:
            raise APIError(f'{err_prefix} due to missing {err} key in list of components.')

    @handle_api_errors
    def query_components(self, component: Optional[str] = None, **kwargs: str) -> Iterable[Dict[str, str]]:
        """Query the HSM database to retrieve components matching given parameters.

        Args:
            component: if a str, then query HSM for that
                component. If None, retrieve all components matching the parameters.
            kwargs: keyword arguments should correspond to parameters accepted
                by the /State/Components/Query HSM API.
        Returns:
            list of dictionaries of Node components.
        Raises:
            APIError: if there is a failure querying the HSM API or getting
                the required information from the response, or if the component
                xname is invalid.
        """
        # TODO: Consolidate query_components() and get_node_components().
        if component:
            component_xname = XName(component)
            if not component_xname.is_valid:
                raise APIError(f'Could not query component {component}: invalid xname')

            components = self.get('State', 'Components', 'Query', str(component_xname), params=kwargs).json()

        else:
            components = self.get('State', 'Components', params=kwargs).json()

        return components['Components']

    def get_node_components(self, ancestor: Optional[str] = None) -> Components:
        """Get the components of Type=Node from HSM.

        Args:
            ancestor: a component xname, which, if specified, is the
                ancestor of all node components returned by this function

        Returns:
            list of dictionaries of Node components.

        Raises:
            APIError: if there is a failure querying the HSM API or getting
                the required information from the response, or if the ancestor
                xname is invalid.
        """

        err_prefix = 'Failed to get Node components'
        try:
            if ancestor:
                ancestor_xname = XName(ancestor)
                if not ancestor_xname.is_valid:
                    raise APIError(f'Could not get descendants of {ancestor}: invalid xname')

                components = self.get('State', 'Components', 'Query', str(ancestor_xname),
                                      params={'type': 'Node'}).json()
            else:
                components = self.get('State', 'Components', params={'type': 'Node'}).json()

            components = components['Components']
        except APIError as err:
            raise APIError(f'{err_prefix}: {err}')
        except ValueError as err:
            raise APIError(f'{err_prefix} due to bad JSON in response: {err}')
        except KeyError as err:
            raise APIError(f'{err_prefix} due to missing {err} key in response.')

        return components

    def get_all_components(self) -> Components:
        """Get all components from HSM.

        Returns:
            components: A list of dictionaries from HSM.

        Raises:
            APIError: if there is a failure querying the HSM API or getting
                the required information from the response.
        """

        err_prefix = 'Failed to get HSM components'
        try:
            components = self.get('State', 'Components').json()['Components']
        except APIError as err:
            raise APIError(f'{err_prefix}: {err}')
        except ValueError as err:
            raise APIError(f'{err_prefix} due to bad JSON in response: {err}')
        except KeyError as err:
            raise APIError(f'{err_prefix} due to missing {err} key in response.')

        return components

    def get_component_history_by_id(self, cid: Optional[str] = None, by_fru: bool = False) -> Components:
        """Get component history from HSM, optionally for a single ID or FRUID.

        Args:
            cid: A component ID which is either an xname or FRUID or None.
            by_fru: if True, query HSM history using HardwareByFRU.

        Returns:
            A list of dictionaries from HSM with component history or None.

        Raises:
            APIError: if there is a failure querying the HSM API or getting
                the required information from the response.
        """
        err_prefix = 'Failed to get HSM component history'
        params = {}
        if by_fru:
            inventory_type = 'HardwareByFRU'
            if cid:
                params = {'fruid': cid}
        else:
            inventory_type = 'Hardware'
            if cid:
                params = {'id': cid}

        try:
            components = self.get('Inventory', inventory_type, 'History', params=params).json()['Components']
        except APIError as err:
            raise APIError(f'{err_prefix}: {err}')
        except ValueError as err:
            raise APIError(f'{err_prefix} due to bad JSON in response: {err}')
        except KeyError as err:
            raise APIError(f'{err_prefix} due to missing {err} key in response.')

        return components

    def get_component_history(self, cids: Optional[Set[str]] = None, by_fru: bool = False) -> Components:
        """Get component history from HSM.

        Args:
            cids: A set of component IDs which are either an xname or FRUID or None.
            by_fru: if True, query HSM history using HardwareByFRU.

        Returns:
            A list of dictionaries from HSM with component history.

        Raises:
            APIError: if there is a failure querying the HSM API or getting
                the required information from the response.
        """

        if not cids:
            components = self.get_component_history_by_id(None, by_fru)
        else:
            components = []
            for cid in cids:
                # An exception is raised if HSM API returns a 400 when
                # an xname has an invalid format.
                # If the cid is a FRUID or a correctly formatted xname
                # that does not exist in the hardware inventory,
                # then None is returned because History is an empty list.
                # In either case (exception or None is returned),
                # keep going and try to get history for other cids.
                try:
                    component_history = self.get_component_history_by_id(cid, by_fru)
                    if component_history:
                        components.extend(component_history)
                except APIError as err:
                    LOGGER.debug(f'HSM API error for {cid}: {err}')

        return components

    @handle_api_errors
    def set_component_enabled(self, xname: str, *, enabled: bool) -> None:
        """Enable or disable a component in HSM inventory

        Args:
            xname: the xname of the component to modify
            enabled: if True, enable the component. If False, disable
                the component.
        """
        self.patch(
            'State', 'Components', xname, 'Enabled',
            json={
                'enabled': enabled
            }
        )

    @handle_api_errors
    def set_redfish_endpoint_enabled(self, xname: str, *, enabled: bool) -> None:
        """Enable or disable a Redfish endpoint in HSM inventory

        Args:
            xname: the xname of the component to modify
            enabled: if True, enable the Redfish endpoint. If False,
                disable the Redfish endpoint.
        """
        self.patch(
            'Inventory', 'RedfishEndpoints', xname,
            json={
                'enabled': enabled
            }
        )

    @handle_api_errors
    def get_ethernet_interfaces(self, xname: Optional[str] = None) -> Components:
        """Get ethernet interfaces for some component

        Args:
            xname: the xname of the component for which to search for ethernet interfaces

        Returns:
            list of dict: interfaces retrieved from HSM for the given xname, or
                all interfaces if xname is None

        Raises:
            APIError: if there is an issue retrieving ethernet interfaces from HSM
        """
        all_interfaces = self.get('Inventory', 'EthernetInterfaces').json()
        if not xname:
            return all_interfaces
        component_xname = XName(xname)
        return [
            interface for interface in all_interfaces
            if component_xname.contains_component(XName(interface['ComponentID']))
        ]

    @handle_api_errors
    def delete_ethernet_interface(self, interface_id: str) -> None:
        """Delete an ethernet interface from HSM

        Args:
            interface_id: the ID of the ethernet interface to delete

        Raises:
            APIError: if there is an issue deleting the ethernet interface from HSM
        """
        self.delete('Inventory', 'EthernetInterfaces', interface_id)

    @handle_api_errors
    def delete_redfish_endpoint(self, redfish_endpoint: str) -> None:
        """Delete a Redfish endpoint from HSM

        Args:
            redfish_endpoint: the xname of the Redfish endpoint to delete

        Raises:
            APIError: if there is an issue deleting the Redfish endpoint from HSM
        """
        self.delete('Inventory', 'RedfishEndpoints', redfish_endpoint)

    @handle_api_errors
    def begin_discovery(self, xnames: Iterable[str], force: bool = False) -> None:
        """Kick off discovery of the given xname.

        Args:
            xnames: the xnames to discover
            force: if True, force discovery.

        Raises:
            APIError: if there is a problem discovering the given xnames
        """
        self.post('Inventory', 'Discover', json={'xnames': list(xnames), 'force': force})

    @handle_api_errors
    def get_redfish_endpoint_inventory(self, xname: str) -> Components:
        """Get the Redfish endpoint inventory for some component.

        Args:
            xname: the xname to retrieve Redfish endpoints for

        Raises:
            APIError: if there is a problem retrieving the Redfish endpoint inventory
        """
        return self.get('Inventory', 'RedfishEndpoints', xname).json()

    @handle_api_errors
    def bulk_enable_components(self, components: Iterable[str]) -> None:
        """Bulk enable a set of components.

        Args:
            components: xnames of components which should be bulk-enabled.

        Raises:
            APIError: if there is a problem bulk-enabling the components.
        """
        self.patch('State', 'Components', 'BulkEnabled', json={
            'Enabled': True,
            'ComponentIDs': list(components)
        })

    @handle_api_errors
    def create_ethernet_interface(self, interface: Dict[str, str]) -> None:
        """Create an ethernet interface in HSM.

        Args:
            interface: a dictionary with the following keys:
                Description: the description of the interface
                MACAddress: the MAC address of the interface
                IPAddress: the IP address of the interface
                ComponentID: the xname of the associated component

        Raises:
            APIError: if the interface cannot be created
        """
        self.post('Inventory', 'EthernetInterfaces', json=interface)
