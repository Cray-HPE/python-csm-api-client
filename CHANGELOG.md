# Changelog

(C) Copyright 2020-2024 Hewlett Packard Enterprise Development LP

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
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.2] - 2024-09-26

### Fixed
- Fixed for IUF stuck during `update-cfs-config` and `prepare-images` stage while
  executing `git ls-remote` fetching the credentials.

## [2.2.1] - 2024-09-11

### Fixed
- Updating the `poetry.lock` to pull the latest version & hash values for the packages.

## [2.2.0] - 2024-09-04

### Added
- Added support for CFS v3 while retaining support for CFS v2. The existing
  unversioned classes `CFSClient`, `CFSConfiguration`, and
  `CFSConfigurationLayer` are now aliases for the `CFSV2Client`,
  `CFSV2Configuration`, and `CFSV2ConfigurationLayer` classes. New classes
  prefixed with `CFSV3` are added for CFS V3.
- Added support for creating CFS configurations with the `additional_inventory`
  property.
- Added support for creating CFS configuration layers with the
  `ims_require_dkms` special parameter.

### Fixed
- When loading a CFS configuration into the appropriate `CFSV2Configuration` or
  `CFSV3Configuration` class, all properties known to CFS should now be retained
  when it is modified and saved back into CFS via the CFS API. Formerly, the
  `additional_inventory` of a CFS configuration and the `ims_require_dkms`
  property would have been dropped by this library.

## [2.1.2] - 2024-09-04

### Fixed
- Fixed how `put`, `patch`, and `post` methods of `APIGatewayClient` class pass
  their request bodies and request parameters, so that they can use both. To
  call one of these methods with request parameters, use the `req_param` keyword
  argument.

## [2.1.1] - 2024-09-04

### Fixed
- Updating the `poetry.lock` to pull the latest version & hash values for the packages.

## [2.1.0] - 2024-08-09

### Fixed
- Updating the `cray-product-catalog` to latest version that handles the split
  Kubernetes ConfigMap.

## [2.0.0] - 2024-08-08

### Added
- Added `get_api_version` method to `CFSClient` that queries the CFS API for the
  current API version.
- Added `supports_customized_image_name` property to `CFSClient` that checks
  whether the CFS version supports specifying the resultant image name for
  image customization sessions.
- Added use of the CFS API `image_map` parameter when creating CFS image
  customization sessions in the `create_image_customization_session` method of
  the `CFSClient` if the CFS version supports it. This passes the desired name
  of the customized image to the CFS session.

### Changed
- Changed the `APIGatewayClient` handling of `base_resource_path` so that it is
  possible to make a request on the `base_resource_path` without a trailing `/`.
  This is required to allow the `CFSClient` to do a GET on the endpoint
  `/cfs/v2`, which returns the semantic version of CFS. If any users of this
  class were expecting calls to `_make_req` (or `get`, `put`, etc.) without
  arguments to make a request to `base_resource_path` with a trailing slash,
  they must be updated to pass an empty string argument. No such instances are
  known in either the `sat` or `cfs-config-util` users of this library.

## [1.2.4] - 2024-04-25

### Fixed
- Relax the constraint on the version of the `kubernetes` package to allow this
  library to be used by different versions of sat that require different
  versions of Kubernetes matching the versions included in CSM.

## [1.2.3] - 2024-04-05

### Fixed
- Improve the build of `python-csm-api-client` to take less time by
  adding `poetry.lock` file that resolves the dependencies.

## [1.2.2] - 2024-03-26

### Fixed
- Update the `_update_container_status` method of the 
  CFSImageConfigurationSession class to handle the case when either
  `init_container_statuses` or `container_statuses` or both are None.

## [1.2.1] - 2024-03-26

### Changed
- updated `create_image_customization_session` to allow passing of session name 
  in order to maintain consistency with logging

## [1.2.0] - 2023-08-14

### Added
- Added the ability to back up existing CFS configurations or files when saving
  `CFSConfiguration` objects with `save_to_cfs` or `save_to_file` methods by
  adding a `backup_suffix` argument.
- Added a `get_component_ids_using_config` method to `CFSClient` that returns
  the IDs of all components using a given configuration as their desiredConfig.
- Added an `update_component` method to `CFSClient` that updates a given
  component in CFS.

## [1.1.5] - 2023-07-31

### Changed
- Upgrade build pipeline from Poetry 1.2.0 to Poetry 1.5.1
- Allow use of PyYAML versions later than 6.0 in order to allow us to pick up
  the latest version which fixes a compatibility issue with Cython 3.0.

### Fixed
- Restore Python 3.8 compatibility by fixing type hints to use `typing.List`
  instead of `list` with subscripts.

## [1.1.4] - 2023-04-19

### Changed
- Modified the `csm_api_client.k8s.load_kube_api()` helper function to prioritize
  loading the kube config from inside the cluster, and to fall back to the config
  file if the in-cluster config can't be read.

## [1.1.3] - 2023-04-10

### Fixed
- Fixed a bug where the incorrect pod was being recommended for debug log
  viewing when an image customization job failed.

## [1.1.2] - 2023-02-10

### Fixed
- Fixed the `UserSession.token` property to handle token fetching and existing
  tokens correctly.

## [1.1.1] - 2023-01-11

### Fixed
- Fixed the `APIGatewayClient._make_req` method to pass the `json` keyword
  argument through when making a PATCH request with the `patch` method.

## [1.1.0] - 2022-10-10

### Changed
- Changed the type signature of `APIGatewayClient.set_timeout()` to allow
  `None` as an argument to permit disabling request timeouts.

## [1.0.0] - 2022-08-26

### Added
- Initial release of standalone `python-csm-api-client` library.
