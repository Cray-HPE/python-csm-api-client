# Changelog

(C) Copyright 2020-2023 Hewlett Packard Enterprise Development LP

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
