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
OAuth2 authentication support.
"""

from abc import ABC, abstractmethod
import base64
from functools import cached_property
import json
import logging
import os.path
import os
from typing import Any, Dict, Optional

from oauthlib.oauth2 import (UnauthorizedClientError, MissingTokenError,
                             InvalidGrantError, LegacyApplicationClient)
import requests
from requests_oauthlib import OAuth2Session

from csm_api_client.k8s import load_kube_api


LOGGER = logging.getLogger(__name__)


class Session(ABC):
    """Manage API sessions, authentication, and token storage/retrieval."""

    TOKEN_URI = '/keycloak/realms/{}/protocol/openid-connect/token'
    tenant = 'shasta'
    client_id = 'shasta'

    def __init__(self, host: str, cert_verify: bool, session_opts: Optional[Dict[str, Any]] = None) -> None:
        self.host = host
        self.cert_verify = cert_verify

        client = LegacyApplicationClient(client_id=self.client_id, token=self.token)
        if self.token:
            client.parse_request_body_response(json.dumps(self.token))

        self.session = OAuth2Session(
            client=client,
            token=self.token,
            **(session_opts or {})
        )
        self.session.verify = self.cert_verify

    @property
    @abstractmethod
    def token(self) -> Optional[Dict[str, str]]:
        """Deserialized authentication token"""


class UserSession(Session):
    """Session for user authenticated with a saved token."""

    def __init__(self, host: str, cert_verify: bool, username: str, token_filename: str) -> None:
        """Initialize a Session. Wraps an OAuth2Session.

        Parameter management. Initialization of the OAuth2Session passes to
        self.get_session().

        Args:
            host: the API gateway hostname
            cert_verify: if True, verify TLS ceriticates when connecting. If
                False, skip ceritificate verification.
            username: the username whose token to use when authenticating
        """
        self.username = username
        self.token_filename = token_filename
        self.host = host

        super().__init__(host, cert_verify, self.session_opts)

    @cached_property
    def token(self) -> Optional[Dict[str, str]]:
        """dict: Deserialized authentication token.
        """

        if not os.path.exists(self.token_filename):
            return None

        try:
            with open(self.token_filename, 'r') as f:
                token = json.load(f)
                LOGGER.debug('Loaded auth token from %s.', self.token_filename)

        except json.JSONDecodeError as err:
            LOGGER.error('Unable to parse token: %s.', err)
            return None

        except OSError as err:
            LOGGER.error("Unable to create token file '%s': %s", self.token_filename, err)
            return None

        return token

    def save(self) -> None:
        """Serializes an authentication token.

        Tokens are stored with read permissions revoked for anyone
        other than the user or administrator.
        """

        if self.token is None:
            return

        token = self.token

        if 'client_id' not in token:
            token['client_id'] = self.client_id

        with open(self.token_filename, 'w') as f:
            # revoke read permissions from group/other
            os.fchmod(f.fileno(), 0o600)
            json.dump(token, f)

    @property
    def token_url(self) -> str:
        return 'https://{}{}'.format(self.host, self.TOKEN_URI.format(self.tenant))

    def fetch_token(self, password: str) -> None:
        """Fetch a new authentication token.

        Args:
            password: user password for authentication
        """

        opts = dict(client_id=self.client_id, verify=self.cert_verify)
        opts.update(self.session_opts)

        try:
            self._token = self.session.fetch_token(token_url=self.token_url,
                                                   username=self.username, password=password, **opts)
        except (MissingTokenError, UnauthorizedClientError, InvalidGrantError):
            # Avoid recording the authenticated user in the log file
            self._token = None

    @cached_property
    def session_opts(self) -> Dict:
        return dict(auto_refresh_url=self.token_url, token_updater=self.save,
                    auto_refresh_kwargs=dict(client_id=self.client_id))


class AdminSession(Session):
    """Manage API sessions, authentication, and token storage/retrieval.

    This Session class differs from the UserSession authentication mechanism as it
    retrieves the admin credentials from a Kubernetes secret instead of using
    user account credentials.
    """

    @cached_property
    def token(self) -> Optional[Dict[str, str]]:
        k8s = load_kube_api()
        admin_client_auth = k8s.read_namespaced_secret('admin-client-auth', 'default')
        auth_req_payload = {
            'grant_type': 'client_credentials',
            'client_id': 'admin-client',
            'client_secret': base64.b64decode(admin_client_auth.data['client-secret']).decode()
        }
        resp = requests.post(f'https://{self.host}{self.TOKEN_URI.format(self.tenant)}',
                             verify=self.cert_verify, data=auth_req_payload)
        return resp.json()
