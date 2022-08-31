#
# MIT License
#
# (C) Copyright 2022 Hewlett Packard Enterprise Development LP
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

import base64
import unittest
from unittest.mock import patch

from csm_api_client.session import AdminSession, UserSession


class TestSession(unittest.TestCase):
    def setUp(self):
        self.host = 'api-gw-host.local'
        self.cert_verify = True
        self.user = 'a_user'
        self.token_filename = 'a_user_token.json'

        self.mock_legacy_app_client = patch('csm_api_client.session.LegacyApplicationClient').start()

        self.mock_kube_api = patch('csm_api_client.session.load_kube_api').start()
        self.admin_secret = base64.encodebytes(b'0123456789abcdef')
        self.mock_kube_api.return_value.read_namespaced_secret.return_value.data = {
            'client-secret': self.admin_secret
        }
        self.mock_post = patch('csm_api_client.session.requests.post').start()
        self.access_token = 'token_content'
        self.token_resp = {
            'access_token': self.access_token,
            'expires_in': 31536000,
            'refresh_expires_in': 31536000,
            'refresh_token': self.access_token,
            'token_type': 'bearer',
            'not-before-policy': 0,
            'session_state': '1234-4567-90abcdef',
            'scope': [
                'email',
                'profile',
            ],
            'expires_at': 1672552800,
            'client_id': 'shasta',
        }
        self.mock_post.return_value.json.return_value = self.token_resp

    def tearDown(self):
        patch.stopall()

    def test_creating_user_session(self):
        """Test that requests-oauthlib objects are constructed correctly"""
        session = UserSession(
            self.host,
            cert_verify=self.cert_verify,
            username=self.user,
            token_filename=self.token_filename
        )

        self.assertEqual(session.username, self.user)
        self.assertEqual(session.host, self.host)
        self.assertEqual(session.token_filename, self.token_filename)
        self.assertEqual(session.session.verify, self.cert_verify)

    def test_creating_admin_session(self):
        """Test that kubernetes is queried properly when constructing AdminSession"""
        session = AdminSession(
            self.host,
            cert_verify=self.cert_verify,
        )
        self.assertEqual(session.token, self.token_resp)
