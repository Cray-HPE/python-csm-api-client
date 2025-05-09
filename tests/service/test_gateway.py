#
# MIT License
#
# (C) Copyright 2019-2025 Hewlett Packard Enterprise Development LP
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
Unit tests for csm_api_client.service.gateway
"""
from unittest import mock
import unittest
import json
from urllib.request import Request

import requests

from csm_api_client.session import Session
from csm_api_client.service.gateway import APIError, APIGatewayClient


def get_http_url_prefix(hostname):
    """Construct http URL prefix to help with assertions on requests.get calls."""
    return 'https://{}/apis/'.format(hostname)


class TestAPIGatewayClient(unittest.TestCase):
    """Tests for the APIGatewayClient class."""
    def setUp(self):
        self.api_gw_host = 'my-api-gw'
        self.mock_session = mock.MagicMock(autospec=Session, host=self.api_gw_host)
        self.mock_request = mock.MagicMock(autospec=Request)

        # Create a test API client class that sets the base_resource_path
        class TSTClient(APIGatewayClient):
            base_resource_path = 'tst/v2'
        self.tst_client_cls = TSTClient

    def test_setting_timeout_with_constructor(self):
        """Test setting the API client timeout with the constructor argument."""
        for timeout in range(10, 60, 10):
            with self.subTest(timeout=timeout):
                client = APIGatewayClient(self.mock_session, timeout=timeout)
                self.assertEqual(client.timeout, timeout)

    def test_make_req_base_resource_path(self):
        """Test that _make_req with no path makes a request on the base_resource_path"""
        client = self.tst_client_cls(self.mock_session)
        client._make_req(req_type='GET')
        self.mock_session.session.get.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + 'tst/v2',
            params=None, timeout=None
        )

    def test_make_req_base_resource_path_with_path(self):
        """Test that _make_req with a 1-element path requests on the join of path and base_resource_path"""
        client = self.tst_client_cls(self.mock_session)
        client._make_req('characters', req_type='GET')
        self.mock_session.session.get.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + 'tst/v2/characters',
            params=None, timeout=None
        )

    def test_make_req_base_resource_path_with_multi_path(self):
        """Test that _make_req with a 2-element path requests on the join of path and base_resource_path"""
        client = self.tst_client_cls(self.mock_session)
        client._make_req('characters', 'thing1', req_type='GET')
        self.mock_session.session.get.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + 'tst/v2/characters/thing1',
            params=None, timeout=None
        )

    def test_make_req_base_resource_path_trailing_slash_removed(self):
        """Test _make_req on a class that has a trailing slash in base_resource_path"""
        class TSTClient(APIGatewayClient):
            base_resource_path = 'tst/v2/'
        client = TSTClient(self.mock_session)
        client._make_req(req_type='GET')
        self.mock_session.session.get.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + 'tst/v2',
            params=None, timeout=None
        )

    def test_get_no_params(self):
        """Test get method with no additional params."""
        client = APIGatewayClient(self.mock_session, timeout=60)
        path_components = ['foo', 'bar', 'baz']
        response = client.get(*path_components)

        self.mock_session.session.get.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + '/'.join(path_components),
            params=None, timeout=60
        )
        self.assertEqual(response, self.mock_session.session.get.return_value)

    def test_get_with_params(self):
        """Test get method with additional params."""

        client = APIGatewayClient(self.mock_session, timeout=60)
        path_components = ['People']
        params = {'name': 'ryan'}
        response = client.get(*path_components, params=params)

        self.mock_session.session.get.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + '/'.join(path_components),
            params=params, timeout=60
        )
        self.assertEqual(response, self.mock_session.session.get.return_value)

    def test_get_exception(self):
        """Test get method with exception during GET."""
        client = APIGatewayClient(self.mock_session)
        self.mock_session.session.get.side_effect = requests.exceptions.RequestException
        path_components = ['foo', 'bar', 'baz']
        with self.assertRaises(APIError):
            client.get(*path_components)

    def test_post(self):
        """Test post method."""
        self.mock_request.content = json.dumps({}).encode('utf-8')
        self.mock_session.session.post.return_value = self.mock_request
        client = APIGatewayClient(self.mock_session, timeout=60)
        path_components = ['foo', 'bar', 'baz']
        payload = {}
        response = client.post(*path_components, payload=payload)

        self.mock_session.session.post.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + '/'.join(path_components),
            data=payload, json=None, timeout=60, params=None
        )
        self.assertEqual(response, self.mock_session.session.post.return_value)

    def test_post_exception(self):
        """Test post method with exception during POST."""
        self.mock_session.session.post.side_effect = requests.exceptions.RequestException
        client = APIGatewayClient(self.mock_session)
        path_components = ['foo', 'bar', 'baz']
        payload = {}
        with self.assertRaises(APIError):
            client.post(*path_components, payload=payload)

    def test_put(self):
        """Test put method."""
        self.mock_request.content = json.dumps({}).encode('utf-8')
        self.mock_session.session.put.return_value = self.mock_request
        client = APIGatewayClient(self.mock_session, timeout=60)
        path_components = ['foo', 'bar', 'baz']
        payload = {}
        client.put(*path_components, payload=payload)

        self.mock_session.session.put.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + '/'.join(path_components),
            data=payload, json=None, timeout=60, params=None
        )

    def test_put_with_params(self):
        """Test put method with additional params."""
        self.mock_request.content = json.dumps({}).encode('utf-8')
        self.mock_session.session.put.return_value = self.mock_request
        client = APIGatewayClient(self.mock_session, timeout=60)
        path_components = ['People']
        params = {'name': 'ryan'}
        payload = {}
        client.put(*path_components, payload=payload, req_param=params)

        self.mock_session.session.put.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + '/'.join(path_components),
            data=payload, json=None, timeout=60, params=params
        )

    def test_put_exception(self):
        """Test put method with exception during PUT."""
        self.mock_session.session.put.side_effect = requests.exceptions.RequestException
        client = APIGatewayClient(self.mock_session)
        path_components = ['foo', 'bar', 'baz']
        payload = {}
        with self.assertRaises(APIError):
            client.put(*path_components, payload=payload)

    def test_patch(self):
        """Test patch method."""
        self.mock_request.content = json.dumps({}).encode('utf-8')
        self.mock_session.session.patch.return_value = self.mock_request
        client = APIGatewayClient(self.mock_session, timeout=60)
        path_components = ['foo', 'bar', 'baz']
        payload = {}
        client.patch(*path_components, payload=payload)

        self.mock_session.session.patch.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + '/'.join(path_components),
            data=payload, json=None, timeout=60, params=None
        )

    def test_patch_json(self):
        """Test patch method with json payload."""
        self.mock_request.content = json.dumps({}).encode('utf-8')
        self.mock_session.session.patch.return_value = self.mock_request
        client = APIGatewayClient(self.mock_session, timeout=60)
        path_components = ['foo', 'bar', 'baz']
        json_payload = {'field': 'value'}
        client.patch(*path_components, json=json_payload)

        self.mock_session.session.patch.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + '/'.join(path_components),
            data=None, json=json_payload, timeout=60, params=None
        )

    def test_patch_exception(self):
        """Test patch method with exception during PATCH."""
        self.mock_session.session.patch.side_effect = requests.exceptions.RequestException
        client = APIGatewayClient(self.mock_session)
        path_components = ['foo', 'bar', 'baz']
        payload = {}
        with self.assertRaises(APIError):
            client.patch(*path_components, payload=payload)

    def test_delete(self):
        """Test delete method."""
        client = APIGatewayClient(self.mock_session, timeout=60)
        path_components = ['foo', 'bar', 'baz']
        response = client.delete(*path_components)

        self.mock_session.session.delete.assert_called_once_with(
            get_http_url_prefix(self.api_gw_host) + '/'.join(path_components), timeout=60
        )
        self.assertEqual(response, self.mock_session.session.delete.return_value)

    def test_delete_exception(self):
        """Test delete method with exception during DELETE."""
        self.mock_session.session.delete.side_effect = requests.exceptions.RequestException
        client = APIGatewayClient(self.mock_session)
        path_components = ['foo', 'bar', 'baz']
        with self.assertRaises(APIError):
            client.delete(*path_components)

    def test_request_failed_with_problem_description(self):
        """Test get, post, put, patch, and delete with fail HTTP codes and additional problem details"""
        client = APIGatewayClient(self.mock_session)
        path = 'fail'
        expected_url = f'{get_http_url_prefix(self.api_gw_host)}{path}'
        status_code = 400
        reason = 'Bad Request'
        problem_title = 'Title of problem'
        problem_detail = 'Details of problem and how to fix it'

        verbs_to_test = ['get', 'post', 'put', 'patch', 'delete']
        for verb in verbs_to_test:
            with self.subTest(verb=verb):
                mock_response = mock.Mock(ok=False, status_code=status_code, reason=reason)
                mock_response.request.method = verb.upper()
                mock_response.request.url = expected_url
                mock_response.json.return_value = {
                    'title': problem_title,
                    'detail': problem_detail
                }
                setattr(self.mock_session.session, verb, mock.Mock(return_value=mock_response))

                err_regex = (f"{verb.upper()} request to URL '{expected_url}' failed with status "
                             f"code {status_code}: {reason}. {problem_title} Detail: {problem_detail}")

                with self.assertRaisesRegex(APIError, err_regex):
                    getattr(client, verb)(path)

    def test_request_failed_no_problem_description(self):
        """Test get, post, put, patch, and delete with fail HTTP codes and no problem details"""
        client = APIGatewayClient(self.mock_session)
        path = 'fail'
        expected_url = f'{get_http_url_prefix(self.api_gw_host)}{path}'
        status_code = 400
        reason = 'Bad Request'

        verbs_to_test = ['get', 'post', 'put', 'patch', 'delete']
        for verb in verbs_to_test:
            with self.subTest(verb=verb):
                mock_response = mock.Mock(ok=False, status_code=status_code, reason=reason)
                mock_response.request.method = verb.upper()
                mock_response.request.url = expected_url
                mock_response.json.return_value = {}
                setattr(self.mock_session.session, verb, mock.Mock(return_value=mock_response))

                err_regex = (f"{verb.upper()} request to URL '{expected_url}' failed with "
                             f"status code {status_code}: {reason}")

                with self.assertRaisesRegex(APIError, err_regex):
                    getattr(client, verb)(path)

    def test_request_failed_invalid_json_response(self):
        """Test get, post, put, patch, and delete with fail HTTP codes and response not valid JSON"""
        client = APIGatewayClient(self.mock_session)
        path = 'fail'
        expected_url = f'{get_http_url_prefix(self.api_gw_host)}{path}'
        status_code = 400
        reason = 'Bad Request'

        verbs_to_test = ['get', 'post', 'put', 'patch', 'delete']
        for verb in verbs_to_test:
            with self.subTest(verb=verb):
                mock_response = mock.Mock(ok=False, status_code=status_code, reason=reason)
                mock_response.request.method = verb.upper()
                mock_response.request.url = expected_url
                mock_response.json.side_effect = ValueError
                setattr(self.mock_session.session, verb, mock.Mock(return_value=mock_response))

                err_regex = (f"{verb.upper()} request to URL '{expected_url}' failed with "
                             f"status code {status_code}: {reason}")

                with self.assertRaisesRegex(APIError, err_regex):
                    getattr(client, verb)(path)


if __name__ == '__main__':
    unittest.main()
