#
# MIT License
#
# (C) Copyright 2019-2023 Hewlett Packard Enterprise Development LP
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
Client for querying the API gateway.
"""
from functools import wraps
import logging
import requests
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)
from urllib.parse import urlunparse

from requests.models import Response

from csm_api_client.session import Session


LOGGER = logging.getLogger(__name__)


def handle_api_errors(fn: Callable) -> Callable:
    """Decorator to handle common errors with API access.

    This decorator handles APIErrors, ValueErrors, and KeyErrors, raising all
    as APIErrors with appropriate prefixes determined from the name of the
    decorated function.
    """
    err_prefix = f'Failed to {fn.__name__.replace("_", " ")}'

    @wraps(fn)
    def inner(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except APIError as err:
            raise APIError(f'{err_prefix}: {err}') from err
        except ValueError as err:
            raise APIError(f'{err_prefix} due to bad JSON in response: {err}') from err
        except KeyError as err:
            raise APIError(f'{err_prefix} due to missing {err} key in response.') from err
    return inner


class APIError(Exception):
    """An exception occurred when making a request to the API."""
    pass


class ReadTimeout(Exception):
    """An timeout occurred when making a request to the API."""
    pass


class APIGatewayClient:
    """A client to the API Gateway."""

    # This can be set in subclasses to make a client for a specific API
    base_resource_path = ''

    def __init__(self, session: Session, timeout: Optional[int] = None):
        """Initialize the APIGatewayClient.

        Args:
            session: The Session instance to use when making REST calls,
                or None to make connections without a session.
            timeout: timeout to use for requests
        """
        self.session = session
        self.timeout = timeout

    def set_timeout(self, timeout: Optional[int]) -> None:
        self.timeout = timeout

    @staticmethod
    def raise_from_response(response: Response) -> None:
        """Raise an APIError based on the response body

        Args:
            response: the Response object from a request

        Raises:
            APIError: containing an error message with details from the request
        """
        api_err_msg = (f"{response.request.method} request to URL "
                       f"'{response.request.url}' failed with status "
                       f"code {response.status_code}: {response.reason}")
        # Attempt to get more information from response
        try:
            problem = response.json()
        except ValueError:
            raise APIError(api_err_msg)

        if 'title' in problem:
            api_err_msg += f'. {problem["title"]}'
        if 'detail' in problem:
            api_err_msg += f' Detail: {problem["detail"]}'

        raise APIError(api_err_msg)

    def _make_req(
        self,
        *args: str,
        req_type: str = 'GET',
        req_param: Optional[Dict] = None,
        json: Dict = None,
        raise_not_ok: bool = True,
    ) -> Response:
        """Perform HTTP request with type `req_type` to resource given in `args`.
        Args:
            *args: Variable length list of path components used to construct
                the path to the resource.
            req_type: Type of reqest (GET, STREAM, POST, PUT, or DELETE).
            req_param: Parameter(s) depending on request type.
            json: The data dict to encode as JSON and pass as the body of
                a POST request.
            raise_not_ok: If True and the response code is >=400, raise
                an APIError. If False, return the response object.

        Returns:
            The requests.models.Response object if the request was successful.

        Raises:
            ReadTimeout: if the req_type is STREAM and there is a ReadTimeout.
            APIError: if the status code of the response is >= 400 and
                raise_not_ok is True, or request raises a RequestException of any
                kind.
        """
        url = urlunparse(('https', self.session.host, 'apis/{}{}'.format(
            self.base_resource_path, '/'.join(args)), '', '', ''))

        LOGGER.debug("Issuing %s request to URL '%s'", req_type, url)

        requester = self.session.session

        try:
            if req_type == 'GET':
                r = requester.get(url, params=req_param, timeout=self.timeout)
            elif req_type == 'STREAM':
                r = requester.get(url, params=req_param, stream=True, timeout=self.timeout)
            elif req_type == 'POST':
                r = requester.post(url, data=req_param, json=json, timeout=self.timeout)
            elif req_type == 'PUT':
                r = requester.put(url, data=req_param, json=json, timeout=self.timeout)
            elif req_type == 'PATCH':
                r = requester.patch(url, data=req_param, json=json, timeout=self.timeout)
            elif req_type == 'DELETE':
                r = requester.delete(url, timeout=self.timeout)
            else:
                # Internal error not expected to occur.
                raise ValueError("Request type '{}' is invalid.".format(req_type))
        except requests.exceptions.ReadTimeout as err:
            if req_type == 'STREAM':
                raise ReadTimeout("{} request to URL '{}' timeout: {}".format(req_type, url, err))
            else:
                raise APIError("{} request to URL '{}' failed: {}".format(req_type, url, err))
        except requests.exceptions.RequestException as err:
            raise APIError("{} request to URL '{}' failed: {}".format(req_type, url, err))

        LOGGER.debug("Received response to %s request to URL '%s' "
                     "with status code: '%s': %s", req_type, r.url, r.status_code, r.reason)

        if raise_not_ok and not r.ok:
            self.raise_from_response(r)

        return r

    def get(self, *args: str, params: Optional[Dict] = None, **kwargs: Any) -> Response:
        """Issue an HTTP GET request to resource given in `args`.

        Args:
            *args: Variable length list of path components used to construct
                the path to the resource to GET.
            params: Parameters dictionary to pass through to request.get.

        Returns:
            The requests.models.Response object if the request was successful.

        Raises:
            APIError: if the status code of the response is >= 400 or requests.get
                raises a RequestException of any kind.
        """

        r = self._make_req(*args, req_type='GET', req_param=params, **kwargs)

        return r

    def stream(self, *args: str, params: Optional[Dict] = None, **kwargs: Any) -> Response:
        """Issue an HTTP GET stream request to resource given in `args`.

        Args:
            *args: Variable length list of path components used to construct
                the path to the resource to GET.
            params: Parameters dictionary to pass through to request.get.

        Returns:
            The requests.models.Response object if the request was successful.

        Raises:
            ReadTimeout: if there is a ReadTimeout.
            APIError: if the status code of the response is >= 400 or requests.get
                raises a RequestException of any kind.
        """

        r = self._make_req(*args, req_type='STREAM', req_param=params, **kwargs)

        return r

    def post(self, *args: str, payload: Optional[Dict] = None, json: Optional[Dict] = None, **kwargs: Any) -> Response:
        """Issue an HTTP POST request to resource given in `args`.

        Args:
            *args: Variable length list of path components used to construct
                the path to POST target.
            payload: The encoded data to send as the POST body.
            json: The data dict to encode as JSON and send as the POST body.

        Returns:
            The requests.models.Response object if the request was successful.

        Raises:
            APIError: if the status code of the response is >= 400 or requests.post
                raises a RequestException of any kind.
        """

        r = self._make_req(*args, req_type='POST', req_param=payload, json=json, **kwargs)

        return r

    def put(self, *args: str, payload: Optional[Dict] = None, json: Optional[Dict] = None, **kwargs: Any) -> Response:
        """Issue an HTTP PUT request to resource given in `args`.

        Args:
            *args: Variable length list of path components used to construct
                the path to PUT target.
            payload: The encoded data to send as the PUT body.
            json: The data dict to encode as JSON and send as the PUT body.

        Returns:
            The requests.models.Response object if the request was successful.

        Raises:
            APIError: if the status code of the response is >= 400 or requests.put
                raises a RequestException of any kind.
        """

        r = self._make_req(*args, req_type='PUT', req_param=payload, json=json, **kwargs)

        return r

    def patch(self, *args: str, payload: Optional[Dict] = None, json: Optional[Dict] = None, **kwargs: Any) -> Response:
        """Issue an HTTP PATCH request to resource given in `args`.

        Args:
            *args: Variable length list of path components used to construct
                the path to PATCH target.
            payload: The encoded data to send as the PATCH body.
            json: The data dict to encode as JSON and send as the PATCH body.

        Returns:
            The requests.models.Response object if the request was successful.

        Raises:
            APIError: if the status code of the response is >= 400 or requests.put
                raises a RequestException of any kind.
        """

        r = self._make_req(*args, req_type='PATCH', req_param=payload, json=json, **kwargs)

        return r

    def delete(self, *args: str, **kwargs: Any) -> Response:
        """Issue an HTTP DELETE resource given in `args`.

        Args:
            *args: Variable length list of path components used to construct
                the path to DELETE target.

        Returns:
            The requests.models.Response object if the request was successful.

        Raises:
            APIError: if the status code of the response is >= 400 or requests.delete
                raises a RequestException of any kind.
        """

        r = self._make_req(*args, req_type='DELETE', **kwargs)

        return r
