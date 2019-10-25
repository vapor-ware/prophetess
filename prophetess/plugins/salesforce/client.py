"""Client for Salesforce API transactions."""

import asyncio
import datetime
import logging
import os

import aiohttp
import jwt

from prophetess.plugins.salesforce.exceptions import SalesforceAPIException

log = logging.getLogger(__name__)


class SFClient:
    """Handles reads, writes, and authentication with SFDC API."""

    def __init__(self, *, client_id, user, key, instance, audience='https://login.salesforce.com', loop=None):
        """Initialize a single instance with no authentication."""
        self.loop = loop or asyncio.get_event_loop()
        self.client_id = client_id
        self.user = user
        self.key = key
        self.instance = instance
        self.audience = audience
        self.token = None

    @property
    def headers(self):
        """Helper function to construct headers from access token.

        Returns:
            dict: Headers for authorizing SFDC API operations.
        """

        return {
            'Authorization': 'Bearer {}'.format(self.token)
        }

    def build_url(self, apiver='v41.0'):
        """Helper function to construct url from provided configuration.

        Returns:
            str: Base URL used to contact SFDC API instance.
        """

        url = 'https://{0}.salesforce.com/services/data/{1}'.format(self.instance, apiver)
        return url

    async def authenticate(self):
        """Request API authentication, refresh access token."""
        self.token = await self.get_access_token()

    async def get_access_token(self):
        """Request API access token.

        Returns:
            str: Access token to be used in subsequent SFDC API requests.
        """

        claims = {
            'iss': self.client_id,
            'sub': self.user,
            'exp': datetime.datetime.utcnow(),
            'aud': self.audience,
        }

        token = jwt.encode(
            claims,
            self.key,
            algorithm='RS256',
            headers={
                'alg': 'RS256',
                'typ': 'JWT',
            }
        ).decode('utf-8')

        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': token
        }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        try:
            payload = await self.request(
                'POST',
                'https://login.salesforce.com/services/oauth2/token',
                headers=headers,
                data=data
            )
            token = payload.get('access_token', None)

            assert token is not None
        except aiohttp.ClientError as e:
            raise SalesforceAPIException('Error during SFDC API authentication: {}'.format(e))
        except (AssertionError, AttributeError):
            raise SalesforceAPIException('Unexpected response from access token request: {}'.format(payload))

        return token

    async def request(self, method, url, *, params=None, headers=None, data=None):
        if headers is None:
            headers = self.headers

        async with aiohttp.request(method, url, headers=headers, data=data, params=params) as resp:
            return await resp.json()

    async def run_query(self, query, apiver='v41.0'):
        """Executes a SOQL query.

        Args:
            query (str): The SOQL query to execute.

        Returns:
            list: Records returned from query execution.

        Raises:
            SalesforceAPIException: Request failure of invalid response.
        """

        url = '{0}/query'.format(self.build_url(apiver))

        try:
            payload = await self.request('GET', url, params={'q': query})
            records = payload.get('records')
            assert records is not None
        except aiohttp.ClientError as e:
            raise SalesforceAPIException(e)
        except (AssertionError, AttributeError):
            raise SalesforceAPIException(
                'SOQL query returned invalid data: {}'.format(payload))

        return records
