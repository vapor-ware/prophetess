"""Client for Netbox API transactions."""

import asyncio
import logging

import aionetbox

from prophetess.plugins.netbox.exceptions import (
    InvalidPKConfig,
    InvalidNetboxEndpoint,
    InvalidNetboxOperation,
)

log = logging.getLogger(__name__)


class NetboxClient:
    """Re-usable abstraction to aionetbox"""

    def __init__(self, *, host, api_key, loop=None):
        """Initialize a single instance with no authentication."""
        self.loop = loop or asyncio.get_event_loop()
        self.aioconfig = aionetbox.Configuration()
        self.aioconfig.api_key['Authorization'] = api_key
        self.aioconfig.api_key_prefix['Authorization'] = 'Token'
        self.aioconfig.host = host

        self.__cache = {} # TODO: make a decorator that caches api classes?

        self.client = aionetbox.ApiClient(self.aioconfig)

    async def close(self):
        await self.client.rest_client.pool_manager.close() # Fucking swagger code gen.

    def get_api(self, endpoint):
        """ Initialize an Api endpoint from aionetbox """
        name = '{}Api'.format(endpoint.capitalize())
        try:
            return getattr(aionetbox, name)(self.client)
        except AttributeError:
            raise InvalidNetboxEndpoint('{} module not found'.format(name))

    def build_model(self, api, endpoint, method, action):
        """ Return the aionetbox Api method from an endpoint class """
        name = '{}_{}_{}'.format(endpoint, method, action)
        try:
            return getattr(api, name)
        except AttributeError:
            raise InvalidNetboxOperation('{} not a valid operation'.format(name))

    async def __get(self, *, api, endpoint, model, params):
        func = self.build_model(api, endpoint, model, 'list')
        try:
            return await func(**params)
        except ValueError:
            # Bad Response
            raise
        except TypeError:
            # Bad params
            raise

    async def entity(self, *, api, endpoint, model, params):
        """ Fetch a single record from netbox using one or more look up params """

        data = await self.__get(api=api, endpoint=endpoint, model=model, params=params)

        if data.count < 1:
            return None

        elif data.count > 1:
            kwargs = ', '.join('='.join(i) for i in params.items())
            raise InvalidPKConfig('Not enough criteria for {} <{}({})>'.format(self.id, func, kwargs))

        return data.results.pop(-1)

    async def entities(self, *, api, endpoint, model, params):
        """ Fetch all matching records from netbox using one or more look up params """

        data = await self.__get(api=api, endpoint=endpoint, model=model, params=params)

        if data.count < 1:
            return None

        return data.results
