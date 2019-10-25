
import aionetbox
import logging

from prophetess.plugin import Loader
from prophetess.plugins.netbox.exceptions import (
    InvalidPKConfig,
    InvalidNetboxEndpoint,
    InvalidNetboxOperation,
)

log = logging.getLogger(__name__)


class NetboxLoader(Loader):
    required_config = (
        'host',
        'api_key',
        'endpoint',
        'model',
        'pk',
    )

    def __init__(self, **kwargs):
        """ NetboxLoader init """
        super().__init__(**kwargs)

        self.aioconfig = aionetbox.Configuration()
        self.aioconfig.api_key['Authorization'] = self.config.get('api_key')
        self.aioconfig.api_key_prefix['Authorization'] = 'Token'
        self.aioconfig.host = self.config.get('host')

        self.__cache = {} # TODO: make a decorator that caches api classes?

        self.client = aionetbox.ApiClient(self.aioconfig)

    def sanitize_config(self, config):
        """ Overload Loader.sanitize_config to add additional conditioning """
        config = super().sanitize_config(config)

        for k in ('model', 'endpoint'):
            config[k] = config[k].lower()

        if not isinstance(config['pk'], list):
            config['pk'] = [config['pk']]

        return config

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

    async def get_entity(self, *, api, endpoint, model, params):
        """ Fetch a single record from netbox using one or more look up params """
        func = self.build_model(api, endpoint, model, 'list')
        try:
            data = await func(**params)
        except ValueError:
            # Bad Response
            raise
        except TypeError:
            # Bad params
            raise

        if data.count < 1:
            return None

        elif data.count > 1:
            kwargs = ', '.join('='.join(i) for i in params.items())
            raise InvalidPKConfig('Not enough criteria for {} <{}({})>'.format(self.id, func, kwargs))

        return data.results.pop(-1)

    async def run(self, record):
        """ Overload Loader.run to execute netbox loading of a record """
        api = self.get_api(self.config.get('endpoint'))
        func = self.build_model(api, self.config.get('endpoint'), self.config.get('model'), 'list')

        try:
            existing_record = await self.get_entity(
                api=api,
                endpoint=self.config.get('endpoint'),
                model=self.config.get('model'),
                params={k: record.get(k) for k in self.config.get('pk')}
            )
        except:
            raise

        payload = {
            'data': record
        }

        method = 'create'
        if existing_record:
            method = 'partial_update'
            payload['id'] = existing_record.id

        func = self.build_model(api, self.config.get('endpoint'), self.config.get('model'), method)

        try:
            resp = await func(**payload)
        except aionetbox.rest.ApiException:
            raise
        except ValueError:
            # Bad response
            raise
        except TypeError:
            # Bad parameters
            raise
