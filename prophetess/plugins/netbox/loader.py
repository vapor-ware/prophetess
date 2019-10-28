
import logging
import collections

from prophetess.plugin import Loader
from prophetess.plugins.netbox.client import NetboxClient
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

        self.update_method = self.config.get('update_method', 'update')
        self.client = NetboxClient(host=self.config.get('host'), api_key=self.config.get('api_key'))

    def sanitize_config(self, config):
        """ Overload Loader.sanitize_config to add additional conditioning """
        config = super().sanitize_config(config)

        for k in ('model', 'endpoint'):
            config[k] = config[k].lower()

        if not isinstance(config['pk'], list):
            config['pk'] = [config['pk']]

        return config

    async def lookup(self, record):
        extracts = self.config.get('lookup')
        if not extracts or not isinstance(extracts, collections.Mapping):
            return record

        for key, rules in extracts.items():
            if key not in record:
                log.debug('Skipping lookup "{}". Not found in record'.format(key))
                continue

            r = await self.client.entity(
                api=self.client.get_api(rules.get('endpoint')),
                endpoint=rules.get('endpoint'),
                model=rules.get('model'),
                params={k: record.get(key) for k in rules.get('pk', [])}
            )

            if not r:
                log.debug('Lookup for {} ({}) failed, no record found'.fortmat(key, record.get(key)))
                record[key] = None
                continue

            record[key] = r.id

        return record

    async def run(self, record):
        """ Overload Loader.run to execute netbox loading of a record """
        api = self.client.get_api(self.config.get('endpoint'))

        try:
            er = await self.client.entity(
                api=api,
                endpoint=self.config.get('endpoint'),
                model=self.config.get('model'),
                params={k: record.get(k) for k in self.config.get('pk')}
            )
        except:
            raise

        record = await self.lookup(record)

        payload = {
            'data': record
        }

        method = 'create'
        if er:
            method = self.update_method
            payload['id'] = er.id

        if method == 'partial_update':
            lookups = self.config.get('lookups', {}).keys()
            changed_record = {k: record[k] for k, v in record.items() if getattr(er, k) != v}
            if not changed_record:
                log.debug('Skipping {} as no data has changed'.format(record))
                return

            payload['data'] = changed_record

        func = self.client.build_model(api, self.config.get('endpoint'), self.config.get('model'), method)

        try:
            resp = await func(**payload)
        except ValueError:
            # Bad response
            raise
        except TypeError:
            # Bad parameters
            raise
