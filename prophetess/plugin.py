
import asyncio
import importlib

from prophetess.exceptions import InvalidConfigurationException


class PluginBase(object):
    config = {}
    required_config = ()

    def __init__(self, *, id, config, loop=None):

        self.id = id
        self.config = self.sanitize_config(config)
        self._loop = loop

    def sanitize_config(self, config):
        if not all(required in config for required in self.required_config):
            raise InvalidConfigurationException('Missing required keys: {}'.format(', '.join(self.required_config)))

        return config

    @property
    def loop(self):
        return self._loop or asyncio.get_event_loop()

    async def close(self):
        pass

    def __str__(self):
        return '{}({})'.format(type(self).__name__, self.id)


class Extractor(PluginBase):

    async def run(self):
        pass


class Loader(PluginBase):

    async def run(self, record):
        pass


class Transformer(PluginBase):

    def format(self, str, map):
        return str.format(**map)

    async def parse(self, keys, values):
        if isinstance(keys, str):
            return self.format(keys, values)

        return {k: await self.parse(v, values) for k, v in keys.items()}

    async def run(self, data):
        return await self.parse(self.config, data)
