
import asyncio

from prophetess.exceptions import InvalidConfigurationException
from prophetess.metrics import Timer, plugin_latency


class PluginBase(object):
    config = {}
    required_config = ()

    def __init__(self, *, id, config, labels=None, loop=None):

        self.id = id
        self.config = self.sanitize_config(config)
        self._loop = loop
        self.timer = Timer(
            observer=plugin_latency,
            labels=(self.id,) + (labels or (type(self).__name__, type(self).__name__, type(self).__name__))
        )

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
        raise NotImplementedError


class Loader(PluginBase):

    async def run(self, record):
        raise NotImplementedError


class Transformer(PluginBase):

    def format(self, str, map):
        return str.format(**map)

    def parse(self, keys, values):
        if isinstance(keys, str):
            return self.format(keys, values)

        return {k: self.parse(v, values) for k, v in keys.items()}

    async def run(self, data):
        yield self.parse(self.config, data)
