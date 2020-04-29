
import asyncio
from typing import Any, AsyncGenerator, Collection, Dict, Union

from prophetess.exceptions import InvalidConfigurationException
from prophetess.metrics import Timer, plugin_latency


class PluginBase(object):
    config = {}
    required_config = ()

    def __init__(
            self, *,
            id: str,
            config: Dict[str, Any],
            labels: Collection[str] = None,
            loop: asyncio.AbstractEventLoop = None,
    ) -> None:

        self.id = id
        self.config = self.sanitize_config(config)
        self._loop = loop
        self.timer = Timer(
            observer=plugin_latency,
            labels=(self.id,) + (labels or (type(self).__name__, type(self).__name__, type(self).__name__))
        )

    def sanitize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        if not all(required in config for required in self.required_config):
            raise InvalidConfigurationException('Missing required keys: {}'.format(', '.join(self.required_config)))

        return config

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop or asyncio.get_event_loop()

    async def close(self) -> None:
        pass

    def __str__(self) -> str:
        return '{}({})'.format(type(self).__name__, self.id)


class Extractor(PluginBase):

    async def run(self) -> Dict[str, Any]:
        raise NotImplementedError


class Loader(PluginBase):

    async def run(self, record: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class Transformer(PluginBase):

    @staticmethod
    def format(string: str, mapping: Dict[str, Any]) -> str:
        try:
            return string.format(**mapping)
        except (TypeError, KeyError):
            return None

    def parse(self, keys: Union[str, Dict[str, Any]], values: Dict[str, Any]) -> Union[str, Dict[str, Any]]:
        if isinstance(keys, str):
            return self.format(keys, values)

        return {k: self.parse(v, values) for k, v in keys.items()}

    async def run(self, data: Dict[str, Any]) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        yield self.parse(self.config, data)
