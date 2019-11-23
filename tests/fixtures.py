
from prophetess.plugin import PluginBase


class FakePlugin(PluginBase):
    """An implementation of PluginBase to use for testing."""

    required_config = (
        'host',
        'port',
    )


class FakeExtractor(PluginBase):

    async def run(self):
        pass


class FakeLoader(PluginBase):

    async def run(self):
        pass


class FakeTransformer(PluginBase):

    async def run(self):
        pass
