"""Unit tests for the prophetess.plugin package."""

import asyncio

import pytest
from prophetess import exceptions, metrics, plugin

from .fixtures import FakePlugin


class TestPluginBase:

    def test_init(self):
        p = FakePlugin(
            id='fake-plugin',
            config={'host': 'localhost', 'port': 5000},
        )

        assert p.id == 'fake-plugin'
        assert p.config == {'host': 'localhost', 'port': 5000}
        assert p._loop is None
        assert p.timer.histogram == metrics.plugin_latency
        assert p.timer.labels == ('fake-plugin', 'FakePlugin', 'FakePlugin', 'FakePlugin')

    def test_init_with_labels(self):
        p = FakePlugin(
            id='fake-plugin',
            config={'host': 'localhost', 'port': 5000},
            labels=('lab1', 'lab2')
        )

        assert p.id == 'fake-plugin'
        assert p.config == {'host': 'localhost', 'port': 5000}
        assert p._loop is None
        assert p.timer.histogram == metrics.plugin_latency
        assert p.timer.labels == ('fake-plugin', 'lab1', 'lab2')

    def test_sanitize_config_error(self):
        with pytest.raises(exceptions.InvalidConfigurationException):
            FakePlugin(
                id='fake-plugin',
                config={'host': 'localhost'},
            )

    def test_loop_default(self):
        p = plugin.PluginBase(
            id='test-plugin',
            config={},
        )

        assert p.loop == asyncio.get_event_loop()

    def test_loop_new(self):
        loop = asyncio.new_event_loop()
        p = plugin.PluginBase(
            id='test-plugin',
            config={},
            loop=loop,
        )

        assert p.loop == loop

    @pytest.mark.asyncio
    async def test_close(self):
        p = plugin.PluginBase(
            id='test-plugin',
            config={},
        )

        # this does nothing for the base class
        await p.close()

    def test_str_fmt(self):
        p = plugin.PluginBase(
            id='test-plugin',
            config={},
        )

        assert str(p) == 'PluginBase(test-plugin)'


@pytest.mark.asyncio
class TestExtractor:

    async def test_run(self):
        extractor = plugin.Extractor(
            id='test-extractor',
            config={},
        )

        with pytest.raises(NotImplementedError):
            await extractor.run()


@pytest.mark.asyncio
class TestLoader:

    async def test_run(self):
        loader = plugin.Loader(
            id='test-loader',
            config={},
        )

        with pytest.raises(NotImplementedError):
            await loader.run({})


class TestTransformer:

    def test_format(self):
        transformer = plugin.Transformer(
            id='test-transformer',
            config={},
        )

        val = transformer.format('host={host} port={port}', {'host': 'localhost', 'port': 5000})
        assert val == 'host=localhost port=5000'

    def test_parse_string_key(self):
        transformer = plugin.Transformer(
            id='test-transformer',
            config={},
        )

        val = transformer.parse('foo{val}', {'val': 'bar'})
        assert val == 'foobar'

    def test_parse_dict_key(self):
        transformer = plugin.Transformer(
            id='test-transformer',
            config={},
        )

        val = transformer.parse({'key': 'foo{val}'}, {'val': 'bar'})
        assert val == {'key': 'foobar'}

    @pytest.mark.asyncio
    async def test_run(self):
        transformer = plugin.Transformer(
            id='test-transformer',
            config={
                'host': 'localhost-{host}',
                'port': '5000',
            },
        )

        vals = [t async for t in transformer.run({'host': 1})]
        assert vals == [{
            'host': 'localhost-1',
            'port': '5000',
        }]
