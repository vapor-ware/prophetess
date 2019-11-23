"""Unit tests for the prophetess.pipeline package."""

from unittest.mock import call, patch

import asynctest
import pytest
from prophetess import exceptions, metrics, pipeline, plugin


def test_build_pipelines_empty():
    p = pipeline.build_pipelines({})
    assert len(p) == 0


@patch('prophetess.pipeline.build_plugin')
def test_build_pipelines_map_transform(build_mock):
    build_mock.side_effect = [
        plugin.Extractor(id='test-extract', config={'host': 'localhost'}),
        plugin.Loader(id='test-load', config={'host': 'localhost'}),
    ]

    cfg = {
        'extractors': {
            'test-extract': {
                'plugin': 'FakeExtractor',
                'config': {
                    'host': 'localhost',
                },
            },
        },
        'loaders': {
            'test-load': {
                'plugin': 'FakeLoader',
                'config': {
                    'host': 'localhost',
                },
            },
        },
        'pipelines': {
            'test-pipe': {
                'extractors': ['test-extract'],
                'loaders': ['test-load'],
                'transform': {
                    'name': '{Name}',
                }
            }
        }
    }

    p = pipeline.build_pipelines(cfg)
    assert len(p) == 1
    assert 'test-pipe' in p

    pipe = p['test-pipe']
    assert pipe.id == 'test-pipe'
    assert len(pipe.extractors) == 1
    assert len(pipe.loaders) == 1

    assert pipe.transform.id == 'YAMLTransformer'
    assert pipe.transform.config == {'name': '{Name}'}
    assert pipe.extractors[0].id == 'test-extract'
    assert pipe.extractors[0].config == {'host': 'localhost'}
    assert pipe.loaders[0].id == 'test-load'
    assert pipe.loaders[0].config == {'host': 'localhost'}

    build_mock.assert_has_calls([
        call('Extractor', 'test-extract', {'plugin': 'FakeExtractor', 'config': {'host': 'localhost'}}),
        call('Loader', 'test-load', {'plugin': 'FakeLoader', 'config': {'host': 'localhost'}}),
    ])


@patch('prophetess.pipeline.build_plugin')
def test_build_pipelines_str_transform(build_mock):
    build_mock.side_effect = [
        plugin.Transformer(id='test-transform', config={'host': 'localhost'}),
        plugin.Extractor(id='test-extract', config={'host': 'localhost'}),
        plugin.Loader(id='test-load', config={'host': 'localhost'}),
    ]

    cfg = {
        'extractors': {
            'test-extract': {
                'plugin': 'FakeExtractor',
                'config': {
                    'host': 'localhost',
                },
            },
        },
        'transformers': {
            'test-transform': {
                'plugin': 'FakeTransformer',
                'config': {
                    'host': 'localhost',
                }
            }
        },
        'loaders': {
            'test-load': {
                'plugin': 'FakeLoader',
                'config': {
                    'host': 'localhost',
                },
            },
        },
        'pipelines': {
            'test-pipe': {
                'extractors': ['test-extract'],
                'loaders': ['test-load'],
                'transform': 'test-transform',
            }
        }
    }

    p = pipeline.build_pipelines(cfg)
    assert len(p) == 1
    assert 'test-pipe' in p

    pipe = p['test-pipe']
    assert pipe.id == 'test-pipe'
    assert len(pipe.extractors) == 1
    assert len(pipe.loaders) == 1

    assert pipe.transform.id == 'test-transform'
    assert pipe.transform.config == {'host': 'localhost'}
    assert pipe.extractors[0].id == 'test-extract'
    assert pipe.extractors[0].config == {'host': 'localhost'}
    assert pipe.loaders[0].id == 'test-load'
    assert pipe.loaders[0].config == {'host': 'localhost'}

    build_mock.assert_has_calls([
        call('Transformer', 'test-transform', {'plugin': 'FakeTransformer', 'config': {'host': 'localhost'}}),
        call('Extractor', 'test-extract', {'plugin': 'FakeExtractor', 'config': {'host': 'localhost'}}),
        call('Loader', 'test-load', {'plugin': 'FakeLoader', 'config': {'host': 'localhost'}}),
    ])


@patch('prophetess.pipeline.build_plugin')
def test_build_pipelines_invalid_transform(build_mock):
    cfg = {
        'pipelines': {
            'test-pipe': {
                'extractors': ['test-extract'],
                'loaders': ['test-load'],
                'transform': [{
                    'name': '{Name}',
                }],
            }
        }
    }

    p = pipeline.build_pipelines(cfg)
    assert len(p) == 0
    build_mock.assert_not_called()


class TestPipelines:

    @pytest.mark.asyncio
    async def test_close(self):
        p1 = pipeline.Pipeline(id='1', extractors=None, transform=None, loaders=None)
        p1.close = asynctest.CoroutineMock()

        p = pipeline.Pipelines()
        p['1'] = p1

        await p.close()
        p1.close.assert_called_once()

    def test_append(self):
        p1 = pipeline.Pipeline(id='1', extractors=None, transform=None, loaders=None)

        p = pipeline.Pipelines()
        assert len(p) == 0

        p.append(p1)
        assert len(p) == 1
        assert '1' in p

    def test_str_fmt(self):
        p1 = pipeline.Pipeline(id='1', extractors=None, transform=None, loaders=None)

        p = pipeline.Pipelines()
        p['1'] = p1

        assert str(p) == 'Pipelines([Pipeline(1)])'


class TestPipeline:

    def test_init(self):
        p = pipeline.Pipeline(id='test', extractors=['extractor'], transform='transform', loaders=['loader'])
        assert p.id == 'test'
        assert p.extractors == ['extractor']
        assert p.transform == 'transform'
        assert p.loaders == ['loader']
        assert p.timer.histogram == metrics.pipeline_latency
        assert p.timer.labels == ('test',)

    @pytest.mark.asyncio
    async def test_close(self):
        extractor = plugin.PluginBase(id='test-extractor', config={})
        transformer = plugin.PluginBase(id='test-transformer', config={})
        loader = plugin.PluginBase(id='test-loader', config={})

        extractor.close = asynctest.CoroutineMock()
        transformer.close = asynctest.CoroutineMock()
        loader.close = asynctest.CoroutineMock()

        p = pipeline.Pipeline(
            id='test',
            extractors=[extractor],
            transform=transformer,
            loaders=[loader],
        )

        await p.close()

        extractor.close.assert_awaited_once()
        transformer.close.assert_awaited_once()
        loader.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run(self):
        extractor = plugin.PluginBase(id='test-extractor', config={})
        transformer = plugin.PluginBase(id='test-transformer', config={})
        loader = plugin.PluginBase(id='test-loader', config={})

        async def extract_fn(*args, **kwargs):
            yield 'extract-record'

        extractor.run = asynctest.MagicMock(side_effect=extract_fn)
        transformer.run = asynctest.CoroutineMock(return_value='transform-record')
        loader.run = asynctest.CoroutineMock(return_value='load-record')

        p = pipeline.Pipeline(
            id='test',
            extractors=[extractor],
            transform=transformer,
            loaders=[loader],
        )
        p.process = asynctest.CoroutineMock()

        assert p.timer._start_time is None
        await p.run()

        assert p.timer._start_time is not None
        transformer.run.assert_not_awaited()
        loader.run.assert_not_awaited()
        extractor.run.assert_called_once()
        p.process.assert_awaited_once_with('extract-record')

    @pytest.mark.asyncio
    async def test_process(self):
        extractor = plugin.PluginBase(id='test-extractor', config={})
        transformer = plugin.PluginBase(id='test-transformer', config={})
        loader = plugin.PluginBase(id='test-loader', config={})

        async def transform_fn(*args, **kwargs):
            yield 'transform-record'

        extractor.run = asynctest.CoroutineMock(side_effect='extract-record')
        transformer.run = asynctest.MagicMock(side_effect=transform_fn)
        loader.run = asynctest.CoroutineMock(return_value='load-record')

        p = pipeline.Pipeline(
            id='test',
            extractors=[extractor],
            transform=transformer,
            loaders=[loader],
        )
        p.load = asynctest.CoroutineMock()

        assert p.timer._start_time is None
        assert transformer.timer._start_time is None

        await p.process({'test': 'record'})

        assert p.timer._start_time is None
        assert transformer.timer._start_time is not None

        extractor.run.assert_not_awaited()
        loader.run.assert_not_awaited()
        transformer.run.assert_called_once()
        p.load.assert_awaited_once_with('transform-record')

    @pytest.mark.asyncio
    async def test_process_with_error(self):
        transformer = plugin.PluginBase(id='test-transformer', config={})
        transformer.run = asynctest.MagicMock(side_effect=KeyError)

        p = pipeline.Pipeline(
            id='test',
            extractors=[],
            transform=transformer,
            loaders=[],
        )
        p.load = asynctest.CoroutineMock()

        assert p.timer._start_time is None
        assert transformer.timer._start_time is None

        with pytest.raises(KeyError):
            await p.process({'test': 'record'})

        assert p.timer._start_time is None
        assert transformer.timer._start_time is not None

        transformer.run.assert_called_once()
        p.load.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_load_no_record(self):
        p = pipeline.Pipeline(
            id='test',
            extractors=[],
            transform=None,
            loaders=[],
        )

        assert p.timer._start_time is None
        await p.load(None)
        assert p.timer._start_time is None

    @pytest.mark.asyncio
    async def test_load(self):
        extractor = plugin.PluginBase(id='test-extractor', config={})
        transformer = plugin.PluginBase(id='test-transformer', config={})
        loader = plugin.PluginBase(id='test-loader', config={})

        extractor.run = asynctest.CoroutineMock(side_effect='extract-record')
        transformer.run = asynctest.CoroutineMock(side_effect='transform-record')
        loader.run = asynctest.CoroutineMock(return_value='load-record')

        p = pipeline.Pipeline(
            id='test',
            extractors=[extractor],
            transform=transformer,
            loaders=[loader],
        )

        assert p.timer._start_time is None
        assert loader.timer._start_time is None

        await p.load({'test': 'record'})

        assert p.timer._start_time is None
        assert loader.timer._start_time is not None

        extractor.run.assert_not_awaited()
        transformer.run.assert_not_awaited()
        loader.run.assert_awaited_once_with({'test': 'record'})

    @pytest.mark.asyncio
    async def test_load_error(self):
        loader = plugin.PluginBase(id='test-loader', config={})
        loader.run = asynctest.CoroutineMock(side_effect=exceptions.ProphetessException)

        p = pipeline.Pipeline(
            id='test',
            extractors=[],
            transform=None,
            loaders=[loader],
        )

        assert p.timer._start_time is None
        assert loader.timer._start_time is None

        await p.load({'test': 'record'})

        assert p.timer._start_time is None
        assert loader.timer._start_time is not None

        loader.run.assert_awaited_once_with({'test': 'record'})

    @pytest.mark.asyncio
    async def test_load_error_2(self):
        loader = plugin.PluginBase(id='test-loader', config={})
        loader.run = asynctest.CoroutineMock(side_effect=ValueError)

        p = pipeline.Pipeline(
            id='test',
            extractors=[],
            transform=None,
            loaders=[loader],
        )

        assert p.timer._start_time is None
        assert loader.timer._start_time is None

        await p.load({'test': 'record'})

        assert p.timer._start_time is None
        assert loader.timer._start_time is not None

        loader.run.assert_awaited_once_with({'test': 'record'})

    def test_str_fmt(self):
        p = pipeline.Pipeline(id='test', extractors=None, transform=None, loaders=None)

        assert str(p) == 'Pipeline(test)'
