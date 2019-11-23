"""Unit tests for the prophetess.app package."""

from unittest.mock import patch

import asynctest
import pytest
from prophetess import app, exceptions, pipeline


class TestProphetess:

    @patch('prophetess.app.build_pipelines')
    def test_init(self, bp_mock):
        bp_mock.return_value = ['pipeline']

        mage = app.Prophetess({'test': 'config'})
        assert mage.config == {'test': 'config'}
        assert mage.pipelines == ['pipeline']

        bp_mock.assert_called_once_with({'test': 'config'})

    @pytest.mark.asyncio
    @asynctest.patch('prophetess.pipeline.Pipelines.close')
    async def test_close(self, close_mock):
        mage = app.Prophetess({})
        await mage.close()

        close_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_no_pipelines(self):
        mage = app.Prophetess({})
        await mage.run()

    @pytest.mark.asyncio
    async def test_run_with_pipelines(self):
        p1 = pipeline.Pipeline(id='p1', extractors=None, transform=None, loaders=None)
        p2 = pipeline.Pipeline(id='p2', extractors=None, transform=None, loaders=None)
        p1.run = asynctest.CoroutineMock()
        p2.run = asynctest.CoroutineMock()

        mage = app.Prophetess({})
        mage.pipelines.append(p1)
        mage.pipelines.append(p2)
        assert len(mage.pipelines) == 2

        await mage.run()
        p1.run.assert_called_once()
        p2.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_with_pipelines_error(self):
        p1 = pipeline.Pipeline(id='p1', extractors=None, transform=None, loaders=None)
        p2 = pipeline.Pipeline(id='p2', extractors=None, transform=None, loaders=None)
        p1.run = asynctest.CoroutineMock(side_effect=exceptions.ProphetessException)
        p2.run = asynctest.CoroutineMock()

        mage = app.Prophetess({})
        mage.pipelines.append(p1)
        mage.pipelines.append(p2)
        assert len(mage.pipelines) == 2

        await mage.run()
        p1.run.assert_called_once()
        p2.run.assert_called_once()
