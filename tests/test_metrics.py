"""Unit tests for the prophetess.metrics package."""

from unittest.mock import patch

import pytest

from prophetess import metrics


class TestTimer:

    def test_init(self):
        t = metrics.Timer(
            observer=metrics.plugin_latency,
            labels=('a', 'b', 'c', 'd'),
        )
        assert t.histogram == metrics.plugin_latency
        assert t._start_time is None
        assert t.labels == ('a', 'b', 'c', 'd')

    @patch('time.time')
    def test_start(self, time_mock):
        time_mock.return_value = 123456

        t = metrics.Timer(
            observer=metrics.plugin_latency,
            labels=('a', 'b', 'c', 'd'),
        )
        t.start()
        assert t._start_time == 123456

    @patch('time.time')
    @patch('prometheus_client.Histogram.observe')
    def test_stop(self, observe_mock, time_mock):
        # NOTE: need three time values here because it appears that time.time is called
        #  somewhere else in the call path, likely within a dependency.
        time_mock.side_effect = [123456, 123460, 123462]

        t = metrics.Timer(
            observer=metrics.plugin_latency,
            labels=('a', 'b', 'c', 'd'),
        )
        t.start()
        t.stop()

        assert t._start_time == 123456
        observe_mock.assert_called_once_with(4)

    @patch('time.time')
    @patch('prometheus_client.Histogram.observe')
    def test_context_managed_ok(self, observe_mock, time_mock):
        # NOTE: need three time values here because it appears that time.time is called
        #  somewhere else in the call path, likely within a dependency.
        time_mock.side_effect = [123456, 123460, 123462]

        t = metrics.Timer(
            observer=metrics.plugin_latency,
            labels=('a', 'b', 'c', 'd'),
        )

        with t:
            pass

        assert t._start_time == 123456
        observe_mock.assert_called_once_with(4)

    @patch('time.time')
    @patch('prometheus_client.Histogram.observe')
    def test_context_managed_on_exception(self, observe_mock, time_mock):
        # NOTE: need three time values here because it appears that time.time is called
        #  somewhere else in the call path, likely within a dependency.
        time_mock.side_effect = [123456, 123460, 123462]

        t = metrics.Timer(
            observer=metrics.plugin_latency,
            labels=('a', 'b', 'c', 'd'),
        )

        with pytest.raises(ValueError):
            with t:
                raise ValueError()

        assert t._start_time == 123456
        observe_mock.assert_called_once_with(4)
