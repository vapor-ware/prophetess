
import time
from typing import Collection

from prometheus_client import Histogram

pipeline_latency = Histogram(
    name='prophetess_pipeline_exec_time',
    documentation='The time it takes for a prophetess pipeline to complete',
    labelnames=('id',),
)

plugin_latency = Histogram(
    name='prophetess_plugin_exec_time',
    documentation='The time it takes for a prophetess plugin to complete',
    labelnames=('id', 'plugin', 'type', 'class'),
)


class Timer:

    def __init__(self, *, observer: Histogram, labels: Collection[str]) -> None:
        self.histogram = observer
        self._start_time = None
        self.labels = labels

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self) -> None:
        self._start_time = time.time()

    def stop(self) -> None:
        latency = time.time() - self._start_time
        self.histogram.labels(*self.labels).observe(latency)
