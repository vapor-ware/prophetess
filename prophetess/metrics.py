
import time

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


class Timer():
    def __init__(self, *, observer, labels):
        self.histogram = observer
        self._start_time = None
        self.labels = labels

    def start(self):
        self._start_time = time.time()

    def stop(self):
        latency = time.time() - self._start_time
        self.histogram.labels(*self.labels).observe(latency)
