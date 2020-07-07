
import collections
import logging
from typing import Any, Dict, List, Tuple, Union

from prophetess.exceptions import ProphetessException
from prophetess.metrics import Timer, pipeline_latency
from prophetess.plugin import Extractor, Loader, Transformer
from prophetess.utils import build_plugin

log = logging.getLogger(__name__)


def build_pipelines(cfg: Dict[str, Any]) -> 'Pipelines':
    pipelines = Pipelines()
    extractors = cfg.get('extractors')
    loaders = cfg.get('loaders')
    transformers = cfg.get('transformers', {})

    for name, data in cfg.get('pipelines', {}).items():
        transform = data.get('transform')

        if isinstance(transform, str):
            transformer = build_plugin('Transformer', transform, transformers.get(transform))
        elif isinstance(transform, collections.Mapping):
            transformer = Transformer(id='YAMLTransformer', config=transform)
        else:
            log.error('Invalid pipeline configuration for {}, bad transform'.format(name))
            continue

        pipelines.append(Pipeline(
            id=name,
            extractors=[build_plugin('Extractor', e, extractors[e])
                        for e in data.get('extractors', [])],
            transform=transformer,
            loaders=[build_plugin('Loader', e, loaders[e]) for e in data.get('loaders', [])]
        ))

    return pipelines


class Pipelines(collections.OrderedDict):

    async def close(self) -> None:
        for p in self.values():
            await p.close()

    def append(self, pipeline: 'Pipeline') -> None:
        self[pipeline.id] = pipeline

    def __str__(self) -> str:
        return '{}([{}])'.format(type(self).__name__, ', '.join([str(p) for p in self.values()]))


class Pipeline:

    def __init__(
            self, *,
            id: str,
            extractors: Union[List[Extractor], Tuple[Extractor]],
            transform: Transformer,
            loaders: Union[List[Loader], Tuple[Loader]],
    ) -> None:
        self.id = id
        self.extractors = extractors
        self.transform = transform
        self.loaders = loaders
        self.timer = Timer(observer=pipeline_latency, labels=(self.id,))

    async def close(self) -> None:
        for e in self.extractors + [self.transform] + self.loaders:
            await e.close()

    async def run(self) -> None:
        with self.timer:
            for e in self.extractors:
                log.debug('Running Extractor {}'.format(e))
                e.timer.start()
                async for record in e.run():
                    e.timer.stop()
                    log.debug('{} produced {}'.format(e, record))
                    await self.process(record)

    async def process(self, record: Dict[str, Any]) -> None:
        with self.transform.timer:
            try:
                async for payload in self.transform.run(record):
                    log.debug('{} produced {}'.format(self.transform, payload))
                    await self.load(payload)
            except KeyError:
                raise

    async def load(self, record: Dict[str, Any]) -> None:
        if not record:
            return

        for loader in self.loaders:
            log.debug('Running Loader: {}'.format(loader))

            with loader.timer:
                try:
                    await loader.run(record)
                except ProphetessException as e:
                    log.warning('{} Loader failed: {}'.format(loader.id, e))
                except Exception as e:
                    log.error('{} raised unexpected exception: {}'.format(loader.id, e))

    def __str__(self) -> str:
        return '{}({})'.format(type(self).__name__, self.id)
