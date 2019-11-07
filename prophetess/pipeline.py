
import logging
import collections

from prophetess.plugin import Transformer
from prophetess.utils import build_plugin

log = logging.getLogger(__name__)


def build_pipelines(cfg):
    pipelines = Pipelines()
    extractors = cfg.get('extractors')
    loaders = cfg.get('loaders')
    for name, data in cfg.get('pipelines', {}).items():
        pipelines.append(Pipeline(
            id=name,
            extractors=[build_plugin('Extractor', e, extractors[e]) for e in data.get('extractors', [])],
            transform=Transformer(id='YAMLTransformer', config=data.get('transform')),
            loaders=[build_plugin('Loader', e, loaders[e]) for e in data.get('loaders', [])]
        ))

    return pipelines


class Pipelines(collections.OrderedDict):
    def __init__(self):
        pass

    async def close(self):
        for p in self.values():
            await p.close()

    def append(self, pipeline):
        self[pipeline.id] = pipeline

    def __str__(self):
        return '{}([{}])'.format(type(self).__name__, ', '.join([str(p) for p in self.values()]))


class Pipeline(object):

    @classmethod
    def from_config(cls, data):
        pass

    def __init__(self, *, id, extractors, transform, loaders):
        self.id = id
        self.extractors = extractors
        self.transform = transform
        self.loaders = loaders

    async def close(self):
        for e in self.extractors + [self.transform] + self.loaders:
            await e.close()

    async def run(self):
        for e in self.extractors:
            log.debug('Running Extractor {}'.format(e))

            async for record in e.run():
                log.debug('{} produced {}'.format(e, record))
                payload = await self.process(record)
                await self.load(payload)

    async def process(self, record):
        try:
            payload = await self.transform.run(record)
            log.debug('{} produced {}'.format(self.transform, payload))
        except KeyError:
            raise

        return payload

    async def load(self, record):
        for l in self.loaders:
            log.debug('Running Loader: {}'.format(l))

            try:
                await l.run(record)
            except:
                raise

    def __str__(self):
        return '{}({})'.format(type(self).__name__, self.id)
