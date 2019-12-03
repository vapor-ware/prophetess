"""Prophetess entry point"""

import asyncio
import logging

from prophetess.exceptions import ProphetessException
from prophetess.pipeline import build_pipelines

log = logging.getLogger(__name__)


class Prophetess():

    def __init__(self, config):
        self.config = config
        self.pipelines = build_pipelines(self.config)

    async def close(self):
        await self.pipelines.close()

    async def run(self):
        for pipeline_name, pipeline in self.pipelines.items():
            log.info('Running Pipeline: {}'.format(pipeline_name))
            try:
                await pipeline.run()
            except ProphetessException as e:
                log.exception(e)
            log.info('Finished Pipeline: {}'.format(pipeline_name))

    async def start(self):
        log.info('Starting Control process')
        while True:
            await self.run()
            await asyncio.sleep(90)
