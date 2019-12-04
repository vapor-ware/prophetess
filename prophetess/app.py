"""Prophetess entry point"""

import asyncio
import logging
from typing import Dict

from prophetess.exceptions import ProphetessException
from prophetess.pipeline import build_pipelines

log = logging.getLogger(__name__)


class Prophetess():

    def __init__(self, config: Dict) -> None:
        self.config = config
        self.pipelines = build_pipelines(self.config)

    async def close(self) -> None:
        await self.pipelines.close()

    async def run(self) -> None:
        for pipeline_name, pipeline in self.pipelines.items():
            log.info('Running Pipeline: {}'.format(pipeline_name))
            try:
                await pipeline.run()
            except ProphetessException as e:
                log.exception(e)
            log.info('Finished Pipeline: {}'.format(pipeline_name))

    async def start(self) -> None:
        log.info('Starting Control process')
        while True:
            await self.run()
            await asyncio.sleep(90)
