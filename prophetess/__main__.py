#!/usr/bin/env python

import sys
import yaml
import asyncio
import logging

from prophetess.app import Prophetess
from prophetess.config import DEBUG, CONFIG_FILE


log = logging.getLogger('prophetess')

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
)


if DEBUG:
    logging.getLogger('prophetess').setLevel(logging.DEBUG)

with open(CONFIG_FILE) as f:
    cfg = yaml.safe_load(f.read())

loop = asyncio.get_event_loop()
log.info('Starting Prophetess')

mage = Prophetess(cfg)
asyncio.ensure_future(mage.start())

try:
    loop.run_forever()
except KeyboardInterrupt:
    log.warn('Control loop interrupted via Keyboard')
finally:
    log.info('Cleaning pipelines')
    loop.run_until_complete(mage.close())

log.info('Prophetess stopped')
