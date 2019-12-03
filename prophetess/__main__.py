#!/usr/bin/env python

import asyncio
import logging
import sys

import yaml
from aiohttp import web

from prophetess.app import Prophetess
from prophetess.config import CONFIG_FILE, DEBUG, PORT
from prophetess.web import MetricsView

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

# Some real raw AIOHtpp
app = web.Application(logger=logging.getLogger('prophetess.web'))

app.add_routes([
    web.view('/metrics', MetricsView),
])

handler = app.make_handler()
srv = loop.run_until_complete(loop.create_server(handler, '0.0.0.0', '8080'))
log.info(f'Metric server running on 127.0.0.1:{PORT}')

mage = Prophetess(cfg)
asyncio.ensure_future(mage.start())

try:
    loop.run_forever()
except KeyboardInterrupt:
    log.warn('Control loop interrupted via Keyboard')
finally:
    log.info('Cleaning pipelines')
    loop.run_until_complete(mage.close())
    log.info('Shutting down')
    srv.close()
    loop.run_until_complete(srv.wait_closed())
    loop.run_until_complete(app.shutdown())

log.info('Prophetess stopped')
