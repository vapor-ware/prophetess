import logging

from aiohttp import web
from prometheus_client import core
from prometheus_client.exposition import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)


class MetricsView(web.View):
    async def get(self) -> web.Response:
        resp = web.Response(body=generate_latest(core.REGISTRY))
        resp.content_type = CONTENT_TYPE_LATEST
        return resp
