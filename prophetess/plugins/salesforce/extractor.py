
from prophetess.plugin import Extractor
from prophetess.plugins.salesforce.client import SFClient
from prophetess.plugins.salesforce.exceptions import SalesforceAPIException


class SalesforceExtractor(Extractor):
    required_config = (
        'client_id',
        'user',
        'instance',
        'key',
        'query',
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.sf = SFClient(
            client_id=self.config.get('client_id'),
            user=self.config.get('user'),
            key=self.config.get('key'),
            instance=self.config.get('instance')
        )

    async def run(self):

        try:
            await self.sf.authenticate()
            data = await self.sf.run_query(self.config.get('query'))
        except SalesforceAPIException:
            raise

        for d in data:
            yield d
