
import logging

from prophetess import config

log = logging.getLogger(__name__)


def build_plugin(plugin_type, plugin_id, plugin_config):
    plugin_name = plugin_config.get('plugin')
    module_name = plugin_name.lower()

    if module_name not in config.PLUGINS:
        raise InvalidPlugin(f'{plugin_name} not found, try `pip install prophetess-{module_name}`?')

    name = '{}{}'.format(plugin_name, plugin_type)
    plugin = getattr(config.PLUGINS.get(module_name), name)

    return plugin(id=plugin_id, config=plugin_config.get('config'))
