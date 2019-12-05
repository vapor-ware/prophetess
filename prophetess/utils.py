
import logging
from typing import Any, Dict

from prophetess import config, plugin
from prophetess.exceptions import InvalidPlugin

log = logging.getLogger(__name__)


def build_plugin(plugin_type: str, plugin_id: str, plugin_config: Dict[str, Any]) -> plugin.PluginBase:
    plugin_name = plugin_config.get('plugin')
    module_name = plugin_name.lower()

    if module_name not in config.PLUGINS:
        raise InvalidPlugin(f'{plugin_name} not found, try `pip install prophetess-{module_name}`?')

    name = plugin_config.get('class', '{}{}'.format(plugin_name, plugin_type))
    plugin_class = getattr(config.PLUGINS.get(module_name), name)

    return plugin_class(
        id=plugin_id,
        config=plugin_config.get('config'),
        labels=(plugin_name, plugin_type, name),
    )
