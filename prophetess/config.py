
import os
import pkg_resources

PLUGINS = {ep.name: ep.load() for ep in pkg_resources.iter_entry_points('prophetess.plugins')}
CONFIG_FILE = os.environ.get('PROPHETESS_CONFIG', '/etc/prophetess/pipeline.yaml')
DEBUG = os.environ.get('DEBUG', False)
PORT = os.environ.get('PORT', 8080)
