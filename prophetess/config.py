import pkg_resources

PLUGINS = {ep.name: ep.load() for ep in pkg_resources.iter_entry_points('prophetess.plugins')}
