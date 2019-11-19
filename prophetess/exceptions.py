
class ProphetessException(Exception):
    """Base exception for all of Prophetess"""
    pass


class ServiceError(ProphetessException):
    """Base exception for external services"""
    pass


class InvalidConfigurationException(ProphetessException):
    """Configuration for Prophetess or a plugin is invalid"""
    pass


class InvalidPlugin(ProphetessException):
    """A requested plugin does not exist"""
    pass
