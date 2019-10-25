
from prophetess.exceptions import ServiceError


class NetboxPluginException(ServiceError):
    """Raised when Netbox plugin encounters errors."""
    pass


class InvalidPKConfig(NetboxPluginException):
    """Raised when Netbox PK configuration yields invalid results."""
    pass

class InvalidNetboxEndpoint(NetboxPluginException):
    """Raised when invalid netbox endpoint is provided."""
    pass

class InvalidNetboxOperation(NetboxPluginException):
    """Raised when invalid netbox model or method is provided."""
    pass
