
from prophetess.exceptions import ServiceError


class SalesforcePluginException(ServiceError):
    """Raised when Salesforce plugin encounters errors."""
    pass


class SalesforceAPIException(SalesforcePluginException):
    """Raised when Salesforce API operations fail."""
    pass
