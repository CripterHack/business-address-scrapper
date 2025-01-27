class ScraperException(Exception):
    """Base exception for scraper errors"""
    pass

class ConfigurationError(ScraperException):
    """Raised when there's an error in the configuration"""
    pass

class APIError(ScraperException):
    """Raised when there's an error with API calls"""
    pass

class ValidationError(ScraperException):
    """Raised when data validation fails"""
    pass

class DataProcessingError(ScraperException):
    """Raised when there's an error processing data"""
    pass

class ProxyError(ScraperException):
    """Raised when there's an error with proxy handling"""
    pass

class ModelError(ScraperException):
    """Raised when there's an error with ML models"""
    pass

class AddressValidationError(ValidationError):
    """Raised when address validation fails"""
    pass

class DataExtractionError(ScraperException):
    """Raised when data extraction fails"""
    pass

class StorageError(ScraperException):
    """Raised when there's an error storing data"""
    pass 