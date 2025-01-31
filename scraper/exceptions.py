"""Custom exceptions for the scraper."""


class ScraperException(Exception):
    """Base exception for all scraper errors."""
    def __init__(self, message: str, code: int = None, details: dict = None):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(self.message)


class ConfigurationError(ScraperException):
    """Raised when there's an issue with configuration."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=1000, details=details)


class APIError(ScraperException):
    """Raised when there's an error with API calls"""
    pass


class ValidationError(ScraperException):
    """Raised when data validation fails."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=2000, details=details)


class DataProcessingError(ScraperException):
    """Raised when there's an error processing data"""
    pass


class ProxyError(ScraperException):
    """Raised when there's an issue with proxy connections."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=3000, details=details)


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


class CacheError(ScraperException):
    """Raised when there's an error with cache operations."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=5100, details=details)


class CaptchaError(ScraperException):
    """Raised when there's an issue with captcha solving."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=6100, details=details)


class LLaMAError(ScraperException):
    """Raised when there's an issue with the LLaMA model."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=6200, details=details)


class DatabaseError(ScraperException):
    """Raised when there's an issue with database operations."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=5000, details=details)


class NetworkError(ScraperException):
    """Raised when there's a network-related issue."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=3100, details=details)


class RateLimitError(ScraperException):
    """Raised when rate limits are exceeded."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=3200, details=details)


class ParseError(ScraperException):
    """Raised when there's an issue parsing data."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=4000, details=details) 