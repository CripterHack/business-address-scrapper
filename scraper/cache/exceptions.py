"""Cache specific exceptions."""

class CacheError(Exception):
    """Base exception for cache errors."""
    pass

class CacheConnectionError(CacheError):
    """Error connecting to cache node."""
    pass

class CacheAuthenticationError(CacheError):
    """Error during authentication."""
    pass

class CacheEncryptionError(CacheError):
    """Error during encryption/decryption."""
    pass

class CacheConsistencyError(CacheError):
    """Error when consistency requirements are not met."""
    pass

class CacheLockError(CacheError):
    """Error acquiring or releasing locks."""
    pass

class CacheCompressionError(CacheError):
    """Error during compression/decompression."""
    pass

class CacheQuotaError(CacheError):
    """Error when cache quota is exceeded."""
    pass

class CacheTimeoutError(CacheError):
    """Error when operation times out."""
    pass 