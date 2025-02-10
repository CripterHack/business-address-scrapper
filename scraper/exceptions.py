"""Excepciones personalizadas para el scraper."""

from typing import Optional, Dict, Any
import json
from datetime import datetime


class ScraperBaseException(Exception):
    """Base exception for all scraper errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging/serialization."""
        return {
            "type": self.__class__.__name__,
            "message": str(self),
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }

    def __str__(self) -> str:
        base = super().__str__()
        if self.details:
            return f"{base} - Details: {json.dumps(self.details, indent=2)}"
        return base


# Errores de Configuración
class ConfigurationError(ScraperBaseException):
    """Error en la configuración del scraper."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


# Errores de Validación
class ValidationError(ScraperBaseException):
    """Error de validación de datos."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class AddressValidationError(ValidationError):
    """Error en la validación de direcciones."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


# Errores de Red
class NetworkError(ScraperBaseException):
    """Error relacionado con la red."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class ProxyError(NetworkError):
    """Error en conexiones proxy."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class RateLimitError(NetworkError):
    """Error por límite de tasa alcanzado."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class APIError(NetworkError):
    """Error en llamadas a APIs."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


# Errores de Datos
class DataError(ScraperBaseException):
    """Error base para problemas con datos."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class DataProcessingError(DataError):
    """Error procesando datos."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class DataExtractionError(DataError):
    """Error extrayendo datos."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class AddressExtractionError(DataExtractionError):
    """Error extrayendo direcciones."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class ParseError(DataError):
    """Error parseando datos."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class SearchError(DataError):
    """Error en la búsqueda de información."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


# Errores de Almacenamiento
class StorageError(ScraperBaseException):
    """Error base para almacenamiento."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class DatabaseError(StorageError):
    """Error en operaciones de base de datos."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class CacheError(StorageError):
    """Error en operaciones de caché."""

    def __init__(
        self, message: str, node_id: Optional[str] = None, operation: Optional[str] = None
    ):
        details = {"node_id": node_id, "operation": operation}
        super().__init__(message, details=details)


class CacheConnectionError(CacheError):
    """Error conectando a nodos de caché."""

    def __init__(self, message: str, node_id: Optional[str] = None):
        super().__init__(message, node_id=node_id, operation="connect")


class CacheAuthenticationError(CacheError):
    """Error de autenticación en caché."""

    def __init__(self, message: str, node_id: Optional[str] = None):
        super().__init__(message, node_id=node_id, operation="authenticate")


class CacheEncryptionError(CacheError):
    """Error de encriptación en caché."""

    def __init__(self, message: str, node_id: Optional[str] = None):
        super().__init__(message, node_id=node_id, operation="encrypt")


class CacheConsistencyError(CacheError):
    """Error de consistencia en caché."""

    def __init__(self, message: str, node_id: Optional[str] = None):
        super().__init__(message, node_id=node_id, operation="validate")


class CacheLockError(CacheError):
    """Error en bloqueos de caché."""

    def __init__(self, message: str, node_id: Optional[str] = None):
        super().__init__(message, node_id=node_id, operation="lock")


class CacheCompressionError(CacheError):
    """Error en compresión de caché."""

    def __init__(
        self, message: str, algorithm: Optional[str] = None, data_size: Optional[int] = None
    ):
        node_id = f"compression_{algorithm}_{data_size}"
        super().__init__(message, node_id=node_id, operation="compress")


class CacheQuotaError(CacheError):
    """Error por cuota excedida en caché."""

    def __init__(self, message: str, node_id: Optional[str] = None):
        super().__init__(message, node_id=node_id, operation="quota_check")


class CacheTimeoutError(CacheError):
    """Error por timeout en caché."""

    def __init__(self, message: str, node_id: Optional[str] = None):
        super().__init__(message, node_id=node_id, operation="timeout")


class BackupError(StorageError):
    """Error en operaciones de backup."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class MigrationError(StorageError):
    """Error en operaciones de migración."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


# Errores de Modelos
class ModelError(ScraperBaseException):
    """Error en modelos de ML."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class LLaMAError(ModelError):
    """Error específico del modelo LLaMA."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)


class CaptchaError(ModelError):
    """Error en resolución de captchas."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details=details)
