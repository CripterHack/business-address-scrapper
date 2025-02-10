"""Configuración del scraper."""

from typing import Dict, Any, Optional
import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from ..settings import (
    Settings,
    ScraperSettings,
    OutputSettings,
    LoggingSettings,
    DirectorySettings,
    SearchSettings,
    load_settings,
)
from .base import BaseSettings, DatabaseSettings
from .cache_config import (
    CacheConfig,
    NodeConfig,
    PartitioningConfig,
    CompressionConfig,
    SecurityConfig,
    MetricsConfig,
    load_config as load_cache_config,
    save_config as save_cache_config,
)


class ConfigurationError(Exception):
    """Error de configuración."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Inicializar error."""
        super().__init__(message)
        self.details = details or {}


"""Módulo de configuración centralizada."""

__all__ = [
    # Base Configuration
    "BaseSettings",
    "DatabaseSettings",
    # Cache Configuration
    "CacheConfig",
    "NodeConfig",
    "PartitioningConfig",
    "CompressionConfig",
    "SecurityConfig",
    "MetricsConfig",
    "load_cache_config",
    "save_cache_config",
    "validate_cache_config",
    # General Settings
    "Settings",
    "ScraperSettings",
    "OutputSettings",
    "LoggingSettings",
    "DirectorySettings",
    "SearchSettings",
    "load_settings",
]
