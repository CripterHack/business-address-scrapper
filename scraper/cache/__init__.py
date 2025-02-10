"""Módulo de caché del scraper."""

from .compression import CompressionManager, CompressionStats
from .manager import CacheManager

__all__ = ["CompressionManager", "CompressionStats", "CacheManager"]
