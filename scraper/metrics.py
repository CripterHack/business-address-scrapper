"""Sistema de métricas para el scraper."""

import logging
import psutil
from typing import Dict, Any
from datetime import datetime


class MetricsManager:
    """Administrador de métricas del sistema."""

    def __init__(self):
        """Inicializar el administrador de métricas."""
        self.logger = logging.getLogger(__name__)
        self._cache_hits = 0
        self._cache_misses = 0
        self._errors: Dict[str, int] = {}
        self._start_time = datetime.now()

    def record_cache_hit(self) -> None:
        """Registrar un acierto en caché."""
        self._cache_hits += 1

    def record_cache_miss(self) -> None:
        """Registrar un fallo en caché."""
        self._cache_misses += 1

    def record_error(self, error_type: str) -> None:
        """Registrar un error."""
        self._errors[error_type] = self._errors.get(error_type, 0) + 1

    def record_cache_cleanup(self, keys_removed: int, bytes_freed: int) -> None:
        """Registrar una limpieza de caché."""
        self.logger.info(f"Cache cleanup: removed {keys_removed} keys, freed {bytes_freed} bytes")

    def get_report(self) -> Any:
        """Obtener reporte de métricas."""
        return type(
            "MetricsReport",
            (),
            {
                "performance": {
                    "cpu_percent": psutil.cpu_percent(),
                    "memory_mb": psutil.Process().memory_info().rss / 1024 / 1024,
                    "uptime_seconds": (datetime.now() - self._start_time).total_seconds(),
                },
                "cache": {
                    "hits": self._cache_hits,
                    "misses": self._cache_misses,
                    "hit_ratio": (
                        self._cache_hits / (self._cache_hits + self._cache_misses)
                        if (self._cache_hits + self._cache_misses) > 0
                        else 0
                    ),
                },
                "database": {"connections": 0},  # Placeholder para futuras métricas de DB
                "errors": self._errors,
            },
        )
