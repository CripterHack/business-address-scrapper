"""Administrador de caché."""

import logging
import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from ..exceptions import CacheError
from ..metrics import MetricsManager


class CacheManager:
    """Administrador de caché en memoria."""

    def __init__(self):
        """Inicializar el administrador de caché."""
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._hits = 0
        self._misses = 0
        self.logger = logging.getLogger(__name__)

    def get(self, key: str) -> Optional[Any]:
        """Obtener un valor del caché."""
        if key in self._cache:
            entry = self._cache[key]
            if not self._is_expired(entry):
                self._hits += 1
                return entry["value"]
            else:
                del self._cache[key]
        self._misses += 1
        return None

    def set(self, key: str, value: Any, ttl: int = 3600) -> None:
        """Establecer un valor en el caché."""
        self._cache[key] = {"value": value, "expires_at": datetime.now() + timedelta(seconds=ttl)}

    def delete(self, key: str) -> None:
        """Eliminar un valor del caché."""
        if key in self._cache:
            del self._cache[key]

    def cleanup(self, max_size: int = 1000, threshold: int = 800) -> None:
        """Limpiar entradas expiradas y mantener el tamaño del caché."""
        # Eliminar entradas expiradas
        now = datetime.now()
        expired_keys = [key for key, entry in self._cache.items() if self._is_expired(entry, now)]
        for key in expired_keys:
            del self._cache[key]

        # Si aún excede el umbral, eliminar las entradas más antiguas
        if len(self._cache) > threshold:
            sorted_entries = sorted(self._cache.items(), key=lambda x: x[1]["expires_at"])
            entries_to_remove = len(self._cache) - max_size
            if entries_to_remove > 0:
                for key, _ in sorted_entries[:entries_to_remove]:
                    del self._cache[key]

    def clear(self) -> None:
        """Limpiar todo el caché."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def get_stats(self) -> Dict[str, int]:
        """Obtener estadísticas del caché."""
        return {"size": len(self._cache), "hits": self._hits, "misses": self._misses}

    def _is_expired(self, entry: Dict[str, Any], now: Optional[datetime] = None) -> bool:
        """Verificar si una entrada ha expirado."""
        if now is None:
            now = datetime.now()
        return entry["expires_at"] < now

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Obtiene y deserializa un valor JSON de la caché."""
        try:
            value = self.get(key)
            if value is not None:
                return json.loads(value)
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON from cache: {str(e)}")
            return None

    def set_json(self, key: str, value: Dict[str, Any], ttl: int = 3600) -> None:
        """Serializa y guarda un valor JSON en la caché."""
        try:
            serialized = json.dumps(value)
            self.set(key, serialized, ttl)
        except (TypeError, ValueError) as e:
            self.logger.error(f"Error encoding JSON for cache: {str(e)}")
