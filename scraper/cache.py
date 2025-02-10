"""Sistema de caché para el scraper."""

import logging
import json
from typing import Any, Dict, Optional, Union, List
from datetime import datetime, timedelta
from pathlib import Path
import pickle
import hashlib
from abc import ABC, abstractmethod

from .exceptions import CacheError
from .metrics import MetricsManager
from .cache.cleaner import CacheCleaner


class CacheBackend(ABC):
    """Interfaz abstracta para backends de caché."""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Obtiene un valor de la caché."""
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Guarda un valor en la caché."""
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Elimina un valor de la caché."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Limpia toda la caché."""
        pass

    @abstractmethod
    def get_size(self) -> int:
        """Obtiene el tamaño actual de la caché en bytes."""
        pass

    @abstractmethod
    def get_keys(self) -> List[str]:
        """Obtiene todas las claves en la caché."""
        pass


class MemoryCache(CacheBackend):
    """Implementación de caché en memoria."""

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            return None

        entry = self._cache[key]
        if self._is_expired(entry):
            self.delete(key)
            return None

        return entry["value"]

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        self._cache[key] = {"value": value, "timestamp": datetime.now(), "ttl": ttl}

    def delete(self, key: str) -> None:
        self._cache.pop(key, None)

    def clear(self) -> None:
        self._cache.clear()

    def get_size(self) -> int:
        return sum(len(pickle.dumps(entry)) for entry in self._cache.values())

    def get_keys(self) -> List[str]:
        return list(self._cache.keys())

    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        if entry["ttl"] is None:
            return False
        expiry = entry["timestamp"] + timedelta(seconds=entry["ttl"])
        return datetime.now() > expiry


class FileCache(CacheBackend):
    """Implementación de caché en archivo."""

    def __init__(self, cache_dir: str = ".cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, key: str) -> Optional[Any]:
        try:
            file_path = self._get_file_path(key)
            if not file_path.exists():
                return None

            with open(file_path, "rb") as f:
                entry = pickle.load(f)

            if self._is_expired(entry):
                self.delete(key)
                return None

            return entry["value"]
        except Exception as e:
            logging.warning(f"Error reading from file cache: {str(e)}")
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        try:
            file_path = self._get_file_path(key)
            entry = {"value": value, "timestamp": datetime.now(), "ttl": ttl}
            with open(file_path, "wb") as f:
                pickle.dump(entry, f)
        except Exception as e:
            logging.error(f"Error writing to file cache: {str(e)}")

    def delete(self, key: str) -> None:
        try:
            file_path = self._get_file_path(key)
            if file_path.exists():
                file_path.unlink()
        except Exception as e:
            logging.warning(f"Error deleting from file cache: {str(e)}")

    def clear(self) -> None:
        try:
            for file_path in self.cache_dir.glob("*"):
                file_path.unlink()
        except Exception as e:
            logging.error(f"Error clearing file cache: {str(e)}")

    def get_size(self) -> int:
        total_size = 0
        for file_path in self.cache_dir.glob("*.cache"):
            try:
                total_size += file_path.stat().st_size
            except Exception:
                continue
        return total_size

    def get_keys(self) -> List[str]:
        return [self._get_key_from_path(p) for p in self.cache_dir.glob("*.cache")]

    def _get_file_path(self, key: str) -> Path:
        """Genera la ruta del archivo de caché."""
        hashed_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{hashed_key}.cache"

    def _get_key_from_path(self, path: Path) -> str:
        """Obtiene la clave original de la ruta del archivo."""
        return path.stem

    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        if entry["ttl"] is None:
            return False
        expiry = entry["timestamp"] + timedelta(seconds=entry["ttl"])
        return datetime.now() > expiry


class RedisCache(CacheBackend):
    """Implementación de caché en Redis."""

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        try:
            import redis

            self.client = redis.Redis(host=host, port=port, db=db)
            self.client.ping()  # Verificar conexión
        except ImportError:
            raise CacheError("Redis package not installed")
        except Exception as e:
            raise CacheError(f"Error connecting to Redis: {str(e)}")

    def get(self, key: str) -> Optional[Any]:
        try:
            value = self.client.get(key)
            if value is None:
                return None
            return pickle.loads(value)
        except Exception as e:
            logging.warning(f"Error reading from Redis cache: {str(e)}")
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        try:
            serialized_value = pickle.dumps(value)
            if ttl is not None:
                self.client.setex(key, ttl, serialized_value)
            else:
                self.client.set(key, serialized_value)
        except Exception as e:
            logging.error(f"Error writing to Redis cache: {str(e)}")

    def delete(self, key: str) -> None:
        try:
            self.client.delete(key)
        except Exception as e:
            logging.warning(f"Error deleting from Redis cache: {str(e)}")

    def clear(self) -> None:
        try:
            self.client.flushdb()
        except Exception as e:
            logging.error(f"Error clearing Redis cache: {str(e)}")

    def get_size(self) -> int:
        try:
            info = self.client.info(section="memory")
            return info["used_memory"]
        except Exception:
            return 0

    def get_keys(self) -> List[str]:
        try:
            return [k.decode() for k in self.client.keys("*")]
        except Exception:
            return []


class CacheManager:
    """Administrador de caché en memoria."""

    def __init__(self):
        """Inicializar el administrador de caché."""
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._hits = 0
        self._misses = 0

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
            logging.error(f"Error decoding JSON from cache: {str(e)}")
            return None

    def set_json(self, key: str, value: Dict[str, Any], ttl: int = 3600) -> None:
        """Serializa y guarda un valor JSON en la caché."""
        try:
            serialized = json.dumps(value)
            self.set(key, serialized, ttl)
        except (TypeError, ValueError) as e:
            logging.error(f"Error encoding JSON for cache: {str(e)}")

    def generate_key(self, *args: Any, **kwargs: Any) -> str:
        """Genera una clave de caché única basada en los argumentos."""
        key_parts = [str(arg) for arg in args]
        key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
