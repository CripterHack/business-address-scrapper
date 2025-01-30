"""Cache management module."""

import json
import logging
import os
import pickle
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Union

import redis
from diskcache import Cache as DiskCache

from .exceptions import ConfigurationError, CacheError
from .metrics import MetricsManager

logger = logging.getLogger(__name__)

class CacheBackend(ABC):
    """Abstract base class for cache backends."""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with optional TTL in seconds."""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete value from cache."""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all values from cache."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close cache connection."""
        pass

class RedisCache(CacheBackend):
    """Redis cache backend."""
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        socket_timeout: int = 5,
        retry_on_timeout: bool = True,
        max_retries: int = 3
    ):
        """Initialize Redis connection with retry logic."""
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            socket_timeout=socket_timeout,
            retry_on_timeout=retry_on_timeout
        )
        self.max_retries = max_retries
        
        # Test connection
        self._test_connection()

    def _test_connection(self):
        """Test Redis connection with retries."""
        for attempt in range(self.max_retries):
            try:
                self.client.ping()
                return
            except redis.ConnectionError as e:
                if attempt == self.max_retries - 1:
                    raise ConfigurationError(
                        "Failed to connect to Redis",
                        details={'error': str(e), 'attempts': attempt + 1}
                    )
                time.sleep(1)

    def _retry_operation(self, operation):
        """Retry Redis operation with exponential backoff."""
        for attempt in range(self.max_retries):
            try:
                return operation()
            except redis.RedisError as e:
                if attempt == self.max_retries - 1:
                    raise CacheError(
                        "Redis operation failed",
                        details={'error': str(e), 'attempts': attempt + 1}
                    )
                time.sleep(2 ** attempt)

    def get(self, key: str) -> Optional[Any]:
        """Get value from Redis with retry logic."""
        return self._retry_operation(lambda: self._get(key))

    def _get(self, key: str) -> Optional[Any]:
        """Internal get implementation."""
        value = self.client.get(key)
        if value is None:
            return None
        try:
            return pickle.loads(value)
        except pickle.UnpicklingError as e:
            logger.error(f"Failed to unpickle value for key {key}: {e}")
            self.delete(key)
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in Redis with retry logic."""
        self._retry_operation(lambda: self._set(key, value, ttl))

    def _set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Internal set implementation."""
        try:
            pickled_value = pickle.dumps(value)
            if ttl:
                self.client.setex(key, ttl, pickled_value)
            else:
                self.client.set(key, pickled_value)
        except pickle.PicklingError as e:
            raise CacheError(
                "Failed to pickle value",
                details={'error': str(e), 'key': key}
            )

    def delete(self, key: str) -> None:
        """Delete value from Redis with retry logic."""
        self._retry_operation(lambda: self.client.delete(key))

    def clear(self) -> None:
        """Clear all values from Redis with retry logic."""
        self._retry_operation(lambda: self.client.flushdb())

    def close(self) -> None:
        """Close Redis connection."""
        try:
            self.client.close()
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

class MemoryCache(CacheBackend):
    """In-memory cache backend with size limit and periodic cleanup."""
    
    def __init__(self, max_size: int = 1000, cleanup_interval: int = 300):
        """Initialize in-memory cache with size limit."""
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        self.cleanup_interval = cleanup_interval
        self.lock = threading.Lock()
        
        # Start cleanup thread
        self._start_cleanup_thread()

    def _start_cleanup_thread(self):
        """Start periodic cleanup thread."""
        def cleanup_task():
            while True:
                time.sleep(self.cleanup_interval)
                self._cleanup_expired()
        
        self.cleanup_thread = threading.Thread(
            target=cleanup_task,
            daemon=True
        )
        self.cleanup_thread.start()

    def _cleanup_expired(self):
        """Remove expired entries."""
        with self.lock:
            now = datetime.now()
            expired_keys = [
                key for key, data in self.cache.items()
                if 'expires_at' in data and now > data['expires_at']
            ]
            for key in expired_keys:
                del self.cache[key]

    def _enforce_size_limit(self):
        """Remove oldest entries if cache exceeds size limit."""
        with self.lock:
            if len(self.cache) > self.max_size:
                # Sort by creation time and remove oldest entries
                sorted_items = sorted(
                    self.cache.items(),
                    key=lambda x: x[1]['created_at']
                )
                excess = len(self.cache) - self.max_size
                for key, _ in sorted_items[:excess]:
                    del self.cache[key]

    def get(self, key: str) -> Optional[Any]:
        """Get value from memory with expiration check."""
        with self.lock:
            if key not in self.cache:
                return None
                
            data = self.cache[key]
            now = datetime.now()
            
            # Check TTL
            if 'expires_at' in data and now > data['expires_at']:
                del self.cache[key]
                return None
                
            # Update access time
            data['last_accessed'] = now
            return data['value']

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in memory with size limit enforcement."""
        with self.lock:
            now = datetime.now()
            data = {
                'value': value,
                'created_at': now,
                'last_accessed': now
            }
            
            if ttl:
                data['expires_at'] = now + timedelta(seconds=ttl)
            
            self.cache[key] = data
            self._enforce_size_limit()

    def delete(self, key: str) -> None:
        """Delete value from memory."""
        with self.lock:
            self.cache.pop(key, None)

    def clear(self) -> None:
        """Clear all values from memory cache."""
        with self.lock:
            self.cache.clear()

    def close(self) -> None:
        """No-op for memory cache."""
        pass

class FileSystemCache(CacheBackend):
    """File system cache backend with error handling."""
    
    def __init__(self, cache_dir: Union[str, Path], cleanup_interval: int = 3600):
        """Initialize file system cache."""
        self.cache_dir = Path(cache_dir)
        self.cleanup_interval = cleanup_interval
        self.last_cleanup = 0
        
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise ConfigurationError(
                "Failed to create cache directory",
                details={'error': str(e), 'path': str(cache_dir)}
            )

    def _cleanup_if_needed(self):
        """Perform cleanup if interval has passed."""
        now = time.time()
        if now - self.last_cleanup > self.cleanup_interval:
            self._cleanup_expired()
            self.last_cleanup = now

    def _cleanup_expired(self):
        """Remove expired cache files."""
        try:
            for path in self.cache_dir.glob('*.cache'):
                try:
                    with open(path, 'rb') as f:
                        data = pickle.load(f)
                    
                    if 'expires_at' in data and datetime.now() > data['expires_at']:
                        path.unlink()
                except (OSError, pickle.UnpicklingError) as e:
                    logger.error(f"Error cleaning up cache file {path}: {e}")
                    try:
                        path.unlink()
                    except OSError:
                        pass
        except Exception as e:
            logger.error(f"Error during cache cleanup: {e}")

    def _get_path(self, key: str) -> Path:
        """Get file path for cache key."""
        # Use hash of key to avoid file system issues
        return self.cache_dir / f"{hash(key)}.cache"

    def get(self, key: str) -> Optional[Any]:
        """Get value from file system with error handling."""
        self._cleanup_if_needed()
        path = self._get_path(key)
        
        if not path.exists():
            return None
            
        try:
            with open(path, 'rb') as f:
                data = pickle.load(f)
                
            # Check TTL
            if 'expires_at' in data and datetime.now() > data['expires_at']:
                self.delete(key)
                return None
                
            return data['value']
            
        except (OSError, pickle.UnpicklingError) as e:
            logger.error(f"Error reading cache file: {e}")
            try:
                path.unlink()
            except OSError:
                pass
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in file system with error handling."""
        path = self._get_path(key)
        
        data = {
            'value': value,
            'created_at': datetime.now()
        }
        
        if ttl:
            data['expires_at'] = datetime.now() + timedelta(seconds=ttl)
        
        try:
            with open(path, 'wb') as f:
                pickle.dump(data, f)
        except (OSError, pickle.PicklingError) as e:
            raise CacheError(
                "Failed to write cache file",
                details={'error': str(e), 'path': str(path)}
            )

    def delete(self, key: str) -> None:
        """Delete value from file system with error handling."""
        path = self._get_path(key)
        try:
            if path.exists():
                path.unlink()
        except OSError as e:
            logger.error(f"Error deleting cache file: {e}")

    def clear(self) -> None:
        """Clear all values from file system cache with error handling."""
        try:
            for path in self.cache_dir.glob('*.cache'):
                try:
                    path.unlink()
                except OSError as e:
                    logger.error(f"Error deleting cache file {path}: {e}")
        except Exception as e:
            logger.error(f"Error clearing cache directory: {e}")

    def close(self) -> None:
        """No-op for file system cache."""
        pass

class DiskCacheBackend(CacheBackend):
    """Disk cache backend using diskcache library."""
    
    def __init__(self, cache_dir: Union[str, Path]):
        """Initialize disk cache."""
        self.cache = DiskCache(str(cache_dir))

    def get(self, key: str) -> Optional[Any]:
        """Get value from disk cache."""
        return self.cache.get(key)

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in disk cache."""
        self.cache.set(key, value, expire=ttl)

    def delete(self, key: str) -> None:
        """Delete value from disk cache."""
        self.cache.delete(key)

    def clear(self) -> None:
        """Clear all values from disk cache."""
        self.cache.clear()

    def close(self) -> None:
        """Close disk cache."""
        self.cache.close()

class CacheManager:
    """Cache manager with metrics tracking."""
    
    def __init__(
        self,
        backend: str = 'memory',
        cache_dir: Optional[Union[str, Path]] = None,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        metrics_manager: Optional[MetricsManager] = None
    ):
        """Initialize cache manager."""
        self.metrics = metrics_manager
        
        if backend == 'redis':
            self.backend = RedisCache(
                host=redis_host,
                port=redis_port,
                db=redis_db
            )
        elif backend == 'filesystem':
            if not cache_dir:
                raise ConfigurationError("cache_dir required for filesystem cache")
            self.backend = FileSystemCache(cache_dir)
        elif backend == 'diskcache':
            if not cache_dir:
                raise ConfigurationError("cache_dir required for disk cache")
            self.backend = DiskCacheBackend(cache_dir)
        elif backend == 'memory':
            self.backend = MemoryCache()
        else:
            raise ConfigurationError(f"Unknown cache backend: {backend}")

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache with metrics tracking."""
        value = self.backend.get(key)
        
        if self.metrics:
            if value is None:
                self.metrics.record_cache_miss()
            else:
                self.metrics.record_cache_hit()
        
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        self.backend.set(key, value, ttl)

    def delete(self, key: str) -> None:
        """Delete value from cache."""
        self.backend.delete(key)

    def clear(self) -> None:
        """Clear all values from cache."""
        self.backend.clear()

    def close(self) -> None:
        """Close cache connection."""
        self.backend.close()

    def __enter__(self):
        """Context manager enter."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close() 