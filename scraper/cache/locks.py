"""Sistema de bloqueos distribuidos para la caché."""

import logging
import time
import threading
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import uuid

logger = logging.getLogger(__name__)

class DistributedLock:
    """Implementación de bloqueo distribuido."""
    
    def __init__(
        self,
        cache,  # DistributedCache
        lock_timeout: int = 30,
        retry_interval: float = 0.1,
        max_retries: int = 50
    ):
        """Inicializa el bloqueo distribuido.
        
        Args:
            cache: Instancia de caché distribuida
            lock_timeout: Tiempo de expiración del bloqueo en segundos
            retry_interval: Intervalo entre intentos de bloqueo
            max_retries: Número máximo de reintentos
        """
        self.cache = cache
        self.lock_timeout = lock_timeout
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        self.local_locks: Dict[str, threading.Lock] = {}
        self._local_lock = threading.Lock()
    
    def acquire(self, key: str, timeout: Optional[int] = None) -> bool:
        """Adquiere un bloqueo.
        
        Args:
            key: Clave a bloquear
            timeout: Tiempo máximo de espera
            
        Returns:
            bool: True si se adquirió el bloqueo
        """
        lock_key = f"lock:{key}"
        lock_id = str(uuid.uuid4())
        start_time = time.time()
        attempts = 0
        
        while attempts < self.max_retries:
            try:
                # Intentar adquirir bloqueo
                success = self.cache._set_in_node(
                    self.cache._get_primary_node(key),
                    lock_key,
                    lock_id,
                    nx=True,
                    ex=self.lock_timeout
                )
                
                if success:
                    # Registrar bloqueo local
                    with self._local_lock:
                        if key not in self.local_locks:
                            self.local_locks[key] = threading.Lock()
                        self.local_locks[key].acquire()
                    return True
                
                # Verificar timeout
                if timeout and time.time() - start_time > timeout:
                    return False
                
                time.sleep(self.retry_interval)
                attempts += 1
                
            except Exception as e:
                logger.error(f"Error acquiring lock for {key}: {str(e)}")
                return False
        
        return False
    
    def release(self, key: str) -> bool:
        """Libera un bloqueo.
        
        Args:
            key: Clave a desbloquear
            
        Returns:
            bool: True si se liberó el bloqueo
        """
        try:
            lock_key = f"lock:{key}"
            
            # Liberar bloqueo distribuido
            self.cache._delete_from_node(
                self.cache._get_primary_node(key),
                lock_key
            )
            
            # Liberar bloqueo local
            with self._local_lock:
                if key in self.local_locks:
                    self.local_locks[key].release()
                    del self.local_locks[key]
            
            return True
            
        except Exception as e:
            logger.error(f"Error releasing lock for {key}: {str(e)}")
            return False
    
    def is_locked(self, key: str) -> bool:
        """Verifica si una clave está bloqueada.
        
        Args:
            key: Clave a verificar
            
        Returns:
            bool: True si está bloqueada
        """
        try:
            lock_key = f"lock:{key}"
            return bool(self.cache._get_from_node(
                self.cache._get_primary_node(key),
                lock_key
            ))
        except Exception:
            return False
    
    def cleanup_expired_locks(self) -> None:
        """Limpia bloqueos expirados."""
        try:
            # Limpiar bloqueos locales expirados
            with self._local_lock:
                expired = []
                for key in self.local_locks:
                    if not self.is_locked(key):
                        expired.append(key)
                
                for key in expired:
                    self.local_locks[key].release()
                    del self.local_locks[key]
            
        except Exception as e:
            logger.error(f"Error cleaning up locks: {str(e)}")
    
    def __enter__(self):
        """Soporte para context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Limpieza al salir del context manager."""
        self.cleanup_expired_locks() 