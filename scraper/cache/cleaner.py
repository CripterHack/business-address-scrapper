"""Sistema de limpieza automática para la caché distribuida."""

import logging
import time
import threading
from typing import Dict, Any, Optional, Set, List
from datetime import datetime, timedelta
from enum import Enum

from .events import EventManager, CacheEvent, EventType, EventPriority
from .priority import PriorityManager
from ..metrics import MetricsManager

logger = logging.getLogger(__name__)

class CleanupStrategy(Enum):
    """Estrategias de limpieza."""
    LRU = 'lru'  # Least Recently Used
    LFU = 'lfu'  # Least Frequently Used
    TTL = 'ttl'  # Time To Live
    PRIORITY = 'priority'  # Based on Priority

class CacheCleaner:
    """Limpiador de caché."""
    
    def __init__(
        self,
        cache,  # DistributedCache
        event_manager: Optional[EventManager] = None,
        priority_manager: Optional[PriorityManager] = None,
        metrics: Optional[MetricsManager] = None,
        max_size_mb: int = 1000,  # 1GB
        max_items: int = 1000000,  # 1M items
        cleanup_interval: int = 300,  # 5 minutos
        strategy: CleanupStrategy = CleanupStrategy.LRU,
        min_free_space_mb: int = 100  # 100MB
    ):
        """Inicializa el limpiador.
        
        Args:
            cache: Instancia de caché distribuida
            event_manager: Gestor de eventos opcional
            priority_manager: Gestor de prioridades opcional
            metrics: Gestor de métricas opcional
            max_size_mb: Tamaño máximo en MB
            max_items: Número máximo de items
            cleanup_interval: Intervalo de limpieza en segundos
            strategy: Estrategia de limpieza
            min_free_space_mb: Espacio libre mínimo en MB
        """
        self.cache = cache
        self.event_manager = event_manager
        self.priority_manager = priority_manager
        self.metrics = metrics
        self.max_size_mb = max_size_mb
        self.max_items = max_items
        self.cleanup_interval = cleanup_interval
        self.strategy = strategy
        self.min_free_space_mb = min_free_space_mb
        
        # Estado interno
        self._running = False
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop)
        self._cleanup_thread.daemon = True
        self._last_cleanup = datetime.now()
        self._cleanup_stats: Dict[str, Any] = {
            'total_cleanups': 0,
            'total_items_removed': 0,
            'last_cleanup_time': None,
            'last_cleanup_duration': None,
            'errors': []
        }
    
    def start(self) -> None:
        """Inicia el limpiador."""
        if self._running:
            return
            
        self._running = True
        self._cleanup_thread.start()
        logger.info("Cache cleaner started")
        
        if self.event_manager:
            self.event_manager.publish(
                EventType.INFO,
                message="Cache cleaner started",
                metadata={
                    'strategy': self.strategy.value,
                    'max_size_mb': self.max_size_mb,
                    'max_items': self.max_items
                }
            )
    
    def stop(self) -> None:
        """Detiene el limpiador."""
        self._running = False
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join()
        logger.info("Cache cleaner stopped")
        
        if self.event_manager:
            self.event_manager.publish(
                EventType.INFO,
                message="Cache cleaner stopped",
                metadata=self._cleanup_stats
            )
    
    def _cleanup_loop(self) -> None:
        """Loop principal de limpieza."""
        while self._running:
            try:
                start_time = time.time()
                items_removed = self._perform_cleanup()
                duration = time.time() - start_time
                
                # Actualizar estadísticas
                self._cleanup_stats['total_cleanups'] += 1
                self._cleanup_stats['total_items_removed'] += items_removed
                self._cleanup_stats['last_cleanup_time'] = datetime.now().isoformat()
                self._cleanup_stats['last_cleanup_duration'] = duration
                
                # Registrar métricas
                if self.metrics:
                    self.metrics.record_cleanup(
                        items_removed=items_removed,
                        duration=duration,
                        strategy=self.strategy.value
                    )
                
                # Esperar hasta el próximo ciclo
                time.sleep(self.cleanup_interval)
                
            except Exception as e:
                error_msg = f"Error in cleanup loop: {str(e)}"
                logger.error(error_msg)
                
                self._cleanup_stats['errors'].append({
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e)
                })
                
                if self.event_manager:
                    self.event_manager.publish(
                        EventType.ERROR,
                        message=error_msg,
                        priority=EventPriority.HIGH
                    )
                
                time.sleep(60)  # Esperar antes de reintentar
    
    def _perform_cleanup(self) -> int:
        """
        Realiza la limpieza de la caché.
        
        Returns:
            int: Número de items eliminados
        """
        items_removed = 0
        
        try:
            # Obtener estado actual
            current_size = self.cache.get_size()
            current_items = len(self.cache.get_keys())
            
            # Verificar si es necesario limpiar
            if (current_size < self.max_size_mb * 1024 * 1024 and 
                current_items < self.max_items):
                return 0
            
            # Determinar cuánto espacio liberar
            target_size = self.max_size_mb * 0.7 * 1024 * 1024  # 70% del máximo
            bytes_to_free = current_size - target_size
            
            # Obtener claves a eliminar según estrategia
            keys_to_remove = self._get_keys_to_remove(bytes_to_free)
            
            # Eliminar claves
            for key in keys_to_remove:
                try:
                    size = len(str(self.cache.get(key)).encode())
                    self.cache.delete(key)
                    items_removed += 1
                    
                    if self.event_manager:
                        self.event_manager.publish(
                            EventType.CLEANUP,
                            key=key,
                            metadata={
                                'strategy': self.strategy.value,
                                'size': size,
                                'operation': 'cleanup'
                            }
                        )
                        
                except Exception as e:
                    logger.error(f"Error removing key {key}: {str(e)}")
            
            return items_removed
            
        except Exception as e:
            error_msg = f"Error during cleanup: {str(e)}"
            logger.error(error_msg)
            
            if self.event_manager:
                self.event_manager.publish(
                    EventType.ERROR,
                    message=error_msg,
                    priority=EventPriority.HIGH
                )
            
            return 0
    
    def _get_keys_to_remove(self, bytes_to_free: int) -> List[str]:
        """
        Obtiene claves a eliminar según la estrategia.
        
        Args:
            bytes_to_free: Bytes a liberar
            
        Returns:
            List[str]: Claves a eliminar
        """
        keys = self.cache.get_keys()
        
        if self.strategy == CleanupStrategy.LRU:
            # Ordenar por último acceso
            return sorted(
                keys,
                key=lambda k: self.cache.get_last_access(k)
            )
        elif self.strategy == CleanupStrategy.LFU:
            # Ordenar por frecuencia de acceso
            return sorted(
                keys,
                key=lambda k: self.cache.get_access_count(k)
            )
        elif self.strategy == CleanupStrategy.TTL:
            # Ordenar por tiempo restante de TTL
            return sorted(
                keys,
                key=lambda k: self.cache.get_ttl(k) or 0
            )
        elif self.strategy == CleanupStrategy.PRIORITY:
            if self.priority_manager:
                # Ordenar por prioridad
                return sorted(
                    keys,
                    key=lambda k: self.priority_manager.get_priority(k).value
                )
            else:
                logger.warning("Priority strategy selected but no manager available")
                return sorted(
                    keys,
                    key=lambda k: self.cache.get_last_access(k)
                )
        else:
            logger.error(f"Unknown cleanup strategy: {self.strategy}")
            return []
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del limpiador.
        
        Returns:
            Dict[str, Any]: Estadísticas
        """
        return {
            'strategy': self.strategy.value,
            'max_size_mb': self.max_size_mb,
            'max_items': self.max_items,
            'cleanup_interval': self.cleanup_interval,
            'is_running': self._running,
            'last_cleanup': self._last_cleanup.isoformat(),
            'cleanup_stats': self._cleanup_stats
        }
    
    def __enter__(self) -> 'CacheCleaner':
        """Soporte para context manager."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Limpieza al salir del context manager."""
        self.stop()
    
    def __del__(self) -> None:
        """Limpieza al destruir la instancia."""
        self.stop() 