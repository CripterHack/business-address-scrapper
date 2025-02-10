"""Sistema de priorización de claves para la caché distribuida."""

import logging
import time
from typing import Dict, Any, Optional, Set, List
from datetime import datetime, timedelta
import threading
import heapq

from .events import EventPriority

logger = logging.getLogger(__name__)

class PriorityItem:
    """Item con prioridad para la cola."""
    
    def __init__(
        self,
        key: str,
        priority: EventPriority,
        timestamp: datetime,
        ttl: Optional[int] = None
    ):
        """Inicializa el item.
        
        Args:
            key: Clave del item
            priority: Nivel de prioridad
            timestamp: Timestamp de creación
            ttl: Tiempo de vida en segundos
        """
        self.key = key
        self.priority = priority
        self.timestamp = timestamp
        self.ttl = ttl
        self.expiry = timestamp + timedelta(seconds=ttl) if ttl else None
    
    def __lt__(self, other: 'PriorityItem') -> bool:
        """Comparación para ordenamiento."""
        if self.priority != other.priority:
            return self.priority.value > other.priority.value
        return self.timestamp < other.timestamp

class PriorityManager:
    """Gestor de prioridades de caché."""
    
    def __init__(
        self,
        max_high_priority: int = 1000,
        max_medium_priority: int = 5000,
        cleanup_interval: int = 300  # 5 minutos
    ):
        """Inicializa el gestor de prioridades.
        
        Args:
            max_high_priority: Máximo de items de alta prioridad
            max_medium_priority: Máximo de items de prioridad media
            cleanup_interval: Intervalo de limpieza en segundos
        """
        self.max_high_priority = max_high_priority
        self.max_medium_priority = max_medium_priority
        self.cleanup_interval = cleanup_interval
        
        self.items: Dict[str, PriorityItem] = {}
        self.priority_queues: Dict[EventPriority, List[PriorityItem]] = {
            EventPriority.HIGH: [],
            EventPriority.MEDIUM: [],
            EventPriority.LOW: []
        }
        
        self._lock = threading.Lock()
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop)
        self._cleanup_thread.daemon = True
        self._running = False
    
    def start(self) -> None:
        """Inicia el proceso de limpieza."""
        self._running = True
        self._cleanup_thread.start()
        logger.info("Priority manager started")
    
    def stop(self) -> None:
        """Detiene el proceso de limpieza."""
        self._running = False
        self._cleanup_thread.join()
        logger.info("Priority manager stopped")
    
    def add_key(
        self,
        key: str,
        priority: EventPriority = EventPriority.LOW,
        ttl: Optional[int] = None
    ) -> None:
        """Añade una clave con prioridad.
        
        Args:
            key: Clave a añadir
            priority: Nivel de prioridad
            ttl: Tiempo de vida en segundos
        """
        with self._lock:
            # Remover si ya existe
            if key in self.items:
                self._remove_key(key)
            
            # Verificar límites
            if priority == EventPriority.HIGH:
                if len(self.priority_queues[EventPriority.HIGH]) >= self.max_high_priority:
                    self._demote_oldest(EventPriority.HIGH)
            elif priority == EventPriority.MEDIUM:
                if len(self.priority_queues[EventPriority.MEDIUM]) >= self.max_medium_priority:
                    self._demote_oldest(EventPriority.MEDIUM)
            
            # Crear nuevo item
            item = PriorityItem(
                key=key,
                priority=priority,
                timestamp=datetime.now(),
                ttl=ttl
            )
            
            # Añadir a estructuras
            self.items[key] = item
            heapq.heappush(self.priority_queues[priority], item)
    
    def get_priority(self, key: str) -> Optional[EventPriority]:
        """Obtiene la prioridad de una clave.
        
        Args:
            key: Clave a consultar
            
        Returns:
            Optional[EventPriority]: Prioridad de la clave
        """
        with self._lock:
            item = self.items.get(key)
            return item.priority if item else None
    
    def update_priority(
        self,
        key: str,
        new_priority: EventPriority
    ) -> bool:
        """Actualiza la prioridad de una clave.
        
        Args:
            key: Clave a actualizar
            new_priority: Nueva prioridad
            
        Returns:
            bool: True si se actualizó correctamente
        """
        with self._lock:
            if key not in self.items:
                return False
            
            # Obtener item actual
            item = self.items[key]
            old_priority = item.priority
            
            if old_priority == new_priority:
                return True
            
            # Verificar límites
            if new_priority == EventPriority.HIGH:
                if len(self.priority_queues[EventPriority.HIGH]) >= self.max_high_priority:
                    self._demote_oldest(EventPriority.HIGH)
            elif new_priority == EventPriority.MEDIUM:
                if len(self.priority_queues[EventPriority.MEDIUM]) >= self.max_medium_priority:
                    self._demote_oldest(EventPriority.MEDIUM)
            
            # Actualizar prioridad
            self._remove_key(key)
            item.priority = new_priority
            heapq.heappush(self.priority_queues[new_priority], item)
            
            return True
    
    def remove_key(self, key: str) -> bool:
        """Elimina una clave.
        
        Args:
            key: Clave a eliminar
            
        Returns:
            bool: True si se eliminó correctamente
        """
        with self._lock:
            return self._remove_key(key)
    
    def _remove_key(self, key: str) -> bool:
        """Elimina una clave (sin lock).
        
        Args:
            key: Clave a eliminar
            
        Returns:
            bool: True si se eliminó correctamente
        """
        if key not in self.items:
            return False
            
        item = self.items[key]
        self.priority_queues[item.priority].remove(item)
        heapq.heapify(self.priority_queues[item.priority])
        del self.items[key]
        
        return True
    
    def _demote_oldest(self, priority: EventPriority) -> None:
        """Degrada el item más antiguo de una prioridad.
        
        Args:
            priority: Prioridad a degradar
        """
        if not self.priority_queues[priority]:
            return
            
        item = heapq.heappop(self.priority_queues[priority])
        
        if priority == EventPriority.HIGH:
            item.priority = EventPriority.MEDIUM
            if len(self.priority_queues[EventPriority.MEDIUM]) >= self.max_medium_priority:
                self._demote_oldest(EventPriority.MEDIUM)
            heapq.heappush(self.priority_queues[EventPriority.MEDIUM], item)
        else:  # MEDIUM
            item.priority = EventPriority.LOW
            heapq.heappush(self.priority_queues[EventPriority.LOW], item)
    
    def _cleanup_loop(self) -> None:
        """Loop de limpieza de items expirados."""
        while self._running:
            try:
                self._cleanup_expired()
                time.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"Error in cleanup loop: {str(e)}")
                time.sleep(60)
    
    def _cleanup_expired(self) -> None:
        """Limpia items expirados."""
        now = datetime.now()
        expired_keys = set()
        
        with self._lock:
            # Identificar claves expiradas
            for key, item in self.items.items():
                if item.expiry and now >= item.expiry:
                    expired_keys.add(key)
            
            # Eliminar claves expiradas
            for key in expired_keys:
                self._remove_key(key)
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired items")
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del gestor.
        
        Returns:
            Dict[str, Any]: Estadísticas
        """
        with self._lock:
            return {
                'total_items': len(self.items),
                'high_priority': len(self.priority_queues[EventPriority.HIGH]),
                'medium_priority': len(self.priority_queues[EventPriority.MEDIUM]),
                'low_priority': len(self.priority_queues[EventPriority.LOW])
            }