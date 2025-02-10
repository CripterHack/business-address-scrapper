"""Módulo para el manejo de eventos del sistema de caché distribuida."""

import logging
import threading
from typing import Dict, Any, List, Callable, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import queue
from enum import Enum, auto
import time

logger = logging.getLogger(__name__)

class EventPriority(Enum):
    """Prioridades de eventos."""
    HIGH = auto()
    MEDIUM = auto()
    LOW = auto()

class EventType(Enum):
    """Tipos de eventos del sistema."""
    # Eventos críticos (HIGH priority)
    ERROR = auto()
    NODE_DOWN = auto()
    RECOVERY_FAILED = auto()
    MIGRATION_FAILED = auto()
    REBALANCE_FAILED = auto()
    
    # Eventos de operación (MEDIUM priority)
    WARNING = auto()
    MIGRATION_START = auto()
    MIGRATION_COMPLETE = auto()
    REBALANCE_START = auto()
    REBALANCE_COMPLETE = auto()
    RECOVERY_START = auto()
    RECOVERY_COMPLETE = auto()
    BACKUP = auto()
    RESTORE = auto()
    THRESHOLD_EXCEEDED = auto()
    CLEANUP = auto()
    
    # Eventos informativos (LOW priority)
    INFO = auto()
    GET = auto()
    SET = auto()
    DELETE = auto()

@dataclass
class CacheEvent:
    """Evento de caché."""
    type: EventType
    key: Optional[str]
    value: Optional[Any]
    node_id: str
    timestamp: datetime
    metadata: Dict[str, Any]
    priority: EventPriority = EventPriority.LOW

    @classmethod
    def create(
        cls,
        event_type: EventType,
        key: Optional[str] = None,
        value: Optional[Any] = None,
        node_id: str = '',
        metadata: Optional[Dict[str, Any]] = None,
        priority: Optional[EventPriority] = None
    ) -> 'CacheEvent':
        """Crea un nuevo evento con prioridad automática.
        
        Args:
            event_type: Tipo de evento
            key: Clave afectada
            value: Valor asociado
            node_id: ID del nodo
            metadata: Metadatos adicionales
            priority: Prioridad opcional
            
        Returns:
            CacheEvent: Nuevo evento
        """
        if priority is None:
            # Asignar prioridad según tipo de evento
            if event_type in {
                EventType.ERROR,
                EventType.NODE_DOWN,
                EventType.RECOVERY_FAILED,
                EventType.MIGRATION_FAILED,
                EventType.REBALANCE_FAILED
            }:
                priority = EventPriority.HIGH
            elif event_type in {
                EventType.WARNING,
                EventType.MIGRATION_START,
                EventType.MIGRATION_COMPLETE,
                EventType.REBALANCE_START,
                EventType.REBALANCE_COMPLETE,
                EventType.RECOVERY_START,
                EventType.RECOVERY_COMPLETE,
                EventType.BACKUP,
                EventType.RESTORE,
                EventType.THRESHOLD_EXCEEDED
            }:
                priority = EventPriority.MEDIUM
            else:
                priority = EventPriority.LOW
        
        return cls(
            type=event_type,
            key=key,
            value=value,
            node_id=node_id,
            timestamp=datetime.now(),
            metadata=metadata or {},
            priority=priority
        )

@dataclass
class EventPattern:
    """Patrón de eventos compuestos."""
    events: List[EventType]
    window: timedelta
    order_matters: bool = True
    metadata_match: Optional[Dict[str, Any]] = None

@dataclass
class EventWindow:
    """Ventana de eventos para patrones."""
    pattern: EventPattern
    events: List[CacheEvent] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    completed: bool = False

@dataclass
class EventMetrics:
    """Métricas de eventos."""
    total_events: int = 0
    events_by_type: Dict[EventType, int] = field(default_factory=dict)
    events_by_priority: Dict[EventPriority, int] = field(default_factory=dict)
    failed_events: int = 0
    avg_processing_time: float = 0.0
    total_processing_time: float = 0.0
    last_event_time: Optional[datetime] = None
    pattern_matches: int = 0
    pattern_failures: int = 0

class EventManager:
    """Gestor de eventos de caché."""
    
    def __init__(
        self,
        max_queue_size: int = 1000,
        worker_count: int = 2,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        metrics_interval: int = 60  # Intervalo de métricas en segundos
    ):
        """Inicializa el gestor de eventos.
        
        Args:
            max_queue_size: Tamaño máximo de la cola de eventos
            worker_count: Número de workers para procesar eventos
            max_retries: Número máximo de reintentos para eventos fallidos
            retry_delay: Tiempo entre reintentos en segundos
            metrics_interval: Intervalo de recolección de métricas
        """
        # Colas separadas por prioridad
        self.high_priority_queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=max_queue_size)
        self.medium_priority_queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=max_queue_size)
        self.low_priority_queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=max_queue_size)
        self.retry_queue: queue.Queue = queue.Queue()
        
        self.subscribers: Dict[EventType, List[Callable]] = {}
        self.worker_count = worker_count
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.workers: List[threading.Thread] = []
        self.running = False
        self._lock = threading.Lock()
        self._retry_counts: Dict[str, int] = {}
        
        # Nuevas estructuras para eventos compuestos
        self.patterns: Dict[str, EventPattern] = {}
        self.active_windows: Dict[str, List[EventWindow]] = {}
        self._pattern_lock = threading.Lock()
        
        # Métricas
        self.metrics = EventMetrics()
        self.metrics_interval = metrics_interval
        self._last_metrics_time = time.time()
        
        # Callbacks para patrones
        self.pattern_callbacks: Dict[str, List[Callable[[List[CacheEvent]], None]]] = {}
    
    def start(self) -> None:
        """Inicia el procesamiento de eventos."""
        if self.running:
            return
            
        self.running = True
        
        # Workers para cada cola de prioridad
        for _ in range(self.worker_count):
            high_worker = threading.Thread(target=self._process_high_priority)
            medium_worker = threading.Thread(target=self._process_medium_priority)
            low_worker = threading.Thread(target=self._process_low_priority)
            
            high_worker.daemon = True
            medium_worker.daemon = True
            low_worker.daemon = True
            
            high_worker.start()
            medium_worker.start()
            low_worker.start()
            
            self.workers.extend([high_worker, medium_worker, low_worker])
        
        # Worker para reintentos
        retry_worker = threading.Thread(target=self._process_retries)
        retry_worker.daemon = True
        retry_worker.start()
        self.workers.append(retry_worker)
        
        logger.info("Event manager started")
    
    def stop(self) -> None:
        """Detiene el procesamiento de eventos."""
        self.running = False
        
        # Esperar a que terminen los workers
        for worker in self.workers:
            worker.join()
        
        self.workers.clear()
        logger.info("Event manager stopped")
    
    def subscribe(
        self,
        event_type: EventType,
        callback: Callable[[CacheEvent], None]
    ) -> None:
        """Suscribe un callback a un tipo de evento.
        
        Args:
            event_type: Tipo de evento
            callback: Función a llamar
        """
        with self._lock:
            if event_type not in self.subscribers:
                self.subscribers[event_type] = []
            self.subscribers[event_type].append(callback)
    
    def unsubscribe(
        self,
        event_type: EventType,
        callback: Callable[[CacheEvent], None]
    ) -> None:
        """Desuscribe un callback.
        
        Args:
            event_type: Tipo de evento
            callback: Función a remover
        """
        with self._lock:
            if event_type in self.subscribers:
                self.subscribers[event_type].remove(callback)
                if not self.subscribers[event_type]:
                    del self.subscribers[event_type]
    
    def publish(
        self,
        event_type: EventType,
        key: Optional[str] = None,
        value: Optional[Any] = None,
        node_id: str = '',
        metadata: Optional[Dict[str, Any]] = None,
        priority: Optional[EventPriority] = None
    ) -> None:
        """Publica un evento.
        
        Args:
            event_type: Tipo de evento
            key: Clave afectada
            value: Valor asociado
            node_id: ID del nodo
            metadata: Metadatos adicionales
            priority: Prioridad opcional
        """
        try:
            event = CacheEvent.create(
                event_type=event_type,
                key=key,
                value=value,
                node_id=node_id,
                metadata=metadata,
                priority=priority
            )
            
            # Encolar según prioridad
            if event.priority == EventPriority.HIGH:
                queue_to_use = self.high_priority_queue
            elif event.priority == EventPriority.MEDIUM:
                queue_to_use = self.medium_priority_queue
            else:
                queue_to_use = self.low_priority_queue
            
            try:
                queue_to_use.put_nowait(event)
            except queue.Full:
                if event.priority == EventPriority.HIGH:
                    # Para eventos de alta prioridad, intentar en cola media
                    try:
                        self.medium_priority_queue.put_nowait(event)
                    except queue.Full:
                        self.retry_queue.put(event)
                else:
                    self.retry_queue.put(event)
            
        except Exception as e:
            logger.error(f"Error publishing event: {str(e)}")
            self._handle_error(event_type, str(e), metadata)
    
    def _process_high_priority(self) -> None:
        """Procesa eventos de alta prioridad."""
        while self.running:
            try:
                event = self.high_priority_queue.get(timeout=1)
                self._process_with_metrics(
                    event,
                    lambda e: self._notify_subscribers(e)
                )
                self.high_priority_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing high priority event: {str(e)}")
    
    def _process_medium_priority(self) -> None:
        """Procesa eventos de prioridad media."""
        while self.running:
            try:
                event = self.medium_priority_queue.get(timeout=1)
                self._process_with_metrics(
                    event,
                    lambda e: self._notify_subscribers(e)
                )
                self.medium_priority_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing medium priority event: {str(e)}")
    
    def _process_low_priority(self) -> None:
        """Procesa eventos de baja prioridad."""
        while self.running:
            try:
                event = self.low_priority_queue.get(timeout=1)
                self._process_with_metrics(
                    event,
                    lambda e: self._notify_subscribers(e)
                )
                self.low_priority_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing low priority event: {str(e)}")
    
    def _process_retries(self) -> None:
        """Procesa eventos en la cola de reintentos."""
        while self.running:
            try:
                event = self.retry_queue.get(timeout=1)
                event_id = f"{event.type}:{event.key or ''}"
                
                if self._retry_counts.get(event_id, 0) < self.max_retries:
                    time.sleep(self.retry_delay)
                    
                    # Reintentar en la cola correspondiente
                    if event.priority == EventPriority.HIGH:
                        self.high_priority_queue.put(event)
                    elif event.priority == EventPriority.MEDIUM:
                        self.medium_priority_queue.put(event)
                    else:
                        self.low_priority_queue.put(event)
                    
                    self._retry_counts[event_id] = self._retry_counts.get(event_id, 0) + 1
                else:
                    logger.error(f"Max retries exceeded for event: {event_id}")
                    self._handle_error(event.type, "Max retries exceeded", event.metadata)
                
                self.retry_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing retry: {str(e)}")
    
    def _notify_subscribers(self, event: CacheEvent) -> bool:
        """Notifica a los suscriptores de un evento.
        
        Args:
            event: Evento a notificar
            
        Returns:
            bool: True si todos los callbacks fueron exitosos
        """
        with self._lock:
            subscribers = self.subscribers.get(event.type, []).copy()
        
        success = True
        for callback in subscribers:
            try:
                callback(event)
            except Exception as e:
                logger.error(
                    f"Error in event callback for {event.type}: {str(e)}"
                )
                success = False
                self.metrics.failed_events += 1
        
        return success
    
    def _handle_failed_event(self, event: CacheEvent) -> None:
        """Maneja un evento fallido.
        
        Args:
            event: Evento fallido
        """
        try:
            self.retry_queue.put(event)
            self.metrics.failed_events += 1
        except queue.Full:
            logger.error("Retry queue is full, event will be lost")
            self._handle_error(event.type, "Retry queue full", event.metadata)
    
    def _handle_error(
        self,
        event_type: EventType,
        error: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Maneja un error publicando un evento de error.
        
        Args:
            event_type: Tipo de evento original
            error: Mensaje de error
            metadata: Metadatos adicionales
        """
        error_metadata = {
            'original_event_type': event_type.value,
            'error_message': error,
            **(metadata or {})
        }
        
        try:
            error_event = CacheEvent.create(
                event_type=EventType.ERROR,
                metadata=error_metadata,
                priority=EventPriority.HIGH
            )
            self._process_with_metrics(
                error_event,
                lambda e: self._notify_subscribers(e)
            )
        except Exception as e:
            logger.error(f"Error publishing error event: {str(e)}")
            self.metrics.failed_events += 1
    
    def get_queue_size(self) -> Dict[str, int]:
        """Obtiene el tamaño actual de las colas.
        
        Returns:
            Dict[str, int]: Tamaño de cada cola
        """
        return {
            'high_priority': self.high_priority_queue.qsize(),
            'medium_priority': self.medium_priority_queue.qsize(),
            'low_priority': self.low_priority_queue.qsize(),
            'retry': self.retry_queue.qsize()
        }
    
    def get_total_queue_size(self) -> int:
        """Obtiene el tamaño total de todas las colas.
        
        Returns:
            int: Suma de los tamaños de todas las colas
        """
        sizes = self.get_queue_size()
        return sum(sizes.values())
    
    def clear_queues(self) -> None:
        """Limpia todas las colas de eventos."""
        with self._lock:
            while not self.high_priority_queue.empty():
                self.high_priority_queue.get_nowait()
            while not self.medium_priority_queue.empty():
                self.medium_priority_queue.get_nowait()
            while not self.low_priority_queue.empty():
                self.low_priority_queue.get_nowait()
            while not self.retry_queue.empty():
                self.retry_queue.get_nowait()
            
            self._retry_counts.clear()
    
    def clear_patterns(self) -> None:
        """Limpia todos los patrones y ventanas activas."""
        with self._pattern_lock:
            self.patterns.clear()
            self.active_windows.clear()
            self.pattern_callbacks.clear()
    
    def reset_metrics(self) -> None:
        """Reinicia todas las métricas."""
        self.metrics = EventMetrics()
        self._last_metrics_time = time.time()
    
    def get_retry_queue_size(self) -> int:
        """Obtiene el tamaño de la cola de reintentos.
        
        Returns:
            int: Tamaño de la cola de reintentos
        """
        return self.retry_queue.qsize()
    
    def get_subscriber_count(self, event_type: Optional[EventType] = None) -> int:
        """Obtiene el número de suscriptores.
        
        Args:
            event_type: Tipo de evento opcional
            
        Returns:
            int: Número de suscriptores
        """
        with self._lock:
            if event_type:
                return len(self.subscribers.get(event_type, []))
            return sum(len(subs) for subs in self.subscribers.values())
    
    def clear_subscribers(self, event_type: Optional[EventType] = None) -> None:
        """Limpia suscriptores.
        
        Args:
            event_type: Tipo de evento opcional
        """
        with self._lock:
            if event_type:
                self.subscribers.pop(event_type, None)
            else:
                self.subscribers.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del gestor.
        
        Returns:
            Dict[str, Any]: Estadísticas
        """
        return {
            'queue_size': self.get_queue_size(),
            'retry_queue_size': self.get_retry_queue_size(),
            'subscriber_count': self.get_subscriber_count(),
            'worker_count': len(self.workers),
            'is_running': self.running,
            'event_types': [et.value for et in self.subscribers.keys()],
            'retry_counts': self._retry_counts.copy()
        }
    
    def add_pattern(
        self,
        name: str,
        events: List[EventType],
        window: timedelta,
        callback: Callable[[List[CacheEvent]], None],
        order_matters: bool = True,
        metadata_match: Optional[Dict[str, Any]] = None
    ) -> None:
        """Añade un patrón de eventos compuestos.
        
        Args:
            name: Nombre del patrón
            events: Lista de tipos de eventos
            window: Ventana de tiempo para el patrón
            callback: Función a llamar cuando se detecta el patrón
            order_matters: Si el orden de los eventos importa
            metadata_match: Diccionario de metadatos que deben coincidir
        """
        with self._pattern_lock:
            pattern = EventPattern(
                events=events,
                window=window,
                order_matters=order_matters,
                metadata_match=metadata_match
            )
            self.patterns[name] = pattern
            
            if name not in self.pattern_callbacks:
                self.pattern_callbacks[name] = []
            self.pattern_callbacks[name].append(callback)
            
            # Inicializar ventana activa
            if name not in self.active_windows:
                self.active_windows[name] = []
    
    def remove_pattern(self, name: str) -> None:
        """Elimina un patrón de eventos.
        
        Args:
            name: Nombre del patrón
        """
        with self._pattern_lock:
            self.patterns.pop(name, None)
            self.pattern_callbacks.pop(name, None)
            self.active_windows.pop(name, None)
    
    def _check_patterns(self, event: CacheEvent) -> None:
        """Verifica patrones para un evento.
        
        Args:
            event: Evento a verificar
        """
        with self._pattern_lock:
            now = datetime.now()
            
            # Limpiar ventanas expiradas
            for pattern_name, windows in self.active_windows.items():
                pattern = self.patterns[pattern_name]
                self.active_windows[pattern_name] = [
                    w for w in windows
                    if now - w.start_time <= pattern.window and not w.completed
                ]
                
                # Crear nueva ventana si no hay ninguna activa
                if not self.active_windows[pattern_name]:
                    self.active_windows[pattern_name].append(
                        EventWindow(pattern=pattern)
                    )
            
            # Verificar cada patrón
            for pattern_name, pattern in self.patterns.items():
                for window in self.active_windows[pattern_name]:
                    if self._check_event_match(event, pattern, window):
                        window.events.append(event)
                        
                        # Verificar si el patrón está completo
                        if self._is_pattern_complete(window):
                            self._handle_pattern_match(pattern_name, window)
                            window.completed = True
                            
                            # Crear nueva ventana
                            self.active_windows[pattern_name].append(
                                EventWindow(pattern=pattern)
                            )
    
    def _check_event_match(
        self,
        event: CacheEvent,
        pattern: EventPattern,
        window: EventWindow
    ) -> bool:
        """Verifica si un evento coincide con un patrón.
        
        Args:
            event: Evento a verificar
            pattern: Patrón a comparar
            window: Ventana actual
            
        Returns:
            bool: True si el evento coincide
        """
        # Verificar tipo de evento
        if event.type not in pattern.events:
            return False
        
        # Verificar orden si es necesario
        if pattern.order_matters:
            expected_index = len(window.events)
            if expected_index >= len(pattern.events):
                return False
            if pattern.events[expected_index] != event.type:
                return False
        
        # Verificar metadatos si es necesario
        if pattern.metadata_match:
            for key, value in pattern.metadata_match.items():
                if event.metadata.get(key) != value:
                    return False
        
        return True
    
    def _is_pattern_complete(self, window: EventWindow) -> bool:
        """Verifica si un patrón está completo.
        
        Args:
            window: Ventana a verificar
            
        Returns:
            bool: True si el patrón está completo
        """
        if len(window.events) != len(window.pattern.events):
            return False
            
        if window.pattern.order_matters:
            return all(
                a.type == b for a, b in zip(window.events, window.pattern.events)
            )
        else:
            return set(e.type for e in window.events) == set(window.pattern.events)
    
    def _handle_pattern_match(self, pattern_name: str, window: EventWindow) -> None:
        """Maneja una coincidencia de patrón.
        
        Args:
            pattern_name: Nombre del patrón
            window: Ventana completada
        """
        try:
            callbacks = self.pattern_callbacks.get(pattern_name, [])
            for callback in callbacks:
                try:
                    callback(window.events)
                except Exception as e:
                    logger.error(
                        f"Error in pattern callback for {pattern_name}: {str(e)}"
                    )
            
            self.metrics.pattern_matches += 1
            
        except Exception as e:
            logger.error(f"Error handling pattern match: {str(e)}")
            self.metrics.pattern_failures += 1
    
    def _update_metrics(self, event: CacheEvent, processing_time: float) -> None:
        """Actualiza métricas para un evento.
        
        Args:
            event: Evento procesado
            processing_time: Tiempo de procesamiento en segundos
        """
        self.metrics.total_events += 1
        self.metrics.events_by_type[event.type] = (
            self.metrics.events_by_type.get(event.type, 0) + 1
        )
        self.metrics.events_by_priority[event.priority] = (
            self.metrics.events_by_priority.get(event.priority, 0) + 1
        )
        
        # Actualizar tiempo promedio de procesamiento
        total_time = (
            self.metrics.avg_processing_time * (self.metrics.total_events - 1) +
            processing_time
        )
        self.metrics.avg_processing_time = total_time / self.metrics.total_events
        self.metrics.total_processing_time += processing_time
        self.metrics.last_event_time = datetime.now()
    
    def _process_with_metrics(
        self,
        event: CacheEvent,
        process_func: Callable
    ) -> None:
        """Procesa un evento con métricas.
        
        Args:
            event: Evento a procesar
            process_func: Función de procesamiento
        """
        start_time = time.time()
        try:
            process_func(event)
            self._update_metrics(event, time.time() - start_time)
            self._check_patterns(event)
        except Exception as e:
            self.metrics.failed_events += 1
            raise e
    
    def get_metrics(self) -> Dict[str, Any]:
        """Obtiene métricas del gestor.
        
        Returns:
            Dict[str, Any]: Métricas actualizadas
        """
        return {
            'high_priority_queue_size': self.high_priority_queue.qsize(),
            'medium_priority_queue_size': self.medium_priority_queue.qsize(),
            'low_priority_queue_size': self.low_priority_queue.qsize(),
            'retry_queue_size': self.retry_queue.qsize(),
            'subscriber_count': self.get_subscriber_count(),
            'worker_count': len(self.workers),
            'is_running': self.running,
            'event_types': [et.value for et in self.subscribers.keys()],
            'retry_counts': self._retry_counts.copy(),
            'metrics': {
                'total_events': self.metrics.total_events,
                'events_by_type': {
                    k.value: v for k, v in self.metrics.events_by_type.items()
                },
                'events_by_priority': {
                    k.value: v for k, v in self.metrics.events_by_priority.items()
                },
                'failed_events': self.metrics.failed_events,
                'avg_processing_time': self.metrics.avg_processing_time,
                'total_processing_time': self.metrics.total_processing_time,
                'last_event_time': (
                    self.metrics.last_event_time.isoformat()
                    if self.metrics.last_event_time else None
                ),
                'pattern_matches': self.metrics.pattern_matches,
                'pattern_failures': self.metrics.pattern_failures
            }
        } 