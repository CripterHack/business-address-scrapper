"""Sistema de alertas para la caché distribuida."""

import logging
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timedelta
import json
from pathlib import Path
import threading
import time

from .cache.events import EventManager, EventType, EventPriority, CacheEvent
from .metrics import MetricsManager

logger = logging.getLogger(__name__)

def get_event_priority(severity: str) -> EventPriority:
    """Determina la prioridad del evento basado en la severidad.
    
    Args:
        severity: Nivel de severidad ('error', 'warning', 'info')
        
    Returns:
        EventPriority: Prioridad correspondiente
    """
    severity = severity.lower()
    if severity == 'error':
        return EventPriority.HIGH
    elif severity == 'warning':
        return EventPriority.MEDIUM
    else:
        return EventPriority.LOW

class AlertManager:
    """Gestor de alertas del sistema."""
    
    def __init__(
        self,
        event_manager: Optional[EventManager] = None,
        metrics: Optional[MetricsManager] = None,
        alert_threshold: int = 5,
        max_history_size: int = 1000,
        duplicate_window: int = 300,  # 5 minutos
        cleanup_interval: int = 3600  # 1 hora
    ):
        """Inicializa el gestor de alertas.
        
        Args:
            event_manager: Gestor de eventos opcional
            metrics: Gestor de métricas opcional
            alert_threshold: Umbral de alertas antes de notificar
            max_history_size: Tamaño máximo del historial
            duplicate_window: Ventana para detectar duplicados en segundos
            cleanup_interval: Intervalo de limpieza automática en segundos
        """
        self.event_manager = event_manager
        self.metrics = metrics
        self.alert_threshold = alert_threshold
        self.max_history_size = max_history_size
        self.duplicate_window = duplicate_window
        self.cleanup_interval = cleanup_interval
        
        self.alert_counts: Dict[str, int] = {}
        self.notified_alerts: Set[str] = set()
        self.alert_history: List[Dict[str, Any]] = []
        self.last_notification: Dict[str, datetime] = {}
        
        # Estadísticas
        self._stats = {
            'total_alerts': 0,
            'alerts_by_severity': {
                'error': 0,
                'warning': 0,
                'info': 0
            },
            'duplicates_prevented': 0,
            'cleanups_performed': 0,
            'last_cleanup': None,
            'errors': []
        }
        
        # Thread de limpieza
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop)
        self._cleanup_thread.daemon = True
        self._running = False
        
        if self.event_manager:
            # Suscribirse a eventos críticos
            self.event_manager.subscribe(EventType.ERROR, self._handle_error)
            self.event_manager.subscribe(EventType.NODE_DOWN, self._handle_error)
            self.event_manager.subscribe(EventType.RECOVERY_FAILED, self._handle_error)
            self.event_manager.subscribe(EventType.MIGRATION_FAILED, self._handle_error)
            self.event_manager.subscribe(EventType.REBALANCE_FAILED, self._handle_error)
            
            # Suscribirse a eventos de operación
            self.event_manager.subscribe(EventType.WARNING, self._handle_warning)
            self.event_manager.subscribe(EventType.THRESHOLD_EXCEEDED, self._handle_warning)
            
            # Suscribirse a eventos informativos
            self.event_manager.subscribe(EventType.INFO, self._handle_info)
    
    def start(self) -> None:
        """Inicia el gestor de alertas."""
        if self._running:
            return
            
        self._running = True
        self._cleanup_thread.start()
        logger.info("Alert manager started")
    
    def stop(self) -> None:
        """Detiene el gestor de alertas."""
        self._running = False
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join()
        logger.info("Alert manager stopped")
    
    def _cleanup_loop(self) -> None:
        """Loop de limpieza periódica."""
        while self._running:
            try:
                # Limpiar alertas antiguas
                self.clear_alerts(older_than=self.duplicate_window * 2)
                
                # Limpiar historial si excede el tamaño
                self._cleanup_history()
                
                # Esperar hasta el próximo ciclo
                time.sleep(self.cleanup_interval)
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {str(e)}")
                time.sleep(60)  # Esperar antes de reintentar
    
    def __enter__(self) -> 'AlertManager':
        """Soporte para context manager."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Limpieza al salir del context manager."""
        self.stop()
    
    def __del__(self) -> None:
        """Limpieza al destruir la instancia."""
        self.stop()
    
    def _handle_error(self, event: CacheEvent) -> None:
        """Procesa eventos de error."""
        self._process_alert(
            event.message or str(event.type),
            'error',
            EventPriority.HIGH,
            event.metadata
        )
    
    def _handle_warning(self, event: CacheEvent) -> None:
        """Procesa eventos de advertencia."""
        self._process_alert(
            event.message or str(event.type),
            'warning',
            EventPriority.MEDIUM,
            event.metadata
        )
    
    def _handle_info(self, event: CacheEvent) -> None:
        """Procesa eventos informativos."""
        self._process_alert(
            event.message or str(event.type),
            'info',
            EventPriority.LOW,
            event.metadata
        )
    
    def _is_duplicate(self, message: str, severity: str) -> bool:
        """
        Verifica si una alerta es duplicada dentro de la ventana de tiempo.
        
        Args:
            message: Mensaje de la alerta
            severity: Nivel de severidad
            
        Returns:
            bool: True si es duplicada
        """
        key = f"{severity}:{message}"
        now = datetime.now()
        
        if key in self.last_notification:
            time_diff = (now - self.last_notification[key]).total_seconds()
            return time_diff < self.duplicate_window
            
        return False
    
    def _cleanup_history(self) -> None:
        """Limpia el historial de alertas si excede el tamaño máximo."""
        if len(self.alert_history) > self.max_history_size:
            # Mantener las alertas más recientes
            self.alert_history = sorted(
                self.alert_history,
                key=lambda x: x['timestamp'],
                reverse=True
            )[:self.max_history_size]
            
            # Actualizar contadores
            self.alert_counts.clear()
            for alert in self.alert_history:
                msg = alert['message']
                self.alert_counts[msg] = self.alert_counts.get(msg, 0) + 1
            
            self._stats['cleanups_performed'] += 1
            self._stats['last_cleanup'] = datetime.now().isoformat()
    
    def _process_alert(
        self,
        message: str,
        severity: str,
        priority: EventPriority,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Procesa una alerta y notifica si es necesario.
        
        Args:
            message: Mensaje de la alerta
            severity: Nivel de severidad
            priority: Prioridad del evento
            metadata: Metadatos adicionales
        """
        # Verificar duplicados
        if self._is_duplicate(message, severity):
            self._stats['duplicates_prevented'] += 1
            return
            
        self.alert_counts[message] = self.alert_counts.get(message, 0) + 1
        self._stats['total_alerts'] += 1
        self._stats['alerts_by_severity'][severity] += 1
        
        # Registrar en historial
        alert_data = {
            'message': message,
            'severity': severity,
            'priority': priority.name,
            'count': self.alert_counts[message],
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        self.alert_history.append(alert_data)
        
        # Limpiar historial si es necesario
        self._cleanup_history()
        
        if self.alert_counts[message] >= self.alert_threshold:
            self._notify_alert(message, severity, priority, metadata)
            self.notified_alerts.add(message)
            self.last_notification[f"{severity}:{message}"] = datetime.now()
            
            # Registrar métrica si está disponible
            if self.metrics:
                self.metrics.record_alert(
                    severity=severity,
                    message=message,
                    count=self.alert_counts[message]
                )
    
    def _notify_alert(
        self,
        message: str,
        severity: str,
        priority: EventPriority,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Notifica una alerta al sistema.
        
        Args:
            message: Mensaje de la alerta
            severity: Nivel de severidad
            priority: Prioridad del evento
            metadata: Metadatos adicionales
        """
        if self.event_manager:
            event_metadata = {
                'alert_type': severity,
                'alert_count': self.alert_counts[message],
                'timestamp': datetime.now().isoformat()
            }
            if metadata:
                event_metadata.update(metadata)
            
            if severity == 'error':
                self.event_manager.publish(
                    EventType.ERROR,
                    message=f"Alert threshold exceeded: {message}",
                    priority=priority,
                    metadata=event_metadata
                )
            else:
                self.event_manager.publish(
                    EventType.THRESHOLD_EXCEEDED,
                    message=f"Alert threshold exceeded: {message}",
                    priority=priority,
                    metadata=event_metadata
                )
        
        logger.warning(
            f"Alert threshold exceeded for {severity} event: {message}"
        )
    
    def clear_alerts(self, older_than: Optional[int] = None) -> None:
        """
        Limpia el historial de alertas.
        
        Args:
            older_than: Limpiar alertas más antiguas que estos segundos
        """
        if older_than is not None:
            cutoff = datetime.now() - timedelta(seconds=older_than)
            self.alert_history = [
                alert for alert in self.alert_history
                if datetime.fromisoformat(alert['timestamp']) > cutoff
            ]
            
            # Actualizar contadores
            self.alert_counts.clear()
            for alert in self.alert_history:
                msg = alert['message']
                self.alert_counts[msg] = self.alert_counts.get(msg, 0) + 1
                
            # Limpiar notificaciones antiguas
            self.last_notification = {
                key: time for key, time in self.last_notification.items()
                if time > cutoff
            }
        else:
            self.alert_counts.clear()
            self.notified_alerts.clear()
            self.alert_history.clear()
            self.last_notification.clear()
    
    def get_alert_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de alertas.
        
        Returns:
            Dict[str, Any]: Estadísticas de alertas
        """
        severity_counts = {
            'error': 0,
            'warning': 0,
            'info': 0
        }
        
        for alert in self.alert_history:
            severity_counts[alert['severity']] += 1
        
        return {
            'total_alerts': len(self.alert_counts),
            'notified_alerts': len(self.notified_alerts),
            'alert_counts': self.alert_counts.copy(),
            'severity_distribution': severity_counts,
            'threshold': self.alert_threshold,
            'history_size': len(self.alert_history),
            'latest_alert': self.alert_history[-1] if self.alert_history else None,
            'system_stats': self._stats.copy()
        }
    
    def get_alert_history(
        self,
        severity: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Obtiene historial de alertas.
        
        Args:
            severity: Filtrar por severidad
            start_time: Tiempo de inicio
            end_time: Tiempo de fin
            limit: Límite de resultados
            
        Returns:
            List[Dict[str, Any]]: Historial de alertas
        """
        filtered = self.alert_history
        
        if severity:
            filtered = [
                alert for alert in filtered
                if alert['severity'] == severity.lower()
            ]
        
        if start_time:
            filtered = [
                alert for alert in filtered
                if datetime.fromisoformat(alert['timestamp']) >= start_time
            ]
        
        if end_time:
            filtered = [
                alert for alert in filtered
                if datetime.fromisoformat(alert['timestamp']) <= end_time
            ]
        
        return sorted(
            filtered,
            key=lambda x: x['timestamp'],
            reverse=True
        )[:limit]
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """
        Obtiene un resumen de alertas.
        
        Returns:
            Dict[str, Any]: Resumen de alertas
        """
        now = datetime.now()
        last_hour = now - timedelta(hours=1)
        last_day = now - timedelta(days=1)
        
        return {
            'current': {
                'total': self._stats['total_alerts'],
                'by_severity': self._stats['alerts_by_severity'].copy(),
                'duplicates_prevented': self._stats['duplicates_prevented']
            },
            'last_hour': {
                'total': len([
                    a for a in self.alert_history
                    if datetime.fromisoformat(a['timestamp']) >= last_hour
                ]),
                'by_severity': {
                    sev: len([
                        a for a in self.alert_history
                        if a['severity'] == sev and
                        datetime.fromisoformat(a['timestamp']) >= last_hour
                    ])
                    for sev in ['error', 'warning', 'info']
                }
            },
            'last_day': {
                'total': len([
                    a for a in self.alert_history
                    if datetime.fromisoformat(a['timestamp']) >= last_day
                ]),
                'by_severity': {
                    sev: len([
                        a for a in self.alert_history
                        if a['severity'] == sev and
                        datetime.fromisoformat(a['timestamp']) >= last_day
                    ])
                    for sev in ['error', 'warning', 'info']
                }
            }
        }