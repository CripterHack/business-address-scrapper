"""Sistema de monitoreo unificado para la caché distribuida."""

import logging
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json
from pathlib import Path
import psutil
import time

from ..metrics import MetricsManager
from ..cache import DistributedCache, CacheConfig
from ..alerts import AlertManager
from ..exceptions import CacheError
from ..logging_config import setup_logging
from ..cache.events import EventManager, EventType, EventPriority, CacheEvent

logger = logging.getLogger(__name__)

class CacheMonitor:
    """Monitor unificado para la caché distribuida."""
    
    def __init__(
        self,
        cache: DistributedCache,
        config: CacheConfig,
        event_manager: Optional[EventManager] = None,
        metrics: Optional[MetricsManager] = None,
        check_interval: int = 60,
        alert_config: Optional[Dict[str, Any]] = None
    ):
        """Inicializa el monitor.
        
        Args:
            cache: Instancia de caché distribuida
            config: Configuración de la caché
            event_manager: Gestor de eventos opcional
            metrics: Gestor de métricas opcional
            check_interval: Intervalo de chequeo en segundos
            alert_config: Configuración de alertas opcional
        """
        self.cache = cache
        self.config = config
        self.event_manager = event_manager
        self.metrics = metrics
        self.check_interval = check_interval
        
        # Estado interno
        self.node_stats: Dict[str, Dict[str, Any]] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # Inicializar sistema de alertas
        if alert_config:
            self.alert_manager = AlertManager(metrics, event_manager, alert_config)
        else:
            self.alert_manager = None
        
        # Usar configuración de logging centralizada
        setup_logging(config)
    
    def start(self) -> None:
        """Inicia el monitoreo."""
        if self.running:
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.daemon = True
        self.thread.start()
        
        logger.info("Cache monitor started")
        
        if self.event_manager:
            self.event_manager.publish(
                EventType.INFO,
                metadata={
                    'component': 'cache_monitor',
                    'action': 'start'
                }
            )
    
    def stop(self) -> None:
        """Detiene el monitoreo."""
        self.running = False
        if self.thread:
            self.thread.join()
            self.thread = None
        
        logger.info("Cache monitor stopped")
        
        if self.event_manager:
            self.event_manager.publish(
                EventType.INFO,
                metadata={
                    'component': 'cache_monitor',
                    'action': 'stop'
                }
            )
    
    def _monitor_loop(self) -> None:
        """Loop principal de monitoreo."""
        while self.running:
            try:
                self._check_nodes()
                self._update_metrics()
                time.sleep(self.check_interval)
            except Exception as e:
                self._handle_error(e, "monitor loop")
                time.sleep(60)  # Esperar antes de reintentar
    
    def _handle_error(self, error: Exception, context: str) -> None:
        """Manejo centralizado de errores del monitor."""
        error_msg = str(error)
        error_type = type(error).__name__
        error_details = {}

        if isinstance(error, CacheError):
            error_details = error.to_dict()
        else:
            error_details = {
                'type': error_type,
                'message': error_msg,
                'context': context,
                'timestamp': datetime.now().isoformat()
            }

        # Registrar error
        logger.error(f"Error in monitor {context}: {error_msg}", exc_info=True)

        # Publicar evento de error
        self.event_manager.publish(
            EventType.ERROR,
            {
                'component': 'monitor',
                'error_type': error_type,
                'error_message': error_msg,
                'error_details': error_details,
                'context': context
            }
        )
    
    def _check_nodes(self) -> None:
        """Verifica estado de los nodos."""
        try:
            current_time = datetime.now()
            
            for node_id in self.cache.nodes:
                # Verificar estado del nodo
                node_health = self.cache.check_node_health(node_id)
                
                # Inicializar estadísticas si no existen
                if node_id not in self.node_stats:
                    self.node_stats[node_id] = {
                        'status': 'unknown',
                        'uptime': 0,
                        'downtime': 0,
                        'error_count': 0,
                        'last_check': None
                    }
                
                stats = self.node_stats[node_id]
                
                # Actualizar estado
                stats['status'] = node_health['status']
                stats['error_count'] = node_health.get('error_count', 0)
                
                # Actualizar métricas de tiempo
                if stats['last_check']:
                    last_check = datetime.fromisoformat(stats['last_check'])
                    delta = (current_time - last_check).total_seconds()
                    
                    if node_health['status'] == 'healthy':
                        stats['uptime'] += delta
                        if self.metrics:
                            self.metrics.record_custom_metric(
                                'cache.node.uptime',
                                stats['uptime'],
                                {'node_id': node_id}
                            )
                    else:
                        stats['downtime'] += delta
                        if self.metrics:
                            self.metrics.record_custom_metric(
                                'cache.node.downtime',
                                stats['downtime'],
                                {'node_id': node_id}
                            )
                        
                        # Alerta y evento si downtime excede umbral
                        if stats['downtime'] > 300:  # 5 minutos
                            if self.alert_manager:
                                self.alert_manager.send_alert(
                                    'node_downtime',
                                    f"Node {node_id} has been down for {stats['downtime']} seconds",
                                    severity='critical'
                                )
                            
                            if self.event_manager:
                                self.event_manager.publish(
                                    EventType.THRESHOLD_EXCEEDED,
                                    node_id=node_id,
                                    metadata={
                                        'metric': 'downtime',
                                        'value': stats['downtime'],
                                        'threshold': 300
                                    },
                                    priority=EventPriority.HIGH
                                )
                
                stats['last_check'] = current_time.isoformat()
                
                # Actualizar métricas
                if self.metrics:
                    self.metrics.record_custom_metric(
                        'cache.node.status',
                        1 if node_health['status'] == 'healthy' else 0,
                        {'node_id': node_id}
                    )
                    self.metrics.record_custom_metric(
                        'cache.node.errors',
                        stats['error_count'],
                        {'node_id': node_id}
                    )
            
        except Exception as e:
            self._handle_error(e, "checking nodes")
    
    def get_node_stats(self, node_id: str) -> Dict[str, Any]:
        """Obtiene estadísticas de un nodo.
        
        Args:
            node_id: ID del nodo
            
        Returns:
            Dict[str, Any]: Estadísticas del nodo
        """
        with self._lock:
            return self.node_stats.get(node_id, {}).copy()
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Obtiene estadísticas de todos los nodos.
        
        Returns:
            Dict[str, Dict[str, Any]]: Estadísticas por nodo
        """
        with self._lock:
            return self.node_stats.copy()
    
    def get_system_health(self) -> Dict[str, Any]:
        """Obtiene estado general del sistema.
        
        Returns:
            Dict[str, Any]: Estado del sistema
        """
        try:
            total_nodes = len(self.cache.nodes)
            healthy_nodes = sum(
                1 for stats in self.node_stats.values()
                if stats.get('status') == 'healthy'
            )
            
            # Calcular métricas
            availability = healthy_nodes / total_nodes * 100 if total_nodes > 0 else 0
            error_rate = sum(
                stats.get('error_count', 0)
                for stats in self.node_stats.values()
            ) / total_nodes if total_nodes > 0 else 0
            
            # Obtener métricas de rendimiento
            performance = {}
            if self.metrics:
                performance = self.metrics.get_performance_metrics()
            
            return {
                'total_nodes': total_nodes,
                'healthy_nodes': healthy_nodes,
                'availability': availability,
                'error_rate': error_rate,
                'performance': performance,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self._handle_error(e, "getting system health")
            return {
                'total_nodes': 0,
                'healthy_nodes': 0,
                'availability': 0,
                'error_rate': 0,
                'performance': {},
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            } 