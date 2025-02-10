"""Sistema de recuperación automática para la caché distribuida."""

import logging
import threading
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timedelta
import json
from pathlib import Path
import time
from enum import Enum
from dataclasses import dataclass

from ..cache import DistributedCache, CacheConfig
from ..metrics import MetricsManager
from ..exceptions import CacheError, CacheConsistencyError
from ..logging_config import setup_logging, CompressedRotatingFileHandler
from ..cache.events import EventManager, EventType, EventPriority, CacheEvent

logger = logging.getLogger(__name__)

class RecoveryAction(Enum):
    """Tipos de acciones de recuperación."""
    RECONNECT = "reconnect"
    REBALANCE = "rebalance"
    REPAIR = "repair"
    REBUILD = "rebuild"
    ROLLBACK = "rollback"

class RecoveryPriority(Enum):
    """Prioridades de recuperación."""
    LOW = 0
    MEDIUM = 1
    HIGH = 2
    CRITICAL = 3

@dataclass
class RecoveryTask:
    """Tarea de recuperación."""
    id: str
    action: RecoveryAction
    priority: RecoveryPriority
    node_id: str
    timestamp: datetime
    status: str
    error: Optional[str] = None
    retries: int = 0
    max_retries: int = 3
    requires_approval: bool = False
    approved: bool = False
    recovery_data: Optional[Dict[str, Any]] = None

class RecoveryManager:
    """Gestor de recuperación automática."""
    
    def __init__(
        self,
        cache: DistributedCache,
        config: CacheConfig,
        event_manager: Optional[EventManager] = None,
        metrics: Optional[MetricsManager] = None,
        check_interval: int = 30,
        task_timeout: int = 300,
        max_concurrent_tasks: int = 3
    ):
        """Inicializa el gestor de recuperación."""
        self.cache = cache
        self.config = config
        self.event_manager = event_manager
        self.metrics = metrics
        self.check_interval = check_interval
        self.task_timeout = task_timeout
        self.max_concurrent_tasks = max_concurrent_tasks
        
        # Estado interno
        self.tasks: Dict[str, RecoveryTask] = {}
        self.active_tasks: Dict[str, RecoveryTask] = {}
        self.node_states: Dict[str, Dict[str, Any]] = {}
        self.running = False
        
        # Locks
        self._task_lock = threading.Lock()
        self._state_lock = threading.Lock()
        
        # Threads
        self._recovery_thread = threading.Thread(target=self._recovery_loop)
        self._recovery_thread.daemon = True
        
        # Handlers de recuperación
        self._recovery_handlers: Dict[RecoveryAction, Callable] = {
            RecoveryAction.RECONNECT: self._handle_reconnect,
            RecoveryAction.REBALANCE: self._handle_rebalance,
            RecoveryAction.REPAIR: self._handle_repair,
            RecoveryAction.REBUILD: self._handle_rebuild,
            RecoveryAction.ROLLBACK: self._handle_rollback
        }
        
        # Configurar logging
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Configura el sistema de logging."""
        # Usar configuración centralizada
        setup_logging(self.config)
        
        # Añadir handler específico para recuperación
        recovery_handler = CompressedRotatingFileHandler(
            filename=Path(self.config.metrics.log_dir) / 'recovery' / 'recovery.log',
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        recovery_handler.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        )
        logger.addHandler(recovery_handler)
    
    def start(self) -> None:
        """Inicia el gestor de recuperación."""
        if self.running:
            return
        
        self.running = True
        self._recovery_thread.start()
        logger.info("Recovery manager started")
        
        if self.event_manager:
            self.event_manager.publish(
                EventType.INFO,
                metadata={
                    'component': 'recovery_manager',
                    'action': 'start'
                }
            )
    
    def stop(self) -> None:
        """Detiene el gestor de recuperación."""
        self.running = False
        self._recovery_thread.join()
        logger.info("Recovery manager stopped")
        
        if self.event_manager:
            self.event_manager.publish(
                EventType.INFO,
                metadata={
                    'component': 'recovery_manager',
                    'action': 'stop'
                }
            )
    
    def _recovery_loop(self) -> None:
        """Loop principal de recuperación."""
        while self.running:
            try:
                self._check_node_states()
                self._process_tasks()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in recovery loop: {str(e)}")
                if self.event_manager:
                    self.event_manager.publish(
                        EventType.ERROR,
                        metadata={
                            'component': 'recovery_manager',
                            'action': 'recovery_loop',
                            'error': str(e)
                        },
                        priority=EventPriority.HIGH
                    )
                time.sleep(60)
    
    def _check_node_states(self) -> None:
        """Verifica el estado de los nodos."""
        health = self.cache.check_health()
        
        with self._state_lock:
            for node_id, node_health in health['nodes'].items():
                if node_id not in self.node_states:
                    self.node_states[node_id] = {
                        'status': 'unknown',
                        'last_error': None,
                        'error_count': 0,
                        'last_recovery': None
                    }
                
                state = self.node_states[node_id]
                current_status = node_health['status']
                
                if current_status == 'error':
                    state['error_count'] += 1
                    state['last_error'] = node_health.get('error')
                    
                    # Publicar evento de nodo caído
                    if self.event_manager:
                        self.event_manager.publish(
                            EventType.NODE_DOWN,
                            node_id=node_id,
                            metadata={
                                'error': node_health.get('error'),
                                'error_count': state['error_count']
                            },
                            priority=EventPriority.HIGH
                        )
                    
                    # Crear tarea de recuperación si es necesario
                    if state['error_count'] >= 3:
                        self._create_recovery_task(
                            node_id,
                            RecoveryAction.RECONNECT,
                            RecoveryPriority.HIGH
                        )
                else:
                    if state['status'] == 'error' and current_status == 'ok':
                        # Nodo recuperado
                        if self.event_manager:
                            self.event_manager.publish(
                                EventType.NODE_UP,
                                node_id=node_id,
                                metadata={
                                    'previous_error_count': state['error_count']
                                }
                            )
                    state['error_count'] = 0
                
                state['status'] = current_status
    
    def _process_tasks(self) -> None:
        """Procesa tareas de recuperación."""
        with self._task_lock:
            # Limpiar tareas completadas
            self.active_tasks = {
                task_id: task
                for task_id, task in self.active_tasks.items()
                if task.status in ['running', 'pending']
            }
            
            # Procesar nuevas tareas
            if len(self.active_tasks) < self.max_concurrent_tasks:
                available_slots = self.max_concurrent_tasks - len(self.active_tasks)
                pending_tasks = sorted(
                    [
                        task for task in self.tasks.values()
                        if task.status == 'pending'
                    ],
                    key=lambda x: (x.priority.value, x.timestamp),
                    reverse=True
                )
                
                for task in pending_tasks[:available_slots]:
                    if task.requires_approval and not task.approved:
                        continue
                    
                    self._execute_task(task)
    
    def _execute_task(self, task: RecoveryTask) -> None:
        """Ejecuta una tarea de recuperación."""
        try:
            logger.info(f"Executing recovery task: {task.id}")
            task.status = 'running'
            self.active_tasks[task.id] = task
            
            if self.event_manager:
                self.event_manager.publish(
                    EventType.RECOVERY_START,
                    metadata={
                        'task_id': task.id,
                        'action': task.action.value,
                        'node_id': task.node_id
                    }
                )
            
            # Obtener y ejecutar handler
            handler = self._recovery_handlers.get(task.action)
            if not handler:
                raise ValueError(f"No handler for action: {task.action}")
            
            result = handler(task)
            
            if result:
                task.status = 'completed'
                if self.metrics:
                    self.metrics.record_custom_metric(
                        'recovery_success',
                        1
                    )
                if self.event_manager:
                    self.event_manager.publish(
                        EventType.RECOVERY_COMPLETE,
                        metadata={
                            'task_id': task.id,
                            'action': task.action.value,
                            'node_id': task.node_id
                        }
                    )
            else:
                task.status = 'failed'
                task.retries += 1
                if task.retries < task.max_retries:
                    task.status = 'pending'
                if self.metrics:
                    self.metrics.record_custom_metric(
                        'recovery_failure',
                        1
                    )
                if self.event_manager:
                    self.event_manager.publish(
                        EventType.RECOVERY_FAILED,
                        metadata={
                            'task_id': task.id,
                            'action': task.action.value,
                            'node_id': task.node_id,
                            'retries': task.retries
                        },
                        priority=EventPriority.HIGH
                    )
            
        except Exception as e:
            logger.error(f"Error executing task {task.id}: {str(e)}")
            task.status = 'failed'
            task.error = str(e)
            task.retries += 1
            if task.retries < task.max_retries:
                task.status = 'pending'
            if self.event_manager:
                self.event_manager.publish(
                    EventType.ERROR,
                    metadata={
                        'component': 'recovery_manager',
                        'action': 'execute_task',
                        'task_id': task.id,
                        'error': str(e)
                    },
                    priority=EventPriority.HIGH
                )
    
    def _create_recovery_task(
        self,
        node_id: str,
        action: RecoveryAction,
        priority: RecoveryPriority,
        requires_approval: bool = False,
        recovery_data: Optional[Dict[str, Any]] = None
    ) -> str:
        """Crea una nueva tarea de recuperación."""
        task = RecoveryTask(
            id=f"{action.value}_{node_id}_{int(time.time())}",
            action=action,
            priority=priority,
            node_id=node_id,
            timestamp=datetime.now(),
            status='pending',
            requires_approval=requires_approval,
            recovery_data=recovery_data
        )
        
        with self._task_lock:
            self.tasks[task.id] = task
        
        logger.info(f"Created recovery task: {task.id}")
        
        if self.event_manager:
            self.event_manager.publish(
                EventType.INFO,
                metadata={
                    'component': 'recovery_manager',
                    'action': 'create_task',
                    'task_id': task.id,
                    'recovery_action': action.value,
                    'node_id': node_id,
                    'requires_approval': requires_approval
                }
            )
        
        return task.id
    
    def approve_task(self, task_id: str) -> bool:
        """Aprueba una tarea que requiere aprobación."""
        with self._task_lock:
            if task_id not in self.tasks:
                return False
            
            task = self.tasks[task_id]
            if not task.requires_approval:
                return False
            
            task.approved = True
            logger.info(f"Task approved: {task_id}")
            return True
    
    def _handle_reconnect(self, task: RecoveryTask) -> bool:
        """Maneja la reconexión de un nodo."""
        try:
            node = self.cache._get_node(task.node_id)
            if not node:
                raise CacheError(f"Node not found: {task.node_id}")
            
            # Intentar reconectar
            if node['type'] == 'redis':
                node['client'].ping()
            elif node['type'] == 'memcached':
                node['client'].get('test_key')
            
            return True
            
        except Exception as e:
            logger.error(f"Reconnect failed for node {task.node_id}: {str(e)}")
            return False
    
    def _handle_rebalance(self, task: RecoveryTask) -> bool:
        """Maneja el rebalanceo de datos."""
        try:
            self.cache.rebalance_data()
            return True
        except Exception as e:
            logger.error(f"Rebalance failed: {str(e)}")
            return False
    
    def _handle_repair(self, task: RecoveryTask) -> bool:
        """Maneja la reparación de datos inconsistentes."""
        try:
            if not task.recovery_data:
                raise ValueError("No recovery data provided")
            
            keys = task.recovery_data.get('keys', [])
            for key in keys:
                try:
                    # Verificar consistencia
                    values = self.cache.get_all_replicas(key)
                    if not values:
                        continue
                    
                    # Encontrar valor correcto
                    correct_value = max(
                        values,
                        key=lambda x: x.get('timestamp', 0)
                    )
                    
                    # Reparar réplicas inconsistentes
                    for node_id, value in values.items():
                        if value != correct_value:
                            self.cache.set_on_node(
                                node_id,
                                key,
                                correct_value['value'],
                                correct_value['timestamp']
                            )
                except Exception as e:
                    logger.error(f"Error repairing key {key}: {str(e)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Repair failed: {str(e)}")
            return False
    
    def _handle_rebuild(self, task: RecoveryTask) -> bool:
        """Maneja la reconstrucción de un nodo."""
        try:
            if not task.recovery_data:
                raise ValueError("No recovery data provided")
            
            source_node = task.recovery_data.get('source_node')
            if not source_node:
                raise ValueError("No source node specified")
            
            # Copiar datos del nodo fuente
            keys = self.cache.get_node_keys(source_node)
            for key in keys:
                try:
                    value = self.cache.get_from_node(source_node, key)
                    self.cache.set_on_node(task.node_id, key, value)
                except Exception as e:
                    logger.error(f"Error copying key {key}: {str(e)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Rebuild failed: {str(e)}")
            return False
    
    def _handle_rollback(self, task: RecoveryTask) -> bool:
        """Maneja el rollback de cambios."""
        try:
            if not task.recovery_data:
                raise ValueError("No recovery data provided")
            
            snapshot = task.recovery_data.get('snapshot')
            if not snapshot:
                raise ValueError("No snapshot data provided")
            
            # Restaurar datos desde snapshot
            for key, value in snapshot.items():
                try:
                    self.cache.set(key, value)
                except Exception as e:
                    logger.error(f"Error rolling back key {key}: {str(e)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Rollback failed: {str(e)}")
            return False
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene el estado de una tarea."""
        with self._task_lock:
            task = self.tasks.get(task_id)
            if not task:
                return None
            
            return {
                'id': task.id,
                'action': task.action.value,
                'priority': task.priority.value,
                'node_id': task.node_id,
                'status': task.status,
                'error': task.error,
                'retries': task.retries,
                'requires_approval': task.requires_approval,
                'approved': task.approved,
                'timestamp': task.timestamp.isoformat()
            }
    
    def get_node_status(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene el estado de un nodo."""
        with self._state_lock:
            return self.node_states.get(node_id)