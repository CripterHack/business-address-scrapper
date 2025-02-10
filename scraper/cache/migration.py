"""Sistema de migración de datos para la caché distribuida."""

import logging
import time
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
import threading
from concurrent.futures import ThreadPoolExecutor
import json

from ..exceptions import CacheError
from ..metrics import MetricsManager

logger = logging.getLogger(__name__)

@dataclass
class MigrationTask:
    """Tarea de migración."""
    source_node: str
    target_node: str
    keys: Set[str]
    status: str = 'pending'  # 'pending', 'in_progress', 'completed', 'failed'
    progress: float = 0.0
    error: Optional[str] = None

class MigrationManager:
    """Gestor de migración de datos."""
    
    def __init__(
        self,
        cache,  # DistributedCache
        metrics: Optional[MetricsManager] = None,
        max_workers: int = 4,
        batch_size: int = 100
    ):
        """Inicializa el gestor de migración.
        
        Args:
            cache: Instancia de caché distribuida
            metrics: Gestor de métricas opcional
            max_workers: Número máximo de workers
            batch_size: Tamaño de lote para migración
        """
        self.cache = cache
        self.metrics = metrics
        self.max_workers = max_workers
        self.batch_size = batch_size
        
        self.tasks: Dict[str, MigrationTask] = {}
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def create_migration_plan(
        self,
        movements: Dict[str, List[str]]
    ) -> Dict[str, MigrationTask]:
        """Crea un plan de migración.
        
        Args:
            movements: Mapa de movimientos de datos
            
        Returns:
            Dict[str, MigrationTask]: Plan de migración
        """
        tasks = {}
        
        for source_node, target_nodes in movements.items():
            # Obtener claves del nodo origen
            keys = self._get_node_keys(source_node)
            
            # Crear tarea por cada nodo destino
            for target_node in target_nodes:
                task_id = f"{source_node}-to-{target_node}"
                tasks[task_id] = MigrationTask(
                    source_node=source_node,
                    target_node=target_node,
                    keys=keys
                )
        
        return tasks
    
    def execute_migration(self, tasks: Dict[str, MigrationTask]) -> bool:
        """Ejecuta un plan de migración.
        
        Args:
            tasks: Plan de migración
            
        Returns:
            bool: True si la migración fue exitosa
        """
        self.tasks = tasks
        futures = []
        
        try:
            # Iniciar tareas de migración
            for task_id, task in tasks.items():
                future = self.executor.submit(
                    self._migrate_data,
                    task_id,
                    task
                )
                futures.append(future)
            
            # Esperar a que todas las tareas terminen
            for future in futures:
                future.result()
            
            # Verificar si todas las tareas fueron exitosas
            return all(
                task.status == 'completed'
                for task in self.tasks.values()
            )
            
        except Exception as e:
            logger.error(f"Error executing migration: {str(e)}")
            return False
    
    def _migrate_data(self, task_id: str, task: MigrationTask) -> None:
        """Migra datos entre nodos.
        
        Args:
            task_id: ID de la tarea
            task: Tarea de migración
        """
        try:
            task.status = 'in_progress'
            total_keys = len(task.keys)
            migrated_keys = 0
            
            # Migrar datos en lotes
            keys_list = list(task.keys)
            for i in range(0, total_keys, self.batch_size):
                batch = keys_list[i:i + self.batch_size]
                
                try:
                    self._migrate_batch(
                        task.source_node,
                        task.target_node,
                        batch
                    )
                    migrated_keys += len(batch)
                    
                    # Actualizar progreso
                    with self.lock:
                        task.progress = migrated_keys / total_keys
                    
                except Exception as e:
                    logger.error(
                        f"Error migrating batch in task {task_id}: {str(e)}"
                    )
            
            task.status = 'completed'
            
            if self.metrics:
                self.metrics.record_migration_success(task_id)
            
        except Exception as e:
            error_msg = f"Migration failed: {str(e)}"
            logger.error(error_msg)
            task.status = 'failed'
            task.error = error_msg
            
            if self.metrics:
                self.metrics.record_migration_failure(task_id)
    
    def _migrate_batch(
        self,
        source_node: str,
        target_node: str,
        keys: List[str]
    ) -> None:
        """Migra un lote de datos.
        
        Args:
            source_node: Nodo origen
            target_node: Nodo destino
            keys: Lista de claves a migrar
        """
        source = self.cache._get_node(source_node)
        target = self.cache._get_node(target_node)
        
        for key in keys:
            try:
                # Obtener valor y TTL del origen
                value = self.cache._get_from_node(source, key)
                if value is None:
                    continue
                
                # Obtener TTL restante si existe
                ttl = None
                if source['type'] == 'redis':
                    ttl = source['client'].ttl(key)
                    if ttl < 0:  # No TTL o clave no existe
                        ttl = None
                
                # Guardar en destino
                self.cache._set_in_node(target, key, value, ttl)
                
            except Exception as e:
                logger.error(
                    f"Error migrating key {key} from {source_node} "
                    f"to {target_node}: {str(e)}"
                )
                raise
    
    def _get_node_keys(self, node_id: str) -> Set[str]:
        """Obtiene las claves de un nodo.
        
        Args:
            node_id: ID del nodo
            
        Returns:
            Set[str]: Conjunto de claves
        """
        node = self.cache._get_node(node_id)
        keys = set()
        
        try:
            if node['type'] == 'redis':
                # Usar SCAN para obtener claves de forma eficiente
                cursor = '0'
                while cursor != 0:
                    cursor, batch = node['client'].scan(
                        cursor=cursor,
                        count=self.batch_size
                    )
                    keys.update(batch)
            elif node['type'] == 'memcached':
                # Memcached no tiene forma de listar claves
                # Se podría implementar un mecanismo de tracking
                pass
            
            return keys
            
        except Exception as e:
            logger.error(f"Error getting keys from node {node_id}: {str(e)}")
            raise
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Obtiene el estado de la migración.
        
        Returns:
            Dict[str, Any]: Estado de la migración
        """
        with self.lock:
            status = {
                'tasks': len(self.tasks),
                'completed': sum(
                    1 for task in self.tasks.values()
                    if task.status == 'completed'
                ),
                'failed': sum(
                    1 for task in self.tasks.values()
                    if task.status == 'failed'
                ),
                'in_progress': sum(
                    1 for task in self.tasks.values()
                    if task.status == 'in_progress'
                ),
                'total_progress': sum(
                    task.progress for task in self.tasks.values()
                ) / len(self.tasks) if self.tasks else 0.0,
                'task_details': {
                    task_id: {
                        'status': task.status,
                        'progress': task.progress,
                        'error': task.error
                    }
                    for task_id, task in self.tasks.items()
                }
            }
            
            return status 