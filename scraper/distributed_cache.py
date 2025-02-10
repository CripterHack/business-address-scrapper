"""Sistema de caché distribuida."""

import logging
import time
import threading
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
import json
import pickle
import hashlib
from pathlib import Path

from .cache import CacheBackend
from .metrics import MetricsManager
from .exceptions import CacheError
from .cache.partitioner import create_partitioner, Partition
from .cache.compression import CompressionManager
from .cache.migration import MigrationManager
from .cache.locks import DistributedLock
from .cache.events import EventManager, EventType, EventPriority, CacheEvent
from .cache.backup import BackupManager
from .cache.priority import PriorityManager
from .cache.circuit_breaker import CircuitBreaker
from .cache.cleaner import CacheCleaner, CleanupStrategy
from .cache.encryption import EncryptionManager
from .cache.auth import AuthManager, Permission, Role

logger = logging.getLogger(__name__)

class DistributedCache(CacheBackend):
    """Implementación de caché distribuida."""
    
    def __init__(
        self,
        nodes: List[Dict[str, Any]],
        metrics: Optional[MetricsManager] = None,
        replication_factor: int = 2,
        consistency_level: str = 'quorum',
        retry_attempts: int = 3,
        retry_delay: int = 1,
        partitioning_strategy: str = 'consistent_hash',
        compression_algorithm: str = 'zlib',
        compression_threshold: float = 0.9,
        cleanup_strategy: CleanupStrategy = CleanupStrategy.LRU,
        max_size_mb: int = 1000,
        max_items: int = 1000000,
        operation_timeout: float = 1.0,
        encryption_key: Optional[str] = None,
        auth_file: Optional[str] = None
    ):
        """Inicializa la caché distribuida.
        
        Args:
            nodes: Lista de nodos
            metrics: Gestor de métricas opcional
            replication_factor: Factor de replicación
            consistency_level: Nivel de consistencia
            retry_attempts: Número de reintentos
            retry_delay: Tiempo entre reintentos
            partitioning_strategy: Estrategia de particionamiento
            compression_algorithm: Algoritmo de compresión
            compression_threshold: Umbral de compresión
            cleanup_strategy: Estrategia de limpieza
            max_size_mb: Tamaño máximo en MB
            max_items: Número máximo de items
            operation_timeout: Timeout de operaciones
            encryption_key: Clave de encriptación opcional
            auth_file: Archivo de autenticación opcional
        """
        self.nodes = nodes
        self.metrics = metrics
        self.replication_factor = replication_factor
        self.consistency_level = consistency_level
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        # Inicializar componentes
        self.event_manager = EventManager()
        self.partitioner = create_partitioner(
            partitioning_strategy,
            nodes
        )
        self.compression = CompressionManager(
            default_algorithm=compression_algorithm,
            compression_threshold=compression_threshold
        )
        self.lock_manager = DistributedLock(self)
        self.priority_manager = PriorityManager()
        self.backup_manager = BackupManager(
            self,
            event_manager=self.event_manager,
            compression_manager=self.compression
        )
        self.migration_manager = MigrationManager(
            self,
            metrics=metrics
        )
        self.cleaner = CacheCleaner(
            self,
            event_manager=self.event_manager,
            priority_manager=self.priority_manager,
            max_size_mb=max_size_mb,
            max_items=max_items,
            strategy=cleanup_strategy
        )
        self.encryption = EncryptionManager(
            secret_key=encryption_key
        )
        self.auth = AuthManager(
            auth_file=auth_file
        )
        
        # Circuit breakers por nodo
        self.circuit_breakers = {
            node['id']: CircuitBreaker(
                operation_timeout=operation_timeout
            )
            for node in nodes
        }
        
        # Estado interno
        self.node_status = {node['id']: True for node in nodes}
        self._initialize_connections()
        
        # Iniciar componentes
        self.event_manager.start()
        self.backup_manager.start()
        self.priority_manager.start()
        self.cleaner.start()
        
        logger.info("Distributed cache initialized")
    
    def _initialize_connections(self) -> None:
        """Inicializa conexiones a los nodos."""
        for node in self.nodes:
            try:
                if node['type'] == 'redis':
                    self._init_redis_connection(node)
                elif node['type'] == 'memcached':
                    self._init_memcached_connection(node)
                else:
                    raise CacheError(f"Unsupported cache type: {node['type']}")
            except Exception as e:
                logger.error(f"Error connecting to node {node['id']}: {str(e)}")
                self.node_status[node['id']] = False
    
    def _init_redis_connection(self, node: Dict[str, Any]) -> None:
        """Inicializa conexión Redis."""
        try:
            import redis
            node['client'] = redis.Redis(
                host=node['host'],
                port=node['port'],
                db=node.get('db', 0),
                password=node.get('password'),
                socket_timeout=node.get('timeout', 5)
            )
            node['client'].ping()
        except Exception as e:
            raise CacheError(f"Error initializing Redis connection: {str(e)}")
    
    def _init_memcached_connection(self, node: Dict[str, Any]) -> None:
        """Inicializa conexión Memcached."""
        try:
            import memcache
            node['client'] = memcache.Client([f"{node['host']}:{node['port']}"])
            if not node['client'].get_stats():
                raise CacheError("Could not connect to Memcached")
        except Exception as e:
            raise CacheError(f"Error initializing Memcached connection: {str(e)}")
    
    def _publish_event(
        self,
        event_type: EventType,
        message: Optional[str] = None,
        key: Optional[str] = None,
        value: Any = None,
        priority: EventPriority = EventPriority.LOW,
        **kwargs
    ) -> None:
        """
        Publica un evento en el sistema.
        
        Args:
            event_type: Tipo de evento
            message: Mensaje opcional
            key: Clave opcional
            value: Valor opcional
            priority: Prioridad del evento
            **kwargs: Argumentos adicionales
        """
        if self.event_manager:
            self.event_manager.publish(
                event_type,
                message=message,
                key=key,
                value=value,
                priority=priority,
                **kwargs
            )

    def _handle_operation_error(
        self,
        operation: str,
        error: Exception,
        key: Optional[str] = None,
        value: Any = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Maneja errores de operaciones de caché.
        
        Args:
            operation: Nombre de la operación
            error: Error ocurrido
            key: Clave opcional
            value: Valor opcional
            metadata: Metadatos adicionales
        """
        error_msg = f"Error in {operation}: {str(error)}"
        logger.error(error_msg)
        
        # Publicar evento de error
        event_metadata = {
            'operation': operation,
            'error_type': type(error).__name__,
            'error': str(error)
        }
        if metadata:
            event_metadata.update(metadata)
        
        self._publish_event(
            EventType.ERROR,
            message=error_msg,
            key=key,
            value=value,
            priority=EventPriority.HIGH,
            metadata=event_metadata
        )
        
        # Registrar métrica
        if self.metrics:
            self.metrics.record_error(operation, str(error))

    def _record_operation_metrics(
        self,
        operation: str,
        success: bool,
        start_time: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Registra métricas de una operación.
        
        Args:
            operation: Nombre de la operación
            success: Si la operación fue exitosa
            start_time: Tiempo de inicio
            metadata: Metadatos adicionales
        """
        if not self.metrics:
            return
        
        duration = time.time() - start_time
        
        if operation == 'get':
            if success:
                self.metrics.record_cache_hit()
            else:
                self.metrics.record_cache_miss()
        elif operation == 'set':
            self.metrics.record_write()
        
        self.metrics.record_operation_duration(operation, duration)
        
        if metadata:
            self.metrics.record_operation_metadata(operation, metadata)

    def get(
        self,
        key: str,
        default: Any = None,
        token: Optional[str] = None
    ) -> Any:
        """
        Obtiene un valor de la caché.
        
        Args:
            key: Clave a obtener
            default: Valor por defecto
            token: Token de autenticación opcional
            
        Returns:
            Any: Valor almacenado o default
        """
        start_time = time.time()
        try:
            # Verificar autenticación
            if token and not self.auth.has_permission(token, Permission.READ):
                raise CacheError("Unauthorized access")
            
            # Obtener partición
            partition = self.partitioner.get_partition(key)
            nodes = [partition.node_id] + partition.replica_nodes
            required_reads = self._get_required_reads()
            successful_reads = 0
            value = None
            metadata = {
                'reads_attempted': 0,
                'reads_succeeded': 0,
                'nodes_tried': []
            }
            
            # Intentar leer de nodos
            for node_id in nodes:
                try:
                    node = self._get_node(node_id)
                    if not node:
                        continue
                    
                    # Usar circuit breaker
                    def read_operation():
                        return self._get_from_node(node, key)
                        
                    def fallback():
                        logger.warning(f"Circuit open for node {node_id}")
                        return None
                    
                    node_value = self.circuit_breakers[node_id].execute(
                        read_operation,
                        fallback
                    )
                    
                    if node_value is not None:
                        # Desencriptar si es necesario
                        if self.encryption.is_encrypted(key):
                            node_value = self.encryption.decrypt(node_value)
                        
                        if value is None:
                            value = node_value
                        successful_reads += 1
                        
                        if successful_reads >= required_reads:
                            break
                            
                except Exception as e:
                    logger.warning(
                        f"Error reading from node {node_id}: {str(e)}"
                    )
            
            # Verificar consistencia
            if successful_reads < required_reads:
                raise CacheError(
                    f"Failed to achieve {self.consistency_level} consistency"
                )
            
            # Registrar métricas
            if self.metrics:
                if value is not None:
                    self.metrics.record_cache_hit()
                else:
                    self.metrics.record_cache_miss()
            
            # Publicar evento
            self._publish_event(
                EventType.GET,
                key=key,
                value=value
            )
            
            self._record_operation_metrics(
                'get',
                value is not None,
                start_time,
                metadata
            )
            
            return value or default
            
        except Exception as e:
            self._handle_operation_error('get', e, key, metadata=metadata)
            return default
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        priority: EventPriority = EventPriority.LOW,
        token: Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Almacena un valor en la caché.
        
        Args:
            key: Clave a almacenar
            value: Valor a almacenar
            ttl: Tiempo de vida en segundos
            priority: Prioridad de la operación
            token: Token de autenticación opcional
            **kwargs: Argumentos adicionales
            
        Returns:
            bool: True si se almacenó correctamente
        """
        start_time = time.time()
        metadata = {
            'writes_attempted': 0,
            'writes_succeeded': 0,
            'nodes_tried': []
        }
        
        try:
            # Verificar autenticación
            if token and not self.auth.has_permission(token, Permission.WRITE):
                raise CacheError("Unauthorized write operation")
            
            # Verificar si se debe encriptar
            if self.encryption.should_encrypt(key, value):
                value = self.encryption.encrypt(value)
                self.encryption.mark_as_encrypted(key)
            
            # Comprimir si es posible
            compressed = self.compression.compress(key, value)
            if compressed:
                value = compressed
            
            # Obtener nodos para la clave
            partition = self.partitioner.get_partition(key)
            nodes = [partition.node_id] + partition.replica_nodes[:self.replication_factor - 1]
            
            # Escribir en nodos
            def write_operation():
                success_count = 0
                for node_id in nodes:
                    if self._set_in_node(self._get_node(node_id), key, value, ttl):
                        success_count += 1
                return success_count >= self._get_required_writes()
            
            def fallback():
                # Intentar escribir en cualquier nodo disponible
                for node in self.nodes:
                    try:
                        if self._set_in_node(node, key, value, ttl):
                            return True
                    except:
                        continue
                return False
            
            # Ejecutar con circuit breaker
            success = False
            for attempt in range(self.retry_attempts):
                try:
                    success = write_operation()
                    if success:
                        break
                except Exception as e:
                    logger.error(f"Write attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(self.retry_delay)
            
            if not success:
                success = fallback()
                if not success:
                    raise CacheError("Write operation failed")
            
            # Registrar en priority manager
            self.priority_manager.add_key(key, priority, ttl)
            
            # Publicar evento
            self._publish_event(
                EventType.SET,
                key=key,
                value=value,
                priority=priority,
                ttl=ttl,
                compressed=bool(compressed),
                encrypted=self.encryption.is_encrypted(key),
                **kwargs
            )
            
            # Actualizar métricas
            if self.metrics:
                self.metrics.record_write()
            
            self._record_operation_metrics(
                'set',
                success,
                start_time,
                metadata
            )
            
            return success
            
        except Exception as e:
            self._handle_operation_error('set', e, key, value, metadata)
            return False
    
    def delete(
        self,
        key: str,
        token: Optional[str] = None
    ) -> bool:
        """
        Elimina una clave de la caché.
        
        Args:
            key: Clave a eliminar
            token: Token de autenticación opcional
            
        Returns:
            bool: True si se eliminó correctamente
        """
        start_time = time.time()
        metadata = {
            'deletes_attempted': 0,
            'deletes_succeeded': 0,
            'nodes_tried': []
        }
        
        try:
            # Verificar autenticación
            if token and not self.auth.has_permission(token, Permission.DELETE):
                raise CacheError("Unauthorized access")
            
            partition = self.partitioner.get_partition(key)
            required_writes = self._get_required_writes()
            successful_writes = 0
            
            # Eliminar del nodo primario
            if self.node_status[partition.node_id]:
                try:
                    self._delete_from_node(self._get_node(partition.node_id), key)
                    successful_writes += 1
                except Exception as e:
                    logger.error(f"Error deleting from primary node: {str(e)}")
                    self.node_status[partition.node_id] = False
            
            # Eliminar de réplicas
            for replica_id in partition.replica_nodes:
                if not self.node_status[replica_id]:
                    continue
                
                try:
                    self._delete_from_node(self._get_node(replica_id), key)
                    successful_writes += 1
                    
                    if successful_writes >= required_writes:
                        break
                except Exception as e:
                    logger.error(f"Error deleting from replica node: {str(e)}")
                    self.node_status[replica_id] = False
            
            if successful_writes < required_writes:
                raise CacheError(
                    f"Failed to achieve required deletes "
                    f"({successful_writes}/{required_writes})"
                )
            
            # Limpiar metadatos
            self.encryption.mark_as_encrypted(key)
            self.priority_manager.remove_key(key)
            
            self._publish_event(
                EventType.DELETE,
                key=key
            )
            
            self._record_operation_metrics(
                'delete',
                True,
                start_time,
                metadata
            )
            
            return True
            
        except Exception as e:
            self._handle_operation_error('delete', e, key, metadata=metadata)
            return False
    
    def clear(self) -> None:
        """Limpia toda la caché distribuida."""
        for node in self.nodes:
            if not self.node_status[node['id']]:
                continue
            
            try:
                self._clear_node(node)
            except Exception as e:
                logger.error(f"Error clearing node {node['id']}: {str(e)}")
                self.node_status[node['id']] = False
    
    def _get_node(self, node_id: str) -> Dict[str, Any]:
        """Obtiene un nodo por su ID.
        
        Args:
            node_id: ID del nodo
            
        Returns:
            Dict[str, Any]: Configuración del nodo
        """
        for node in self.nodes:
            if node['id'] == node_id:
                return node
        raise CacheError(f"Node not found: {node_id}")
    
    def _get_required_reads(self) -> int:
        """Obtiene el número requerido de lecturas exitosas."""
        if self.consistency_level == 'one':
            return 1
        elif self.consistency_level == 'quorum':
            return (self.replication_factor // 2) + 1
        else:  # 'all'
            return self.replication_factor
    
    def _get_required_writes(self) -> int:
        """Obtiene el número requerido de escrituras exitosas."""
        if self.consistency_level == 'one':
            return 1
        elif self.consistency_level == 'quorum':
            return (self.replication_factor // 2) + 1
        else:  # 'all'
            return self.replication_factor
    
    def _get_from_node(self, node: Dict[str, Any], key: str) -> Optional[Any]:
        """Obtiene un valor de un nodo.
        
        Args:
            node: Nodo
            key: Clave
            
        Returns:
            Any: Valor almacenado o None si no existe
        """
        if node['type'] == 'redis':
            value = node['client'].get(key)
            if value:
                try:
                    # Intentar descomprimir
                    return self.compression.decompress(value)
                except:
                    # Si falla, asumir que no está comprimido
                    return pickle.loads(value)
            return None
        elif node['type'] == 'memcached':
            value = node['client'].get(key)
            if value:
                try:
                    return self.compression.decompress(value)
                except:
                    return value
            return None
        else:
            raise CacheError(f"Unsupported cache type: {node['type']}")
    
    def _set_in_node(
        self,
        node: Dict[str, Any],
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """Guarda un valor en un nodo.
        
        Args:
            node: Nodo
            key: Clave
            value: Valor
            ttl: Tiempo de vida en segundos
        """
        if node['type'] == 'redis':
            if ttl:
                node['client'].setex(key, ttl, value)
            else:
                node['client'].set(key, value)
            return True
        elif node['type'] == 'memcached':
            node['client'].set(key, value, time=ttl or 0)
            return True
        else:
            raise CacheError(f"Unsupported cache type: {node['type']}")
    
    def _delete_from_node(self, node: Dict[str, Any], key: str) -> None:
        """Elimina un valor de un nodo.
        
        Args:
            node: Nodo
            key: Clave
        """
        if node['type'] == 'redis':
            node['client'].delete(key)
        elif node['type'] == 'memcached':
            node['client'].delete(key)
        else:
            raise CacheError(f"Unsupported cache type: {node['type']}")
    
    def _clear_node(self, node: Dict[str, Any]) -> None:
        """Limpia un nodo.
        
        Args:
            node: Nodo
        """
        if node['type'] == 'redis':
            node['client'].flushdb()
        elif node['type'] == 'memcached':
            node['client'].flush_all()
        else:
            raise CacheError(f"Unsupported cache type: {node['type']}")
    
    def check_health(self) -> Dict[str, Any]:
        """Verifica el estado de los nodos.
        
        Returns:
            Dict[str, Any]: Estado de los nodos
        """
        health = {
            'total_nodes': len(self.nodes),
            'healthy_nodes': sum(1 for status in self.node_status.values() if status),
            'nodes': {}
        }
        
        for node in self.nodes:
            try:
                if node['type'] == 'redis':
                    node['client'].ping()
                    latency = self._measure_latency(node)
                    status = 'healthy'
                elif node['type'] == 'memcached':
                    stats = node['client'].get_stats()
                    latency = self._measure_latency(node)
                    status = 'healthy' if stats else 'unhealthy'
                else:
                    status = 'unknown'
                    latency = None
                
                health['nodes'][node['id']] = {
                    'status': status,
                    'type': node['type'],
                    'latency_ms': latency
                }
                
                self.node_status[node['id']] = status == 'healthy'
                
            except Exception as e:
                health['nodes'][node['id']] = {
                    'status': 'error',
                    'type': node['type'],
                    'error': str(e)
                }
                self.node_status[node['id']] = False
        
        return health
    
    def _measure_latency(self, node: Dict[str, Any]) -> float:
        """Mide la latencia de un nodo.
        
        Args:
            node: Nodo
            
        Returns:
            float: Latencia en milisegundos
        """
        start = time.time()
        
        if node['type'] == 'redis':
            node['client'].ping()
        elif node['type'] == 'memcached':
            node['client'].get_stats()
        
        return (time.time() - start) * 1000  # Convertir a ms
    
    def rebalance(self) -> bool:
        """Rebalancea la caché.
        
        Returns:
            bool: True si el rebalanceo fue exitoso
        """
        try:
            # Crear plan de migración
            movements = self.partitioner.rebalance()
            if not movements:
                return True
            
            # Publicar evento de inicio
            self._publish_event(
                EventType.REBALANCE_START,
                metadata={
                    'movements': movements,
                    'node_count': len(self.nodes),
                    'timestamp': datetime.now().isoformat()
                },
                priority=EventPriority.MEDIUM
            )
            
            # Ejecutar migración
            tasks = self.migration_manager.create_migration_plan(movements)
            success = self.migration_manager.execute_migration(tasks)
            
            # Publicar evento de finalización
            if success:
                self._publish_event(
                    EventType.REBALANCE_COMPLETE,
                    metadata={
                        'success': True,
                        'tasks_completed': len(tasks),
                        'keys_moved': sum(len(task.keys) for task in tasks.values()),
                        'duration': str(datetime.now() - datetime.fromisoformat(tasks[0].timestamp))
                    },
                    priority=EventPriority.MEDIUM
                )
            else:
                self._publish_event(
                    EventType.REBALANCE_FAILED,
                    message="Migration failed during rebalancing",
                    metadata={
                        'tasks_attempted': len(tasks),
                        'tasks_completed': sum(1 for task in tasks.values() if task.status == 'completed'),
                        'tasks_failed': sum(1 for task in tasks.values() if task.status == 'failed')
                    },
                    priority=EventPriority.HIGH
                )
            
            # Registrar métricas
            if self.metrics:
                if success:
                    self.metrics.record_rebalancing(
                        success=True,
                        moved_keys=sum(len(task.keys) for task in tasks.values())
                    )
                else:
                    self.metrics.record_rebalancing(
                        success=False,
                        error="Migration failed during rebalancing"
                    )
            
            return success
            
        except Exception as e:
            logger.error(f"Error during rebalancing: {str(e)}")
            if self.metrics:
                self.metrics.record_rebalancing(
                    success=False,
                    error=str(e)
                )
            return False

    def __del__(self) -> None:
        """Limpieza al destruir la instancia."""
        self.close()

    def close(self) -> None:
        """Cierra todas las conexiones y recursos."""
        try:
            # Detener componentes
            self.event_manager.stop()
            self.backup_manager.stop()
            self.priority_manager.stop()
            self.cleaner.stop()
            
            # Cerrar conexiones
            for node in self.nodes:
                try:
                    if 'client' in node:
                        if node['type'] == 'redis':
                            node['client'].close()
                        elif node['type'] == 'memcached':
                            node['client'].disconnect_all()
                except Exception as e:
                    logger.error(f"Error closing connection to {node['id']}: {str(e)}")
            
            # Limpiar estado interno
            self.nodes.clear()
            self.node_status.clear()
            self.circuit_breakers.clear()
            
            logger.info("Distributed cache closed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    def __enter__(self) -> 'DistributedCache':
        """Soporte para context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Limpieza al salir del context manager."""
        self.close() 