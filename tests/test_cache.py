"""Pruebas para el sistema de caché distribuida."""

import pytest
import time
from unittest.mock import Mock, patch
import json
import os
from typing import Dict, Any
import base64
from datetime import datetime, timedelta

from scraper.cache import (
    DistributedCache,
    Priority,
    Permission,
    Role
)
from scraper.config import CacheConfig
from scraper.metrics import MetricsManager
from scraper.exceptions import CacheError
from scraper.cache.partitioner import ConsistentHashPartitioner, RangePartitioner
from scraper.cache.compression import CompressionManager
from scraper.cache.migration import MigrationManager, MigrationTask

# Configuración de prueba
TEST_NODES = [
    {
        'id': 'redis-1',
        'type': 'redis',
        'host': 'localhost',
        'port': 6379
    },
    {
        'id': 'redis-2',
        'type': 'redis',
        'host': 'localhost',
        'port': 6380
    },
    {
        'id': 'memcached-1',
        'type': 'memcached',
        'host': 'localhost',
        'port': 11211
    }
]

@pytest.fixture
def mock_redis():
    """Mock para cliente Redis."""
    with patch('redis.Redis') as mock:
        client = Mock()
        mock.return_value = client
        yield client

@pytest.fixture
def mock_memcached():
    """Mock para cliente Memcached."""
    with patch('memcache.Client') as mock:
        client = Mock()
        mock.return_value = client
        yield client

@pytest.fixture
def metrics():
    """Fixture para métricas."""
    return MetricsManager()

@pytest.fixture
def config():
    """Fixture para configuración."""
    return CacheConfig()

@pytest.fixture
def auth_config(tmp_path):
    """Fixture para configuración de autenticación."""
    config_file = tmp_path / "auth.json"
    return str(config_file)

@pytest.fixture
def cache(mock_redis, mock_memcached, metrics, config):
    """Fixture para caché distribuida."""
    return DistributedCache(
        nodes=TEST_NODES,
        metrics=metrics,
        replication_factor=2,
        consistency_level='quorum',
        partitioning_strategy='consistent_hash',
        compression_algorithm='zlib'
    )

@pytest.fixture
def cache_with_auth(mock_redis, mock_memcached, metrics, auth_config):
    """Fixture para caché con autenticación."""
    return DistributedCache(
        nodes=TEST_NODES,
        metrics=metrics,
        auth_file=auth_config,
        encryption_key="test_key"
    )

def test_cache_initialization(cache):
    """Prueba la inicialización de la caché."""
    assert len(cache.nodes) == len(TEST_NODES)
    assert cache.replication_factor == 2
    assert cache.consistency_level == 'quorum'
    assert isinstance(cache.partitioner, ConsistentHashPartitioner)
    assert isinstance(cache.compression, CompressionManager)

def test_get_set_operations(cache, mock_redis):
    """Prueba operaciones básicas de get/set."""
    # Configurar mock
    mock_redis.get.return_value = None
    mock_redis.set.return_value = True
    
    # Probar set
    cache.set('test_key', 'test_value')
    mock_redis.set.assert_called()
    
    # Probar get
    value = cache.get('test_key')
    mock_redis.get.assert_called_with('test_key')
    assert value is None  # None porque el mock retorna None

def test_delete_operation(cache, mock_redis):
    """Prueba operación de delete."""
    mock_redis.delete.return_value = 1
    
    cache.delete('test_key')
    mock_redis.delete.assert_called_with('test_key')

def test_clear_operation(cache, mock_redis):
    """Prueba operación de clear."""
    mock_redis.flushdb.return_value = True
    
    cache.clear()
    mock_redis.flushdb.assert_called()

def test_compression(cache):
    """Prueba compresión de datos."""
    # Probar compresión de texto
    text_data = 'x' * 2000
    compressed = cache.compression.compress('test.txt', text_data)
    assert compressed is not None
    assert len(compressed) < len(text_data)
    decompressed = cache.compression.decompress(compressed)
    assert decompressed == text_data
    
    # Probar compresión de JSON
    json_data = {'data': 'x' * 2000}
    compressed = cache.compression.compress('test.json', json_data)
    assert compressed is not None
    decompressed = cache.compression.decompress(compressed)
    assert decompressed == json_data
    
    # Probar datos pequeños (no deberían comprimirse)
    small_data = 'x' * 100
    compressed = cache.compression.compress('small.txt', small_data)
    assert compressed is None
    
    # Probar compresión forzada
    compressed = cache.compression.compress('small.txt', small_data, force=True)
    assert compressed is not None
    decompressed = cache.compression.decompress(compressed)
    assert decompressed == small_data

def test_partitioning_consistent_hash(cache):
    """Prueba particionamiento con consistent hashing."""
    # Obtener partición para una clave
    partition = cache.partitioner.get_partition('test_key')
    
    assert partition.node_id in [node['id'] for node in TEST_NODES]
    assert len(partition.replica_nodes) == min(2, len(TEST_NODES) - 1)

def test_partitioning_range():
    """Prueba particionamiento por rangos."""
    partitioner = RangePartitioner(TEST_NODES, num_partitions=1024)
    
    # Verificar distribución de particiones
    partition = partitioner.get_partition('test_key')
    
    assert partition.node_id in [node['id'] for node in TEST_NODES]
    assert len(partition.replica_nodes) == min(2, len(TEST_NODES) - 1)

def test_node_failure_handling(cache, mock_redis):
    """Prueba manejo de fallos de nodos."""
    # Simular fallo en nodo primario
    mock_redis.get.side_effect = Exception("Connection error")
    
    # La operación debería intentar con réplicas
    value = cache.get('test_key')
    assert value is None
    
    # Verificar que el nodo se marcó como no saludable
    assert not cache.node_status[TEST_NODES[0]['id']]

def test_consistency_levels(cache, mock_redis):
    """Prueba diferentes niveles de consistencia."""
    # Probar quorum
    cache.consistency_level = 'quorum'
    assert cache._get_required_reads() == 2
    assert cache._get_required_writes() == 2
    
    # Probar one
    cache.consistency_level = 'one'
    assert cache._get_required_reads() == 1
    assert cache._get_required_writes() == 1
    
    # Probar all
    cache.consistency_level = 'all'
    assert cache._get_required_reads() == cache.replication_factor
    assert cache._get_required_writes() == cache.replication_factor

def test_metrics_integration(cache, metrics, mock_redis):
    """Prueba integración con métricas."""
    # Simular hit
    mock_redis.get.return_value = b'test_value'
    cache.get('test_key')
    assert metrics.cache_hits == 1
    
    # Simular miss
    mock_redis.get.return_value = None
    cache.get('test_key')
    assert metrics.cache_misses == 1

def test_health_check(cache, mock_redis):
    """Prueba verificación de salud."""
    # Simular nodo saludable
    mock_redis.ping.return_value = True
    
    health = cache.check_health()
    assert health['total_nodes'] == len(TEST_NODES)
    assert health['healthy_nodes'] > 0
    assert 'nodes' in health

def test_rebalancing(cache):
    """Prueba rebalanceo de caché."""
    # Obtener estado inicial
    initial_partition = cache.partitioner.get_partition('test_key')
    
    # Simular rebalanceo
    movements = cache.partitioner.rebalance()
    
    # Verificar que hay información de movimientos
    assert isinstance(movements, dict)

def test_error_handling(cache, mock_redis):
    """Prueba manejo de errores."""
    # Simular error en operación
    mock_redis.set.side_effect = Exception("Test error")
    
    with pytest.raises(CacheError):
        cache.set('test_key', 'test_value')

def test_ttl_support(cache, mock_redis):
    """Prueba soporte de TTL."""
    cache.set('test_key', 'test_value', ttl=60)
    mock_redis.setex.assert_called_with('test_key', 60, b'test_value')

def test_compression_stats(cache):
    """Prueba estadísticas de compresión."""
    # Comprimir diferentes tipos de datos
    text_data = 'x' * 5000
    json_data = {'data': 'x' * 5000}
    binary_data = b'x' * 5000
    
    cache.compression.compress('test.txt', text_data)
    cache.compression.compress('test.json', json_data)
    cache.compression.compress('test.bin', binary_data)
    
    # Verificar estadísticas individuales
    text_stats = cache.compression.get_stats('test.txt')
    assert text_stats.algorithm == 'zlib'
    assert text_stats.ratio < 1.0
    assert text_stats.mime_type == 'text/plain'
    
    json_stats = cache.compression.get_stats('test.json')
    assert json_stats.algorithm == 'zlib'
    assert json_stats.ratio < 1.0
    assert json_stats.mime_type == 'application/json'
    
    binary_stats = cache.compression.get_stats('test.bin')
    assert binary_stats.algorithm == 'lz4'
    assert binary_stats.ratio < 1.0
    assert binary_stats.mime_type == 'application/octet-stream'
    
    # Verificar estadísticas globales
    global_stats = cache.compression.get_stats()
    assert isinstance(global_stats, dict)
    assert global_stats['total_original_mb'] > 0
    assert global_stats['total_compressed_mb'] > 0
    assert global_stats['space_saved_mb'] > 0
    assert 0 < global_stats['compression_ratio'] < 1.0

def test_config_validation(config):
    """Prueba validación de configuración."""
    invalid_config = {
        'partitioning': {
            'strategy': 'invalid'
        }
    }
    
    # La validación debería mantener la configuración por defecto
    validated = config._validate_config(invalid_config)
    assert validated['partitioning']['strategy'] == 'consistent_hash'

def test_node_management(config):
    """Prueba gestión de nodos."""
    # Añadir nodo
    new_node = {
        'id': 'redis-3',
        'type': 'redis',
        'host': 'localhost',
        'port': 6381
    }
    
    assert config.add_node(new_node)
    assert len(config.get_all_nodes()) == len(DEFAULT_CONFIG['nodes']) + 1
    
    # Eliminar nodo
    assert config.remove_node('redis-3')
    assert len(config.get_all_nodes()) == len(DEFAULT_CONFIG['nodes'])

def test_migration_plan_creation(cache):
    """Prueba creación de plan de migración."""
    migration = MigrationManager(cache)
    
    # Simular movimientos
    movements = {
        'redis-1': ['redis-2'],
        'redis-2': ['memcached-1']
    }
    
    # Crear plan
    tasks = migration.create_migration_plan(movements)
    
    assert len(tasks) == 2
    assert all(isinstance(task, MigrationTask) for task in tasks.values())
    assert all(task.status == 'pending' for task in tasks.values())

def test_migration_execution(cache, mock_redis):
    """Prueba ejecución de migración."""
    migration = MigrationManager(cache)
    
    # Configurar mocks
    mock_redis.scan.return_value = (0, [b'key1', b'key2'])
    mock_redis.get.return_value = b'value'
    mock_redis.ttl.return_value = 100
    
    # Crear y ejecutar plan
    tasks = {
        'test-migration': MigrationTask(
            source_node='redis-1',
            target_node='redis-2',
            keys={'key1', 'key2'}
        )
    }
    
    success = migration.execute_migration(tasks)
    assert success
    assert tasks['test-migration'].status == 'completed'
    assert tasks['test-migration'].progress == 1.0

def test_migration_failure_handling(cache, mock_redis):
    """Prueba manejo de fallos en migración."""
    migration = MigrationManager(cache)
    
    # Simular error
    mock_redis.get.side_effect = Exception("Test error")
    
    tasks = {
        'test-migration': MigrationTask(
            source_node='redis-1',
            target_node='redis-2',
            keys={'key1'}
        )
    }
    
    success = migration.execute_migration(tasks)
    assert not success
    assert tasks['test-migration'].status == 'failed'
    assert tasks['test-migration'].error is not None

def test_migration_metrics(cache, metrics, mock_redis):
    """Prueba métricas de migración."""
    migration = MigrationManager(cache, metrics=metrics)
    
    # Configurar mock
    mock_redis.scan.return_value = (0, [b'key1'])
    mock_redis.get.return_value = b'value'
    
    # Ejecutar migración exitosa
    tasks = {
        'success-task': MigrationTask(
            source_node='redis-1',
            target_node='redis-2',
            keys={'key1'}
        )
    }
    migration.execute_migration(tasks)
    
    assert metrics.migration_stats['successful_migrations'] == 1
    assert metrics.migration_stats['failed_migrations'] == 0
    
    # Simular fallo
    mock_redis.get.side_effect = Exception("Test error")
    tasks = {
        'fail-task': MigrationTask(
            source_node='redis-1',
            target_node='redis-2',
            keys={'key1'}
        )
    }
    migration.execute_migration(tasks)
    
    assert metrics.migration_stats['successful_migrations'] == 1
    assert metrics.migration_stats['failed_migrations'] == 1

def test_rebalancing_with_migration(cache, metrics, mock_redis):
    """Prueba rebalanceo completo con migración."""
    # Configurar mocks
    mock_redis.scan.return_value = (0, [b'key1'])
    mock_redis.get.return_value = b'value'
    mock_redis.ttl.return_value = 100
    
    # Ejecutar rebalanceo
    success = cache.rebalance()
    assert success
    
    # Verificar métricas
    assert metrics.migration_stats['total_rebalancing_ops'] == 1
    assert metrics.migration_stats['successful_rebalancing'] == 1

def test_rebalancing_failure(cache, metrics, mock_redis):
    """Prueba manejo de fallos en rebalanceo."""
    # Simular error en scan
    mock_redis.scan.side_effect = Exception("Test error")
    
    # Ejecutar rebalanceo
    success = cache.rebalance()
    assert not success
    
    # Verificar métricas
    assert metrics.migration_stats['failed_rebalancing'] == 1
    assert any(
        'rebalancing_error' in error
        for error in metrics.error_counts.keys()
    )

def test_migration_status(cache, mock_redis):
    """Prueba obtención de estado de migración."""
    migration = MigrationManager(cache)
    
    # Configurar mock
    mock_redis.scan.return_value = (0, [b'key1'])
    mock_redis.get.return_value = b'value'
    
    # Ejecutar migración
    tasks = {
        'test-task': MigrationTask(
            source_node='redis-1',
            target_node='redis-2',
            keys={'key1'}
        )
    }
    migration.execute_migration(tasks)
    
    # Verificar estado
    status = migration.get_migration_status()
    assert status['tasks'] == 1
    assert status['completed'] == 1
    assert status['failed'] == 0
    assert status['total_progress'] == 1.0

def test_batch_migration(cache, mock_redis):
    """Prueba migración por lotes."""
    migration = MigrationManager(cache, batch_size=2)
    
    # Crear conjunto grande de claves
    keys = {f'key{i}' for i in range(5)}
    
    # Configurar mock
    mock_redis.scan.return_value = (0, [b'key1'])
    mock_redis.get.return_value = b'value'
    
    # Ejecutar migración
    tasks = {
        'test-task': MigrationTask(
            source_node='redis-1',
            target_node='redis-2',
            keys=keys
        )
    }
    success = migration.execute_migration(tasks)
    
    assert success
    assert tasks['test-task'].status == 'completed'
    assert mock_redis.get.call_count == len(keys)

def test_concurrent_migrations(cache, mock_redis):
    """Prueba migraciones concurrentes."""
    migration = MigrationManager(cache, max_workers=2)
    
    # Configurar mock
    mock_redis.scan.return_value = (0, [b'key1'])
    mock_redis.get.return_value = b'value'
    
    # Crear múltiples tareas
    tasks = {
        f'task-{i}': MigrationTask(
            source_node='redis-1',
            target_node='redis-2',
            keys={'key1'}
        )
        for i in range(3)
    }
    
    success = migration.execute_migration(tasks)
    assert success
    assert all(task.status == 'completed' for task in tasks.values())

def test_auth_initialization(cache_with_auth, auth_config):
    """Prueba inicialización de autenticación."""
    assert os.path.exists(auth_config)
    assert cache_with_auth.auth.users
    assert 'admin' in cache_with_auth.auth.users

def test_user_creation(cache_with_auth):
    """Prueba creación de usuarios."""
    cache_with_auth.auth.create_user(
        username="test_user",
        password="test_pass",
        role=Role.READER
    )
    
    assert "test_user" in cache_with_auth.auth.users
    assert cache_with_auth.auth.users["test_user"]["role"] == Role.READER

def test_authentication(cache_with_auth):
    """Prueba autenticación de usuarios."""
    # Crear usuario
    cache_with_auth.auth.create_user(
        username="test_user",
        password="test_pass",
        role=Role.READER
    )
    
    # Autenticar
    token = cache_with_auth.auth.authenticate(
        username="test_user",
        password="test_pass"
    )
    
    assert token is not None
    assert cache_with_auth.auth.validate_token(token)

def test_unauthorized_access(cache_with_auth, mock_redis):
    """Prueba acceso no autorizado."""
    # Crear usuario sin permisos de escritura
    cache_with_auth.auth.create_user(
        username="reader",
        password="pass",
        role=Role.READER
    )
    
    # Autenticar
    token = cache_with_auth.auth.authenticate("reader", "pass")
    
    # Intentar escribir
    with pytest.raises(CacheError, match="Unauthorized access"):
        cache_with_auth.set("test_key", "test_value", token=token)

def test_encryption(cache_with_auth, mock_redis):
    """Prueba encriptación de datos."""
    # Escribir valor sensible
    cache_with_auth.set("password", "secret123")
    
    # Verificar que está encriptado
    assert cache_with_auth.encryption.is_encrypted("password")
    
    # Leer valor
    value = cache_with_auth.get("password")
    assert value == "secret123"

def test_compression_with_encryption(cache_with_auth, mock_redis):
    """Prueba compresión con encriptación."""
    # Crear datos grandes
    large_data = "x" * 1000
    
    # Escribir con encriptación
    cache_with_auth.set("large_secret", large_data)
    
    # Verificar que está encriptado
    assert cache_with_auth.encryption.is_encrypted("large_secret")
    
    # Leer y verificar
    value = cache_with_auth.get("large_secret")
    assert value == large_data

def test_permission_inheritance(cache_with_auth):
    """Prueba herencia de permisos."""
    # Crear usuarios con diferentes roles
    cache_with_auth.auth.create_user(
        username="admin",
        password="admin",
        role=Role.ADMIN
    )
    cache_with_auth.auth.create_user(
        username="writer",
        password="writer",
        role=Role.WRITER
    )
    cache_with_auth.auth.create_user(
        username="reader",
        password="reader",
        role=Role.READER
    )
    
    # Obtener tokens
    admin_token = cache_with_auth.auth.authenticate("admin", "admin")
    writer_token = cache_with_auth.auth.authenticate("writer", "writer")
    reader_token = cache_with_auth.auth.authenticate("reader", "reader")
    
    # Verificar permisos
    assert cache_with_auth.auth.has_permission(admin_token, Permission.ADMIN)
    assert cache_with_auth.auth.has_permission(admin_token, Permission.WRITE)
    assert cache_with_auth.auth.has_permission(admin_token, Permission.READ)
    
    assert not cache_with_auth.auth.has_permission(writer_token, Permission.ADMIN)
    assert cache_with_auth.auth.has_permission(writer_token, Permission.WRITE)
    assert cache_with_auth.auth.has_permission(writer_token, Permission.READ)
    
    assert not cache_with_auth.auth.has_permission(reader_token, Permission.ADMIN)
    assert not cache_with_auth.auth.has_permission(reader_token, Permission.WRITE)
    assert cache_with_auth.auth.has_permission(reader_token, Permission.READ)

def test_failed_login_lockout(cache_with_auth):
    """Prueba bloqueo por intentos fallidos."""
    # Crear usuario
    cache_with_auth.auth.create_user(
        username="test_user",
        password="correct_pass"
    )
    
    # Intentar login con contraseña incorrecta
    for _ in range(5):
        token = cache_with_auth.auth.authenticate(
            "test_user",
            "wrong_pass"
        )
        assert token is None
    
    # Verificar bloqueo
    assert cache_with_auth.auth._is_locked_out("test_user")
    
    # Intentar login con contraseña correcta
    token = cache_with_auth.auth.authenticate(
        "test_user",
        "correct_pass"
    )
    assert token is None  # Debería fallar por bloqueo

def test_token_expiration(cache_with_auth):
    """Prueba expiración de tokens."""
    # Crear usuario
    cache_with_auth.auth.create_user(
        username="test_user",
        password="test_pass"
    )
    
    # Autenticar
    token = cache_with_auth.auth.authenticate(
        "test_user",
        "test_pass"
    )
    
    # Modificar tiempo de expiración
    cache_with_auth.auth.tokens[token]['expires_at'] = (
        datetime.now() - timedelta(seconds=1)
    )
    
    # Verificar token expirado
    assert not cache_with_auth.auth.validate_token(token)

def test_encryption_key_rotation(cache_with_auth, mock_redis):
    """Prueba rotación de claves de encriptación."""
    # Escribir valor
    cache_with_auth.set("secret", "value")
    
    # Rotar clave
    old_key = cache_with_auth.encryption.secret_key
    cache_with_auth.encryption.rotate_key()
    
    # Verificar que la clave cambió
    assert cache_with_auth.encryption.secret_key != old_key
    
    # Verificar que aún podemos leer el valor
    assert cache_with_auth.get("secret") == "value"

def test_sensitive_data_detection(cache_with_auth):
    """Prueba detección de datos sensibles."""
    # Probar diferentes claves
    sensitive_keys = [
        "password",
        "secret_key",
        "auth_token",
        "private_key",
        "credentials"
    ]
    
    for key in sensitive_keys:
        assert cache_with_auth.encryption.should_encrypt(key, "value")
    
    # Probar claves no sensibles
    normal_keys = [
        "name",
        "age",
        "address",
        "phone"
    ]
    
    for key in normal_keys:
        assert not cache_with_auth.encryption.should_encrypt(key, "value")

def test_compression_config():
    """Test compression configuration."""
    # Test valid configuration
    config = CompressionConfig(
        enabled=True,
        algorithm='zlib',
        min_size=1024,
        max_size=10 * 1024 * 1024,
        threshold=0.8,
        level=6
    )
    assert config.algorithm == 'zlib'
    assert config.threshold == 0.8
    
    # Test algorithm validation
    with pytest.raises(ValueError) as exc:
        CompressionConfig(algorithm='invalid')
    assert "algorithm must be one of" in str(exc.value)
    
    # Test level validation
    with pytest.raises(ValueError) as exc:
        CompressionConfig(algorithm='zlib', level=10)
    assert "level for zlib must be between 1 and 9" in str(exc.value)
    
    with pytest.raises(ValueError) as exc:
        CompressionConfig(algorithm='lz4', level=20)
    assert "level for lz4 must be between 1 and 16" in str(exc.value)
    
    # Test threshold validation
    with pytest.raises(ValueError) as exc:
        CompressionConfig(threshold=1.5)
    assert "threshold must be between 0.0 and 1.0" in str(exc.value)
    
    # Test size validation
    with pytest.raises(ValueError) as exc:
        CompressionConfig(min_size=0)
    assert "min_size must be greater than 0" in str(exc.value)
    
    with pytest.raises(ValueError) as exc:
        CompressionConfig(min_size=2048, max_size=1024)
    assert "max_size must be greater than min_size" in str(exc.value) 