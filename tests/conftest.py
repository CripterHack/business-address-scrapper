"""Configuración de pruebas para el sistema de caché."""

import pytest
from unittest.mock import Mock, patch
import tempfile
import shutil
from pathlib import Path
import os
import json
import time
from typing import Dict, Any
import random

from scraper.cache import (
    DistributedCache,
    MetricsManager,
    CacheConfig,
    NodeConfig,
    RecoveryManager,
    CompressionManager
)

@pytest.fixture(scope="session")
def test_dir():
    """Directorio temporal para pruebas."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def mock_redis():
    """Mock de cliente Redis."""
    with patch('redis.Redis') as mock:
        client = Mock()
        client.ping.return_value = True
        client.get.return_value = None
        client.set.return_value = True
        client.delete.return_value = True
        client.keys.return_value = []
        client.info.return_value = {
            'used_memory': 1024,
            'connected_clients': 1
        }
        mock.return_value = client
        yield client

@pytest.fixture
def mock_memcached():
    """Mock de cliente Memcached."""
    with patch('memcache.Client') as mock:
        client = Mock()
        client.get.return_value = None
        client.set.return_value = True
        client.delete.return_value = True
        client.get_stats.return_value = [{
            'bytes': '1024',
            'curr_connections': '1'
        }]
        mock.return_value = client
        yield client

@pytest.fixture
def metrics(test_dir):
    """Gestor de métricas para pruebas."""
    metrics_dir = Path(test_dir) / 'metrics'
    metrics_dir.mkdir(exist_ok=True)
    return MetricsManager(
        log_dir=str(metrics_dir),
        max_size_mb=1,
        backup_count=2,
        metrics_interval=1
    )

@pytest.fixture
def config():
    """Configuración de caché para pruebas."""
    return CacheConfig(
        enabled=True,
        nodes=[
            NodeConfig(
                id="test-redis-1",
                type="redis",
                host="localhost",
                port=6379
            ),
            NodeConfig(
                id="test-redis-2",
                type="redis",
                host="localhost",
                port=6380
            )
        ],
        replication_factor=2,
        consistency_level="quorum",
        compression=CompressionConfig(
            enabled=True,
            algorithm="zlib",
            min_size=64,
            level=6
        ),
        security=SecurityConfig(
            encryption_enabled=True,
            encryption_key="test-key-123",
            max_failed_attempts=3
        ),
        metrics=MetricsConfig(
            enabled=True,
            collection_interval=1
        )
    )

@pytest.fixture
def auth_config(test_dir):
    """Configuración de autenticación para pruebas."""
    auth_file = Path(test_dir) / 'auth.json'
    auth_data = {
        'users': {
            'test_user': {
                'password_hash': 'hash123',
                'roles': ['admin']
            }
        },
        'roles': {
            'admin': ['read', 'write', 'delete']
        }
    }
    with open(auth_file, 'w') as f:
        json.dump(auth_data, f)
    return str(auth_file)

@pytest.fixture
def compression():
    """Gestor de compresión para pruebas."""
    return CompressionManager()

@pytest.fixture
def recovery(cache, config, metrics):
    """Gestor de recuperación para pruebas."""
    return RecoveryManager(
        cache=cache,
        config=config,
        metrics=metrics,
        check_interval=1
    )

@pytest.fixture
def cache(mock_redis, mock_memcached, metrics, config, compression):
    """Instancia de caché para pruebas."""
    return DistributedCache(
        config=config,
        metrics=metrics,
        compression=compression
    )

@pytest.fixture
def cache_with_auth(mock_redis, mock_memcached, metrics, config, auth_config, compression):
    """Instancia de caché con autenticación para pruebas."""
    config.security.auth_file = auth_config
    return DistributedCache(
        config=config,
        metrics=metrics,
        compression=compression
    )

@pytest.fixture
def sample_data() -> Dict[str, Any]:
    """Datos de ejemplo para pruebas."""
    return {
        'string': 'test_value',
        'number': 42,
        'dict': {'key': 'value'},
        'list': [1, 2, 3],
        'binary': b'binary_data',
        'large_string': 'x' * 1024
    }

@pytest.fixture
def error_generator():
    """Generador de errores para pruebas."""
    class ErrorGenerator:
        def network_error(self):
            raise ConnectionError("Network error")
            
        def timeout_error(self):
            raise TimeoutError("Operation timed out")
            
        def auth_error(self):
            raise PermissionError("Authentication failed")
            
        def data_error(self):
            raise ValueError("Invalid data")
    
    return ErrorGenerator()

@pytest.fixture
def performance_monitor():
    """Monitor de rendimiento para pruebas."""
    class PerformanceMonitor:
        def __init__(self):
            self.start_time = time.time()
            self.operations = []
        
        def record_operation(self, name: str, duration: float):
            self.operations.append({
                'name': name,
                'duration': duration,
                'timestamp': time.time()
            })
        
        def get_stats(self):
            return {
                'total_time': time.time() - self.start_time,
                'operation_count': len(self.operations),
                'average_duration': sum(op['duration'] for op in self.operations) / len(self.operations) if self.operations else 0
            }
    
    return PerformanceMonitor()

@pytest.fixture
def mock_network():
    """Simulador de red para pruebas."""
    class NetworkSimulator:
        def __init__(self):
            self.latency = 0.01
            self.packet_loss = 0.0
            self.bandwidth = float('inf')
        
        def simulate_request(self):
            if random.random() < self.packet_loss:
                raise ConnectionError("Simulated packet loss")
            time.sleep(self.latency)
        
        def set_conditions(self, latency: float, packet_loss: float, bandwidth: float):
            self.latency = latency
            self.packet_loss = packet_loss
            self.bandwidth = bandwidth
    
    return NetworkSimulator()