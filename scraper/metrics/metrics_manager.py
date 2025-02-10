"""Sistema unificado de métricas."""

import time
import psutil
import logging
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
from pathlib import Path
import threading
from collections import deque
import atexit
import os
from prometheus_client import (
    start_http_server,
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
    CollectorRegistry
)
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily

from ..settings import Settings
from ..logging_config import (
    CompressedRotatingFileHandler,
    get_logger,
    JsonFormatter
)

logger = logging.getLogger(__name__)

@dataclass
class SystemStats:
    """Estadísticas del sistema."""
    cpu_percent: float
    memory_stats: Dict[str, float]
    io_stats: Dict[str, int]
    network_stats: Dict[str, float]
    uptime_seconds: float

    @classmethod
    def collect(cls) -> 'SystemStats':
        """Recolecta estadísticas del sistema."""
        try:
            # CPU y memoria base
            cpu_percent = psutil.cpu_percent(interval=1)
            process = psutil.Process()
            memory_info = process.memory_info()
            
            # Estadísticas detalladas de memoria
            memory_stats = {
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024,
                'shared_mb': memory_info.shared / 1024 / 1024,
                'text_mb': memory_info.text / 1024 / 1024,
                'lib_mb': memory_info.lib / 1024 / 1024,
                'data_mb': memory_info.data / 1024 / 1024,
                'dirty_mb': memory_info.dirty / 1024 / 1024
            }
            
            # Estadísticas de I/O
            io_counters = process.io_counters()
            io_stats = {
                'read_bytes': io_counters.read_bytes,
                'write_bytes': io_counters.write_bytes,
                'read_count': io_counters.read_count,
                'write_count': io_counters.write_count
            }
            
            # Estadísticas de red
            net_io = psutil.net_io_counters()
            network_stats = {
                'bytes_sent': net_io.bytes_sent,
                'bytes_recv': net_io.bytes_recv,
                'packets_sent': net_io.packets_sent,
                'packets_recv': net_io.packets_recv,
                'errin': net_io.errin,
                'errout': net_io.errout,
                'dropin': net_io.dropin,
                'dropout': net_io.dropout
            }
            
            return cls(
                cpu_percent=cpu_percent,
                memory_stats=memory_stats,
                io_stats=io_stats,
                network_stats=network_stats,
                uptime_seconds=time.time() - process.create_time()
            )
            
        except Exception as e:
            logger.error(f"Error collecting system stats: {str(e)}")
            return cls(
                cpu_percent=0.0,
                memory_stats={'error': 'Failed to collect'},
                io_stats={'error': 'Failed to collect'},
                network_stats={'error': 'Failed to collect'},
                uptime_seconds=0.0
            )

@dataclass
class MetricsReport:
    """Reporte de métricas."""
    system_stats: SystemStats
    database: Dict[str, int]
    cache: Dict[str, int]
    errors: Dict[str, int]
    timestamp: str

@dataclass
class MetricsBuffer:
    """Circular buffer for metrics."""
    max_size: int
    data: deque = field(default_factory=deque)
    
    def __post_init__(self):
        """Post-initialization validation."""
        if self.max_size < 1:
            raise ValueError("Buffer size must be positive")
        if not isinstance(self.data, deque):
            raise TypeError("Data must be a deque")
    
    def add(self, item: Any) -> None:
        """Add an item to the buffer."""
        if len(self.data) >= self.max_size:
            self.data.popleft()
        self.data.append(item)
    
    def clear(self) -> None:
        """Clear the buffer."""
        self.data.clear()
    
    def get_all(self) -> List[Any]:
        """Get all items."""
        return list(self.data)

class MetricsCollector:
    """Custom metrics collector for Prometheus."""
    
    def __init__(self, metrics_manager: 'MetricsManager'):
        """Initialize collector.
        
        Args:
            metrics_manager: Metrics manager instance
        """
        self.metrics = metrics_manager
    
    def collect(self):
        """Collect metrics for Prometheus."""
        # Performance metrics
        performance = self.metrics.get_performance_metrics()
        
        cpu_metric = GaugeMetricFamily(
            'scraper_cpu_usage_percent',
            'CPU usage percentage',
            value=performance['cpu_percent']
        )
        yield cpu_metric
        
        memory_metric = GaugeMetricFamily(
            'scraper_memory_usage_mb',
            'Memory usage in MB',
            value=performance['memory_mb']
        )
        yield memory_metric
        
        uptime_metric = CounterMetricFamily(
            'scraper_uptime_seconds',
            'Uptime in seconds',
            value=performance['uptime_seconds']
        )
        yield uptime_metric
        
        # Cache metrics
        cache_hits = CounterMetricFamily(
            'scraper_cache_hits_total',
            'Total number of cache hits'
        )
        cache_hits.add_metric([], self.metrics.cache_hits)
        yield cache_hits
        
        cache_misses = CounterMetricFamily(
            'scraper_cache_misses_total',
            'Total number of cache misses'
        )
        cache_misses.add_metric([], self.metrics.cache_misses)
        yield cache_misses
        
        # Database metrics
        db_connections = GaugeMetricFamily(
            'scraper_db_connections',
            'Number of active database connections'
        )
        db_connections.add_metric([], self.metrics.db_connections)
        yield db_connections
        
        # Error metrics
        for error_type, count in self.metrics.error_counts.items():
            error_metric = CounterMetricFamily(
                'scraper_errors_total',
                'Total number of errors by type',
                labels=['type']
            )
            error_metric.add_metric([error_type], count)
            yield error_metric

class MetricsManager:
    """Gestor unificado de métricas."""
    
    # Prefijos estándar para métricas
    METRIC_PREFIXES = {
        'cache': 'cache',
        'database': 'db',
        'system': 'sys',
        'network': 'net',
        'error': 'error'
    }
    
    def __init__(
        self,
        settings: Optional[Settings] = None,
        metrics_port: int = 9090,
        metrics_addr: str = '0.0.0.0'
    ):
        """Inicializa el gestor de métricas.
        
        Args:
            settings: Configuración opcional
            metrics_port: Puerto para métricas Prometheus
            metrics_addr: Dirección para métricas Prometheus
        """
        self.logger = logging.getLogger(__name__)
        self.settings = settings
        self.start_time = time.time()
        self.error_counts: Dict[str, int] = {}
        self.cache_hits = 0
        self.cache_misses = 0
        self.db_connections = 0
        
        # Configuración Prometheus
        self.registry = CollectorRegistry()
        self.registry.register(MetricsCollector(self))
        self.metrics_port = metrics_port
        self.metrics_addr = metrics_addr
        self.metrics_server = None
        
        # Estadísticas de migración
        self.migration_stats = {
            'total_migrations': 0,
            'successful_migrations': 0,
            'failed_migrations': 0,
            'total_keys_moved': 0,
            'total_rebalancing_ops': 0,
            'successful_rebalancing': 0,
            'failed_rebalancing': 0
        }
        
        # Configurar directorio de métricas
        self.metrics_dir = Path('metrics')
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        
        # Registrar limpieza
        atexit.register(self.cleanup)

    def start(self) -> None:
        """Start metrics collection and server."""
        try:
            start_http_server(
                port=self.metrics_port,
                addr=self.metrics_addr,
                registry=self.registry
            )
            self.logger.info(
                f"Metrics server started on {self.metrics_addr}:{self.metrics_port}"
            )
        except Exception as e:
            self.logger.error(f"Failed to start metrics server: {str(e)}")

    def stop(self) -> None:
        """Stop metrics collection."""
        self.cleanup()
        self.logger.info("Metrics collection stopped")

    def cleanup(self) -> None:
        """Cleanup resources."""
        self._save_final_report()
        self._cleanup_old_logs()

    def record_custom_metric(
        self,
        name: str,
        value: Union[int, float],
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Registra una métrica personalizada.
        
        Args:
            name: Nombre de la métrica
            value: Valor de la métrica
            labels: Etiquetas opcionales
        """
        # Estandarizar nombre de métrica
        parts = name.split('.')
        if len(parts) > 1 and parts[0] in self.METRIC_PREFIXES:
            prefix = self.METRIC_PREFIXES[parts[0]]
            name = f"{prefix}_{'.'.join(parts[1:])}"
        
        # Registrar en Prometheus
        metric = Gauge(name, name, labelnames=list(labels.keys()) if labels else [])
        metric.labels(**labels).set(value) if labels else metric.set(value)
    
    def record_error(self, error_type: str) -> None:
        """Registra un error."""
        error_name = f"error_{error_type.lower()}"
        self.error_counts[error_name] = self.error_counts.get(error_name, 0) + 1
        self.logger.debug(f"Recorded error of type: {error_type}")
    
    def record_cache_hit(self) -> None:
        """Registra un hit de caché."""
        self.cache_hits += 1
        self.record_custom_metric('cache.hits', self.cache_hits)
    
    def record_cache_miss(self) -> None:
        """Registra un miss de caché."""
        self.cache_misses += 1
        self.record_custom_metric('cache.misses', self.cache_misses)
    
    def record_db_connection(self) -> None:
        """Registra una conexión a base de datos."""
        self.db_connections += 1
        self.record_custom_metric('db.connections', self.db_connections)
    
    def record_migration_success(self, task_id: str) -> None:
        """Registra una migración exitosa."""
        self.migration_stats['total_migrations'] += 1
        self.migration_stats['successful_migrations'] += 1
        self.record_custom_metric(
            'cache.migration.success',
            self.migration_stats['successful_migrations'],
            {'task_id': task_id}
        )
    
    def record_migration_failure(self, task_id: str) -> None:
        """Registra una migración fallida."""
        self.migration_stats['total_migrations'] += 1
        self.migration_stats['failed_migrations'] += 1
        self.record_custom_metric(
            'cache.migration.failure',
            self.migration_stats['failed_migrations'],
            {'task_id': task_id}
        )
    
    def record_rebalancing(
        self,
        success: bool,
        moved_keys: Optional[int] = None,
        failed_tasks: Optional[int] = None,
        error: Optional[str] = None
    ) -> None:
        """Registra una operación de rebalanceo."""
        self.migration_stats['total_rebalancing_ops'] += 1
        
        if success:
            self.migration_stats['successful_rebalancing'] += 1
            if moved_keys:
                self.migration_stats['total_keys_moved'] += moved_keys
                self.record_custom_metric(
                    'cache.rebalancing.keys_moved',
                    moved_keys
                )
        else:
            self.migration_stats['failed_rebalancing'] += 1
            if error:
                self.record_error(f"rebalancing_error: {error}")
        
        self.record_custom_metric(
            'cache.rebalancing.success',
            self.migration_stats['successful_rebalancing']
        )
        self.record_custom_metric(
            'cache.rebalancing.failure',
            self.migration_stats['failed_rebalancing']
        )

    def get_performance_metrics(self) -> Dict[str, float]:
        """Get system performance metrics."""
        try:
            stats = SystemStats.collect()
            
            # Check resource thresholds
            memory_percent = (stats.memory_stats['rss_mb'] / 
                            (psutil.virtual_memory().total / 1024 / 1024)) * 100
            disk_usage = psutil.disk_usage('/').percent
            
            # Resource alerts
            if memory_percent > 85:
                self.logger.warning(f"High memory usage: {memory_percent:.1f}%")
            
            if disk_usage > 90:
                self.logger.warning(f"High disk usage: {disk_usage:.1f}%")
            
            if stats.cpu_percent > 80:
                self.logger.warning(f"High CPU usage: {stats.cpu_percent:.1f}%")
            
            return {
                'cpu_percent': stats.cpu_percent,
                'memory_mb': stats.memory_stats['rss_mb'],
                'memory_percent': memory_percent,
                'disk_usage_percent': disk_usage,
                'uptime_seconds': stats.uptime_seconds,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error getting performance metrics: {str(e)}")
            return {
                'cpu_percent': 0.0,
                'memory_mb': 0.0,
                'memory_percent': 0.0,
                'disk_usage_percent': 0.0,
                'uptime_seconds': 0.0,
                'timestamp': datetime.now().isoformat()
            }

    def get_report(self) -> MetricsReport:
        """Generate a complete metrics report."""
        return MetricsReport(
            system_stats=SystemStats.collect(),
            database={'connections': self.db_connections},
            cache={
                'hits': self.cache_hits,
                'misses': self.cache_misses,
                'hit_ratio': self._calculate_hit_ratio(),
                **self.migration_stats
            },
            errors=self.error_counts,
            timestamp=datetime.now().isoformat()
        )

    def _calculate_hit_ratio(self) -> float:
        """Calculate cache hit ratio."""
        total = self.cache_hits + self.cache_misses
        if total == 0:
            return 0.0
        return round(self.cache_hits / total, 2)

    def _save_final_report(self) -> None:
        """Save final metrics report."""
        try:
            report = self.get_report()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = self.metrics_dir / f'metrics_report_{timestamp}.json'
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report.__dict__, f, indent=2, default=str)
                
            self.logger.info(f"Final metrics report saved to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving final metrics report: {str(e)}")

    def _cleanup_old_logs(self) -> None:
        """Clean up old metric logs."""
        try:
            cutoff = datetime.now() - timedelta(days=7)
            for file in self.metrics_dir.glob('*.json'):
                if file.stat().st_mtime < cutoff.timestamp():
                    file.unlink()
            self.logger.info("Old metric logs cleaned up")
        except Exception as e:
            self.logger.error(f"Error cleaning up old logs: {str(e)}")

    def reset_counters(self) -> None:
        """Reset all metric counters."""
        self.error_counts.clear()
        self.cache_hits = 0
        self.cache_misses = 0
        self.db_connections = 0
        self.migration_stats = {
            'total_migrations': 0,
            'successful_migrations': 0,
            'failed_migrations': 0,
            'total_keys_moved': 0,
            'total_rebalancing_ops': 0,
            'successful_rebalancing': 0,
            'failed_rebalancing': 0
        }
        self.logger.debug("Metrics counters reset") 