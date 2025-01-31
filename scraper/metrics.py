"""Metrics and monitoring module."""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, NoReturn
from contextlib import contextmanager

from prometheus_client import Counter, Gauge, Histogram, start_http_server, CollectorRegistry

logger = logging.getLogger(__name__)

MAX_COUNTER_VALUE = 2**53 - 1  # JavaScript max safe integer

@dataclass
class ScraperMetrics:
    """Metrics collector for the scraper."""
    
    registry: CollectorRegistry = field(default_factory=CollectorRegistry)
    requests_total: Counter = field(init=False)
    request_duration_seconds: Histogram = field(init=False)
    items_scraped: Counter = field(init=False)
    items_processed: Counter = field(init=False)
    errors_total: Counter = field(init=False)
    active_requests: Gauge = field(init=False)
    memory_usage_bytes: Gauge = field(init=False)
    cpu_usage_percent: Gauge = field(init=False)
    db_connections: Gauge = field(init=False)
    db_query_duration_seconds: Histogram = field(init=False)
    cache_hits: Counter = field(init=False)
    cache_misses: Counter = field(init=False)
    
    # Valores en memoria para las métricas
    _values: Dict[str, float] = field(default_factory=lambda: {
        'requests_total': 0,
        'active_requests': 0,
        'items_scraped': 0,
        'items_processed': 0,
        'errors_total': 0,
        'memory_usage_bytes': 0,
        'cpu_usage_percent': 0,
        'db_connections': 0,
        'cache_hits': 0,
        'cache_misses': 0
    })
    
    def __post_init__(self) -> None:
        """Initialize metrics with registry."""
        # Initialize Prometheus metrics
        self.requests_total = Counter(
            'scraper_requests_total',
            'Total number of requests made',
            ['method', 'status'],
            registry=self.registry
        )
        
        self.request_duration_seconds = Histogram(
            'scraper_request_duration_seconds',
            'Request duration in seconds',
            ['method'],
            registry=self.registry
        )
        
        self.items_scraped = Counter(
            'scraper_items_scraped_total',
            'Total number of items scraped',
            registry=self.registry
        )
        
        self.items_processed = Counter(
            'scraper_items_processed_total',
            'Total number of items processed',
            registry=self.registry
        )
        
        self.errors_total = Counter(
            'scraper_errors_total',
            'Total number of errors',
            ['type'],
            registry=self.registry
        )
        
        self.active_requests = Gauge(
            'scraper_active_requests',
            'Number of requests currently being processed',
            registry=self.registry
        )
        
        self.memory_usage_bytes = Gauge(
            'scraper_memory_usage_bytes',
            'Current memory usage in bytes',
            registry=self.registry
        )
        
        self.cpu_usage_percent = Gauge(
            'scraper_cpu_usage_percent',
            'Current CPU usage percentage',
            registry=self.registry
        )
        
        self.db_connections = Gauge(
            'scraper_db_connections',
            'Number of active database connections',
            registry=self.registry
        )
        
        self.db_query_duration_seconds = Histogram(
            'scraper_db_query_duration_seconds',
            'Database query duration in seconds',
            ['query_type'],
            registry=self.registry
        )
        
        self.cache_hits = Counter(
            'scraper_cache_hits_total',
            'Total number of cache hits',
            registry=self.registry
        )
        
        self.cache_misses = Counter(
            'scraper_cache_misses_total',
            'Total number of cache misses',
            registry=self.registry
        )
        
        # Initialize performance monitoring
        try:
            import psutil  # type: ignore
            self.process = psutil.Process()
        except ImportError:
            logger.warning("psutil not installed, performance monitoring disabled")
            self.process = None
        
        # Initialize counters at 0
        self._reset_counters()

    def _reset_counters(self) -> None:
        """Reset all counters to 0."""
        try:
            for key in self._values:
                self._values[key] = 0
        except Exception as e:
            logger.error(f"Failed to reset counters: {e}")

    def inc_counter(self, name: str, amount: float = 1) -> None:
        """Increment a counter."""
        self._values[name] += amount

    def dec_counter(self, name: str, amount: float = 1) -> None:
        """Decrement a counter."""
        self._values[name] -= amount

    def set_gauge(self, name: str, value: float) -> None:
        """Set a gauge value."""
        self._values[name] = value

    def get_value(self, name: str) -> float:
        """Get a metric value."""
        return self._values.get(name, 0)

    @contextmanager
    def _safe_counter_increment(self, counter: Counter, labels: Optional[Dict[str, Any]] = None) -> Any:
        """Safely increment a counter with overflow protection."""
        try:
            current_value = sum(
                float(metric.get('_value', 0))
                for metric in counter._metrics.values()
            )
            if current_value < MAX_COUNTER_VALUE:
                if labels:
                    counter.labels(**labels).inc()
                else:
                    counter.inc()
            else:
                logger.warning(f"Counter {counter._name} reached maximum value, resetting")
                counter._metrics.clear()
        except Exception as e:
            logger.error(f"Failed to increment counter: {e}")
        yield

    def start_prometheus_server(self, port: int = 8000) -> None:
        """Start Prometheus metrics server with error handling."""
        max_attempts = 10
        current_port = port
        
        for attempt in range(max_attempts):
            try:
                start_http_server(current_port, registry=self.registry)
                logger.info(f"Prometheus metrics server started on port {current_port}")
                return
            except OSError as e:
                if e.errno == 98:  # Address already in use
                    current_port = port + attempt + 1
                    logger.warning(f"Port {current_port - 1} in use, trying port {current_port}")
                    continue
                else:
                    logger.error(f"Failed to start Prometheus server: {e}")
                    return
            except Exception as e:
                logger.error(f"Failed to start Prometheus server: {e}")
                return
        
        logger.error(f"Failed to find available port after {max_attempts} attempts")

    def track_request(self, method: str):
        """Context manager to track request metrics with error handling."""
        class RequestTracker:
            def __init__(self, metrics, method):
                self.metrics = metrics
                self.method = method
                self.start_time = None
                self.error = None

            def __enter__(self):
                try:
                    self.start_time = time.time()
                    self.metrics.inc_counter('active_requests')
                except Exception as e:
                    logger.error(f"Failed to start request tracking: {e}")
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                try:
                    duration = time.time() - self.start_time
                    self.metrics.request_duration_seconds.labels(
                        method=self.method
                    ).observe(duration)
                    
                    self.metrics.dec_counter('active_requests')
                    self.metrics.inc_counter('requests_total')
                    
                    if exc_type:
                        self.metrics.inc_counter('errors_total')
                except Exception as e:
                    logger.error(f"Failed to complete request tracking: {e}")

        return RequestTracker(self, method)

    def track_query(self, query_type: str):
        """Context manager to track database query metrics."""
        class QueryTracker:
            def __init__(self, metrics, query_type):
                self.metrics = metrics
                self.query_type = query_type
                self.start_time = None

            def __enter__(self):
                self.start_time = time.time()
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                duration = time.time() - self.start_time
                self.metrics.db_query_duration_seconds.labels(
                    query_type=self.query_type
                ).observe(duration)

        return QueryTracker(self, query_type)

    def update_performance_metrics(self):
        """Update performance metrics with error handling and rate limiting."""
        if not self.process:
            return
        
        try:
            # Rate limit updates
            current_time = time.time()
            if hasattr(self, '_last_update') and current_time - self._last_update < 1:
                return
            
            self._last_update = current_time
            
            # Memory usage
            memory_info = self.process.memory_info()
            self.memory_usage_bytes.set(memory_info.rss)
            
            # CPU usage with timeout
            cpu_percent = self.process.cpu_percent(interval=0.1)
            self.cpu_usage_percent.set(cpu_percent)
            
        except Exception as e:
            logger.error(f"Failed to update performance metrics: {e}")

    def record_item_scraped(self):
        """Record a scraped item with overflow protection."""
        with self._safe_counter_increment(self.items_scraped):
            pass

    def record_item_processed(self):
        """Record a processed item with overflow protection."""
        with self._safe_counter_increment(self.items_processed):
            pass

    def record_error(self, error_type: str):
        """Record an error with overflow protection."""
        with self._safe_counter_increment(self.errors_total, {'type': error_type}):
            pass

    def record_cache_hit(self):
        """Record a cache hit with overflow protection."""
        with self._safe_counter_increment(self.cache_hits):
            pass

    def record_cache_miss(self):
        """Record a cache miss with overflow protection."""
        with self._safe_counter_increment(self.cache_misses):
            pass

    def set_db_connections(self, count: int):
        """Set the number of active database connections."""
        self.db_connections.set(count)

@dataclass
class MetricsReport:
    """Report of current metrics."""
    timestamp: datetime
    requests: Dict[str, float]
    errors: Dict[str, float]
    items: Dict[str, float]
    performance: Dict[str, float]
    database: Dict[str, float]
    cache: Dict[str, float]

class MetricsManager:
    """Manager for scraper metrics."""
    
    def __init__(self, enable_prometheus: bool = True, prometheus_port: int = 8000):
        """Initialize metrics manager."""
        self.metrics = ScraperMetrics()
        
        if enable_prometheus:
            self.metrics.start_prometheus_server(prometheus_port)
    
    def get_report(self) -> MetricsReport:
        """Generate a metrics report."""
        return MetricsReport(
            timestamp=datetime.now(),
            requests={
                'total': self.metrics.get_value('requests_total'),
                'active': self.metrics.get_value('active_requests')
            },
            errors={
                'total': self.metrics.get_value('errors_total')
            },
            items={
                'scraped': self.metrics.get_value('items_scraped'),
                'processed': self.metrics.get_value('items_processed')
            },
            performance={
                'memory_mb': self.metrics.get_value('memory_usage_bytes') / 1024 / 1024,
                'cpu_percent': self.metrics.get_value('cpu_usage_percent')
            },
            database={
                'connections': self.metrics.get_value('db_connections')
            },
            cache={
                'hits': self.metrics.get_value('cache_hits'),
                'misses': self.metrics.get_value('cache_misses')
            }
        )

    def log_report(self, report: Optional[MetricsReport] = None):
        """Log current metrics."""
        if report is None:
            report = self.get_report()
        
        logger.info("Metrics Report:")
        logger.info(f"Timestamp: {report.timestamp}")
        logger.info(f"Requests: {report.requests}")
        logger.info(f"Errors: {report.errors}")
        logger.info(f"Items: {report.items}")
        logger.info(f"Performance: {report.performance}")
        logger.info(f"Database: {report.database}")
        logger.info(f"Cache: {report.cache}")

    def track_request(self, method: str):
        """Track request metrics."""
        return self.metrics.track_request(method)

    def track_query(self, query_type: str):
        """Track database query metrics."""
        return self.metrics.track_query(query_type)

    def update_performance_metrics(self):
        """Update performance metrics."""
        self.metrics.update_performance_metrics()

    def record_item_scraped(self):
        """Record a scraped item."""
        self.metrics.record_item_scraped()

    def record_item_processed(self):
        """Record a processed item."""
        self.metrics.record_item_processed()

    def record_error(self, error_type: str):
        """Record an error."""
        self.metrics.record_error(error_type)

    def record_cache_hit(self):
        """Record a cache hit."""
        self.metrics.record_cache_hit()

    def record_cache_miss(self):
        """Record a cache miss."""
        self.metrics.record_cache_miss()

    def set_db_connections(self, count: int):
        """Set the number of active database connections."""
        self.metrics.set_db_connections(count) 