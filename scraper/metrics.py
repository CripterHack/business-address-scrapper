"""Metrics and monitoring module."""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from contextlib import contextmanager

from prometheus_client import Counter, Gauge, Histogram, start_http_server, CollectorRegistry

logger = logging.getLogger(__name__)

MAX_COUNTER_VALUE = 2**53 - 1  # JavaScript max safe integer

@dataclass
class ScraperMetrics:
    """Metrics collector for the scraper."""
    
    registry: CollectorRegistry = field(default_factory=CollectorRegistry)
    
    # Prometheus metrics with registry
    requests_total: Counter = field(default_factory=lambda: Counter(
        'scraper_requests_total',
        'Total number of requests made',
        ['method', 'status'],
        registry=registry
    ))
    
    request_duration_seconds: Histogram = field(default_factory=lambda: Histogram(
        'scraper_request_duration_seconds',
        'Request duration in seconds',
        ['method']
    ))
    
    items_scraped: Counter = field(default_factory=lambda: Counter(
        'scraper_items_scraped_total',
        'Total number of items scraped',
        registry=registry
    ))
    
    items_processed: Counter = field(default_factory=lambda: Counter(
        'scraper_items_processed_total',
        'Total number of items processed',
        registry=registry
    ))
    
    errors_total: Counter = field(default_factory=lambda: Counter(
        'scraper_errors_total',
        'Total number of errors',
        ['type'],
        registry=registry
    ))
    
    active_requests: Gauge = field(default_factory=lambda: Gauge(
        'scraper_active_requests',
        'Number of requests currently being processed',
        registry=registry
    ))
    
    # Performance metrics
    memory_usage_bytes: Gauge = field(default_factory=lambda: Gauge(
        'scraper_memory_usage_bytes',
        'Current memory usage in bytes',
        registry=registry
    ))
    
    cpu_usage_percent: Gauge = field(default_factory=lambda: Gauge(
        'scraper_cpu_usage_percent',
        'Current CPU usage percentage',
        registry=registry
    ))
    
    # Database metrics
    db_connections: Gauge = field(default_factory=lambda: Gauge(
        'scraper_db_connections',
        'Number of active database connections',
        registry=registry
    ))
    
    db_query_duration_seconds: Histogram = field(default_factory=lambda: Histogram(
        'scraper_db_query_duration_seconds',
        'Database query duration in seconds',
        ['query_type'],
        registry=registry
    ))
    
    # Cache metrics
    cache_hits: Counter = field(default_factory=lambda: Counter(
        'scraper_cache_hits_total',
        'Total number of cache hits',
        registry=registry
    ))
    
    cache_misses: Counter = field(default_factory=lambda: Counter(
        'scraper_cache_misses_total',
        'Total number of cache misses',
        registry=registry
    ))
    
    def __post_init__(self):
        """Initialize performance monitoring."""
        try:
            import psutil
            self.process = psutil.Process()
        except ImportError:
            logger.warning("psutil not installed, performance monitoring disabled")
            self.process = None
        
        # Initialize counters at 0
        self._reset_counters()

    def _reset_counters(self):
        """Reset all counters to 0."""
        try:
            self.items_scraped._value.clear()
            self.items_processed._value.clear()
            self.errors_total._value.clear()
            self.cache_hits._value.clear()
            self.cache_misses._value.clear()
        except Exception as e:
            logger.error(f"Failed to reset counters: {e}")

    @contextmanager
    def _safe_counter_increment(self, counter: Counter, labels: Optional[Dict] = None):
        """Safely increment a counter with overflow protection."""
        try:
            current_value = sum(counter._value.values())
            if current_value < MAX_COUNTER_VALUE:
                if labels:
                    counter.labels(**labels).inc()
                else:
                    counter.inc()
            else:
                logger.warning(f"Counter {counter._name} reached maximum value, resetting")
                counter._value.clear()
        except Exception as e:
            logger.error(f"Failed to increment counter: {e}")
        yield

    def start_prometheus_server(self, port: int = 8000):
        """Start Prometheus metrics server with error handling."""
        try:
            start_http_server(port, registry=self.registry)
            logger.info(f"Prometheus metrics server started on port {port}")
        except OSError as e:
            if e.errno == 98:  # Address already in use
                logger.warning(f"Port {port} already in use, trying alternative")
                try:
                    start_http_server(port + 1, registry=self.registry)
                    logger.info(f"Prometheus metrics server started on port {port + 1}")
                except Exception as e2:
                    logger.error(f"Failed to start Prometheus server on alternative port: {e2}")
            else:
                logger.error(f"Failed to start Prometheus server: {e}")
        except Exception as e:
            logger.error(f"Failed to start Prometheus server: {e}")

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
                    self.metrics.active_requests.inc()
                except Exception as e:
                    logger.error(f"Failed to start request tracking: {e}")
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                try:
                    duration = time.time() - self.start_time
                    self.metrics.request_duration_seconds.labels(
                        method=self.method
                    ).observe(duration)
                    
                    self.metrics.active_requests.dec()
                    
                    with self.metrics._safe_counter_increment(
                        self.metrics.requests_total,
                        {'method': self.method, 'status': 'error' if exc_type else 'success'}
                    ):
                        pass
                        
                    if exc_type:
                        with self.metrics._safe_counter_increment(
                            self.metrics.errors_total,
                            {'type': exc_type.__name__}
                        ):
                            pass
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
    requests: Dict[str, int]
    errors: Dict[str, int]
    items: Dict[str, int]
    performance: Dict[str, float]
    database: Dict[str, float]
    cache: Dict[str, int]

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
                'total': sum(self.metrics.requests_total._value.values()),
                'active': self.metrics.active_requests._value
            },
            errors={
                label[0]: value
                for label, value in self.metrics.errors_total._value.items()
            },
            items={
                'scraped': self.metrics.items_scraped._value,
                'processed': self.metrics.items_processed._value
            },
            performance={
                'memory_mb': self.metrics.memory_usage_bytes._value / 1024 / 1024,
                'cpu_percent': self.metrics.cpu_usage_percent._value
            },
            database={
                'connections': self.metrics.db_connections._value
            },
            cache={
                'hits': self.metrics.cache_hits._value,
                'misses': self.metrics.cache_misses._value
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