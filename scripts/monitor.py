#!/usr/bin/env python3
"""
Monitoring script for the web scraper.
Tracks performance metrics, resource usage, and scraping progress.
"""

import os
import sys
import time
import logging
import psutil
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import requests
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Prometheus metrics
SCRAPER_REQUESTS = Counter('scraper_requests_total', 'Total requests made by scraper')
SCRAPER_ERRORS = Counter('scraper_errors_total', 'Total errors encountered')
SCRAPER_PROCESSING_TIME = Histogram('scraper_processing_seconds', 'Time spent processing items')
SCRAPER_MEMORY_USAGE = Gauge('scraper_memory_mb', 'Current memory usage in MB')
SCRAPER_CPU_USAGE = Gauge('scraper_cpu_percent', 'Current CPU usage percentage')

class ScraperMonitor:
    def __init__(self):
        self.start_time = datetime.now()
        self.stats: Dict[str, Any] = {
            'requests_made': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'items_processed': 0,
            'errors': [],
            'warnings': []
        }
        self.process = psutil.Process()

    def update_resource_metrics(self):
        """Update system resource usage metrics"""
        try:
            # Memory usage
            memory_usage = self.process.memory_info().rss / 1024 / 1024  # Convert to MB
            SCRAPER_MEMORY_USAGE.set(memory_usage)

            # CPU usage
            cpu_percent = self.process.cpu_percent()
            SCRAPER_CPU_USAGE.set(cpu_percent)

            logger.debug(f"Memory usage: {memory_usage:.2f}MB, CPU usage: {cpu_percent}%")
        except Exception as e:
            logger.error(f"Error updating resource metrics: {e}")

    def check_log_file(self):
        """Monitor log file for errors and warnings"""
        log_file = os.getenv('LOG_FILE', 'scraper.log')
        if not os.path.exists(log_file):
            logger.warning(f"Log file not found: {log_file}")
            return

        try:
            with open(log_file, 'r') as f:
                for line in f.readlines():
                    if 'ERROR' in line:
                        SCRAPER_ERRORS.inc()
                        self.stats['errors'].append(line.strip())
                    elif 'WARNING' in line:
                        self.stats['warnings'].append(line.strip())
        except Exception as e:
            logger.error(f"Error reading log file: {e}")

    def check_output_files(self):
        """Monitor output files for progress"""
        output_file = os.getenv('CSV_OUTPUT_FILE', 'business_data.csv')
        if os.path.exists(output_file):
            try:
                # Count lines in output file
                with open(output_file, 'r') as f:
                    line_count = sum(1 for _ in f) - 1  # Subtract header
                self.stats['items_processed'] = line_count
                logger.info(f"Processed items: {line_count}")
            except Exception as e:
                logger.error(f"Error checking output file: {e}")

    def send_alerts(self):
        """Send alerts if thresholds are exceeded"""
        alert_email = os.getenv('ALERT_EMAIL')
        error_threshold = int(os.getenv('ERROR_NOTIFICATION_THRESHOLD', 100))

        if len(self.stats['errors']) >= error_threshold and alert_email:
            try:
                # Here you would implement your alert sending logic
                # For example, sending an email or a Slack notification
                logger.warning(f"Error threshold exceeded: {len(self.stats['errors'])} errors")
            except Exception as e:
                logger.error(f"Error sending alert: {e}")

    def export_metrics(self):
        """Export metrics to monitoring system"""
        metrics_endpoint = os.getenv('METRICS_ENDPOINT')
        if not metrics_endpoint:
            return

        try:
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'uptime_seconds': (datetime.now() - self.start_time).total_seconds(),
                'memory_mb': self.process.memory_info().rss / 1024 / 1024,
                'cpu_percent': self.process.cpu_percent(),
                'stats': self.stats
            }

            response = requests.post(
                metrics_endpoint,
                json=metrics,
                headers={'Content-Type': 'application/json'},
                timeout=5
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Error exporting metrics: {e}")

    def run(self):
        """Main monitoring loop"""
        # Start Prometheus metrics server
        start_http_server(8000)
        logger.info("Started monitoring server on port 8000")

        try:
            while True:
                self.update_resource_metrics()
                self.check_log_file()
                self.check_output_files()
                self.send_alerts()
                self.export_metrics()

                # Save current stats to file
                stats_file = Path('monitor_stats.json')
                stats_file.write_text(json.dumps(self.stats, indent=2))

                # Wait before next check
                time.sleep(int(os.getenv('MONITOR_INTERVAL', 60)))
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
            sys.exit(1)

if __name__ == '__main__':
    monitor = ScraperMonitor()
    monitor.run() 