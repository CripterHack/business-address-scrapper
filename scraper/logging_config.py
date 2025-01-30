"""Logging configuration for the scraper."""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from .exceptions import ConfigurationError

class ScraperLogger:
    """Custom logger for the scraper."""
    
    def __init__(self, name: str, settings: Optional[dict] = None):
        """Initialize logger with settings."""
        self.name = name
        self.settings = settings or {}
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Set up and configure logger."""
        logger = logging.getLogger(self.name)
        logger.setLevel(self._get_log_level())
        
        # Remove existing handlers
        logger.handlers = []
        
        # Add handlers
        logger.addHandler(self._setup_console_handler())
        logger.addHandler(self._setup_file_handler())
        
        if self.settings.get('enable_syslog'):
            logger.addHandler(self._setup_syslog_handler())
            
        if self.settings.get('enable_json'):
            logger.addHandler(self._setup_json_handler())
        
        return logger

    def _get_log_level(self) -> int:
        """Get log level from settings or environment."""
        level = os.getenv('LOG_LEVEL', self.settings.get('level', 'INFO'))
        try:
            return getattr(logging, level.upper())
        except AttributeError:
            raise ConfigurationError(
                f"Invalid log level: {level}",
                details={'valid_levels': ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']}
            )

    def _setup_console_handler(self) -> logging.Handler:
        """Set up console handler with colored output."""
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(ColoredFormatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        return handler

    def _setup_file_handler(self) -> logging.Handler:
        """Set up file handler with rotation."""
        log_file = Path(self.settings.get('file', 'logs/scraper.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        if self.settings.get('rotate', True):
            handler = logging.handlers.RotatingFileHandler(
                filename=str(log_file),
                maxBytes=self.settings.get('max_size', 10 * 1024 * 1024),  # 10MB
                backupCount=self.settings.get('backup_count', 5),
                encoding='utf-8'
            )
        else:
            handler = logging.FileHandler(
                filename=str(log_file),
                encoding='utf-8'
            )
        
        handler.setFormatter(logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        return handler

    def _setup_syslog_handler(self) -> logging.Handler:
        """Set up syslog handler."""
        try:
            handler = logging.handlers.SysLogHandler(
                address=self.settings.get('syslog_address', '/dev/log')
            )
            handler.setFormatter(logging.Formatter(
                fmt='%(name)s[%(process)d]: %(levelname)s %(message)s'
            ))
            return handler
        except Exception as e:
            self.logger.warning(f"Failed to set up syslog handler: {e}")
            return None

    def _setup_json_handler(self) -> logging.Handler:
        """Set up JSON format handler."""
        json_file = Path(self.settings.get('json_file', 'logs/scraper.json'))
        json_file.parent.mkdir(parents=True, exist_ok=True)
        
        handler = logging.FileHandler(
            filename=str(json_file),
            encoding='utf-8'
        )
        handler.setFormatter(JsonFormatter())
        return handler

    def get_logger(self) -> logging.Logger:
        """Get configured logger."""
        return self.logger

class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored output."""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[41m',  # Red background
    }
    RESET = '\033[0m'

    def format(self, record):
        """Format log record with colors."""
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.RESET}"
        return super().format(record)

class JsonFormatter(logging.Formatter):
    """Custom formatter for JSON output."""
    
    def format(self, record):
        """Format log record as JSON."""
        import json
        
        data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'name': record.name,
            'level': record.levelname,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        if record.exc_info:
            data['exception'] = self.formatException(record.exc_info)
            
        if hasattr(record, 'stack_info') and record.stack_info:
            data['stack_info'] = self.formatStack(record.stack_info)
        
        return json.dumps(data)

def configure_logging(name: str, settings: Optional[dict] = None) -> logging.Logger:
    """Configure and get logger."""
    logger_instance = ScraperLogger(name, settings)
    return logger_instance.get_logger()

def setup_file_handler(log_file: str) -> logging.Handler:
    """Set up a file handler for the given log file."""
    return ScraperLogger('temp', {'file': log_file})._setup_file_handler()

def setup_console_handler() -> logging.Handler:
    """Set up a console handler."""
    return ScraperLogger('temp')._setup_console_handler() 