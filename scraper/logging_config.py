"""Configuración del sistema de logging para el scraper."""

import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Union, List
import json
import gzip
import shutil
from functools import wraps

from .settings import Settings

class JsonFormatter(logging.Formatter):
    """Formateador de logs en formato JSON."""
    
    def __init__(self, include_extra_fields: bool = True):
        """Inicializa el formateador.
        
        Args:
            include_extra_fields: Si incluir campos extra en el formato
        """
        super().__init__()
        self.include_extra_fields = include_extra_fields
        self.default_fields = {
            'timestamp': '%(asctime)s',
            'level': '%(levelname)s',
            'name': '%(name)s',
            'message': '%(message)s'
        }
        
    def format(self, record: logging.LogRecord) -> str:
        """Formatea el registro en JSON.
        
        Args:
            record: Registro a formatear
            
        Returns:
            str: Registro formateado en JSON
        """
        message = {
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            'name': record.name,
            'message': record.getMessage()
        }
        
        if self.include_extra_fields:
            # Incluir campos extra
            if hasattr(record, 'metadata'):
                message['metadata'] = record.metadata
            
            if record.exc_info:
                message['exception'] = self.formatException(record.exc_info)
            
            # Incluir campos del registro
            for key, value in record.__dict__.items():
                if (
                    key not in ['timestamp', 'level', 'name', 'message', 'metadata']
                    and not key.startswith('_')
                    and isinstance(value, (str, int, float, bool, dict, list))
                ):
                    message[key] = value
        
        return json.dumps(message)

class CompressedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Handler que comprime los archivos rotados."""
    
    def __init__(
        self,
        filename: Union[str, Path],
        mode: str = 'a',
        maxBytes: int = 0,
        backupCount: int = 0,
        encoding: Optional[str] = None,
        delay: bool = False,
        errors: Optional[str] = None
    ):
        """Inicializa el handler.
        
        Args:
            filename: Nombre del archivo
            mode: Modo de apertura
            maxBytes: Tamaño máximo del archivo
            backupCount: Número de backups
            encoding: Codificación del archivo
            delay: Si retrasar la apertura
            errors: Manejo de errores de codificación
        """
        # Asegurar que el directorio existe
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        super().__init__(
            filename,
            mode,
            maxBytes,
            backupCount,
            encoding,
            delay,
            errors
        )
    
    def rotation_filename(self, default_name: str) -> str:
        """Genera nombre para archivo rotado.
        
        Args:
            default_name: Nombre por defecto
            
        Returns:
            str: Nombre del archivo
        """
        return f"{default_name}.gz"
    
    def rotate(self, source: str, dest: str) -> None:
        """Rota y comprime un archivo.
        
        Args:
            source: Archivo fuente
            dest: Archivo destino
        """
        try:
            with open(source, 'rb') as f_in:
                with gzip.open(dest, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(source)
        except Exception as e:
            # No propagar el error, solo loguearlo
            logging.error(f"Error rotating log file: {str(e)}")

class CustomLogger(logging.Logger):
    """Logger personalizado con funcionalidades adicionales."""
    
    def __init__(self, name: str):
        """Inicializa el logger.
        
        Args:
            name: Nombre del logger
        """
        super().__init__(name)
        self.metadata: Dict[str, Any] = {}
    
    def set_metadata(self, **kwargs: Any) -> None:
        """Establece metadatos para el logger.
        
        Args:
            **kwargs: Metadatos a establecer
        """
        self.metadata.update(kwargs)
    
    def clear_metadata(self) -> None:
        """Limpia los metadatos del logger."""
        self.metadata.clear()
    
    def _log_with_metadata(
        self,
        level: int,
        msg: str,
        *args: Any,
        **kwargs: Any
    ) -> None:
        """Añade metadatos al log.
        
        Args:
            level: Nivel de log
            msg: Mensaje
            *args: Argumentos posicionales
            **kwargs: Argumentos nombrados
        """
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra']['metadata'] = self.metadata
        super()._log(level, msg, *args, **kwargs)

def validate_log_config(
    log_dir: str,
    level: str,
    rotation: str,
    retention: int,
    json_format: bool,
    console: bool
) -> None:
    """Valida la configuración de logging.
    
    Args:
        log_dir: Directorio para logs
        level: Nivel de logging
        rotation: Tipo de rotación
        retention: Días de retención
        json_format: Si usar formato JSON
        console: Si mostrar en consola
        
    Raises:
        ValueError: Si la configuración es inválida
    """
    # Validar nivel
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if level.upper() not in valid_levels:
        raise ValueError(f"Invalid log level: {level}")
    
    # Validar rotación
    if rotation not in ['daily', 'size']:
        raise ValueError(f"Invalid rotation type: {rotation}")
    
    # Validar retención
    if retention < 1:
        raise ValueError("Retention must be at least 1 day")
    
    # Validar directorio
    if not isinstance(log_dir, (str, Path)):
        raise ValueError("log_dir must be string or Path")

def setup_logging(settings: Settings) -> None:
    """Configure logging system using settings.
    
    Args:
        settings: Application settings instance
    """
    log_path = settings.directories.logs_dir
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(settings.logging.level)
    
    # Create formatter
    formatter = JsonFormatter() if settings.logging.json_format else logging.Formatter(
        settings.logging.format,
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Configure file handler
    if settings.logging.rotate:
        file_handler = CompressedRotatingFileHandler(
            filename=log_path / 'scraper.log',
            maxBytes=settings.logging.max_size,
            backupCount=settings.logging.backup_count,
            encoding='utf-8'
        )
    else:
        file_handler = logging.FileHandler(
            filename=log_path / 'scraper.log',
            encoding='utf-8'
        )
    
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Configure error log
    error_handler = CompressedRotatingFileHandler(
        filename=log_path / 'error.log',
        maxBytes=settings.logging.max_size,
        backupCount=settings.logging.backup_count,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)
    
    # Configure console output if enabled
    if settings.logging.console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Log initialization
    root_logger.info(
        "Logging system initialized",
        extra={
            'metadata': {
                'log_dir': str(log_path),
                'level': settings.logging.level,
                'rotate': settings.logging.rotate,
                'backup_count': settings.logging.backup_count,
                'json_format': settings.logging.json_format,
                'max_size': settings.logging.max_size
            }
        }
    )

def get_logger(name: str) -> CustomLogger:
    """Obtiene un logger personalizado.
    
    Args:
        name: Nombre del logger
        
    Returns:
        CustomLogger: Logger personalizado
    """
    logging.setLoggerClass(CustomLogger)
    return logging.getLogger(name)

class LoggerAdapter(logging.LoggerAdapter):
    """Adaptador de logger con contexto."""
    
    def __init__(
        self,
        logger: logging.Logger,
        extra: Optional[Dict[str, Any]] = None
    ):
        """Inicializa el adaptador.
        
        Args:
            logger: Logger base
            extra: Datos extra para el contexto
        """
        super().__init__(logger, extra or {})
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Procesa el mensaje añadiendo contexto.
        
        Args:
            msg: Mensaje a procesar
            kwargs: Argumentos adicionales
            
        Returns:
            tuple: Mensaje y kwargs procesados
        """
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra'].update(self.extra)
        return msg, kwargs

def get_logger_with_context(**context: Any) -> LoggerAdapter:
    """Obtiene un logger con contexto.
    
    Args:
        **context: Datos de contexto
        
    Returns:
        LoggerAdapter: Logger con contexto
    """
    logger = get_logger(__name__)
    return LoggerAdapter(logger, context)

def log_execution_time(logger: Optional[logging.Logger] = None):
    """Decorador para registrar tiempo de ejecución.
    
    Args:
        logger: Logger opcional (usa el del módulo si no se especifica)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Usar logger proporcionado o crear uno nuevo
            _logger = logger or logging.getLogger(func.__module__)
            
            start_time = datetime.now()
            try:
                result = func(*args, **kwargs)
                execution_time = (datetime.now() - start_time).total_seconds()
                _logger.info(
                    f"Function {func.__name__} executed in {execution_time:.2f} seconds",
                    extra={
                        'metadata': {
                            'function': func.__name__,
                            'execution_time': execution_time
                        }
                    }
                )
                return result
            except Exception as e:
                execution_time = (datetime.now() - start_time).total_seconds()
                _logger.error(
                    f"Error in function {func.__name__}: {str(e)}",
                    extra={
                        'metadata': {
                            'function': func.__name__,
                            'execution_time': execution_time,
                            'error': str(e)
                        }
                    },
                    exc_info=True
                )
                raise
        return wrapper
    return decorator

def cleanup_old_logs(
    log_dir: Union[str, Path],
    max_age_days: int = 30
) -> None:
    """Limpia logs antiguos.
    
    Args:
        log_dir: Directorio de logs
        max_age_days: Edad máxima en días
    """
    try:
        log_path = Path(log_dir)
        if not log_path.exists():
            return
            
        cutoff = datetime.now() - timedelta(days=max_age_days)
        
        for file in log_path.glob('*.log*'):
            try:
                if file.stat().st_mtime < cutoff.timestamp():
                    file.unlink()
            except Exception as e:
                logging.error(f"Error deleting old log {file}: {str(e)}")
                
    except Exception as e:
        logging.error(f"Error cleaning old logs: {str(e)}")

def get_log_stats(log_dir: Union[str, Path]) -> Dict[str, Any]:
    """Obtiene estadísticas de logs.
    
    Args:
        log_dir: Directorio de logs
        
    Returns:
        Dict[str, Any]: Estadísticas de logs
    """
    try:
        log_path = Path(log_dir)
        if not log_path.exists():
            return {}
            
        stats = {
            'total_size_mb': 0,
            'file_count': 0,
            'oldest_file': None,
            'newest_file': None,
            'files': []
        }
        
        for file in log_path.glob('*.log*'):
            file_stat = file.stat()
            file_info = {
                'name': file.name,
                'size_mb': file_stat.st_size / (1024 * 1024),
                'modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            }
            
            stats['total_size_mb'] += file_info['size_mb']
            stats['file_count'] += 1
            stats['files'].append(file_info)
            
            # Actualizar oldest/newest
            if not stats['oldest_file'] or file_stat.st_mtime < stats['oldest_file']['timestamp']:
                stats['oldest_file'] = {
                    'name': file.name,
                    'timestamp': file_stat.st_mtime
                }
            
            if not stats['newest_file'] or file_stat.st_mtime > stats['newest_file']['timestamp']:
                stats['newest_file'] = {
                    'name': file.name,
                    'timestamp': file_stat.st_mtime
                }
        
        return stats
        
    except Exception as e:
        logging.error(f"Error getting log stats: {str(e)}")
        return {} 