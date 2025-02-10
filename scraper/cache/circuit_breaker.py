"""Sistema de circuit breaker para la caché distribuida."""

import logging
import time
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum
import threading

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    """Estados del circuit breaker."""
    CLOSED = 'closed'      # Operación normal
    OPEN = 'open'         # Fallo detectado
    HALF_OPEN = 'half_open'  # Probando recuperación

class CircuitBreaker:
    """Implementación de circuit breaker."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: int = 60,
        half_open_timeout: int = 30,
        operation_timeout: float = 1.0
    ):
        """Inicializa el circuit breaker.
        
        Args:
            failure_threshold: Número de fallos antes de abrir
            reset_timeout: Tiempo antes de reintentar en segundos
            half_open_timeout: Tiempo en estado half-open
            operation_timeout: Timeout para operaciones en segundos
        """
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_timeout = half_open_timeout
        self.operation_timeout = operation_timeout
        
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_start: Optional[datetime] = None
        self._lock = threading.Lock()
    
    def execute(
        self,
        operation: Callable,
        fallback: Optional[Callable] = None,
        *args,
        **kwargs
    ) -> Any:
        """Ejecuta una operación con circuit breaker.
        
        Args:
            operation: Función a ejecutar
            fallback: Función de respaldo opcional
            *args: Argumentos para la operación
            **kwargs: Argumentos nombrados
            
        Returns:
            Any: Resultado de la operación
            
        Raises:
            Exception: Si la operación falla y no hay fallback
        """
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_reset():
                    self._to_half_open()
                else:
                    return self._handle_open_circuit(fallback, *args, **kwargs)
            
            if self.state == CircuitState.HALF_OPEN:
                if self._half_open_expired():
                    self._to_open()
                    return self._handle_open_circuit(fallback, *args, **kwargs)
        
        try:
            # Ejecutar con timeout
            result = self._execute_with_timeout(operation, *args, **kwargs)
            
            with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self._to_closed()
                self.failures = 0
            
            return result
            
        except Exception as e:
            with self._lock:
                self.failures += 1
                self.last_failure_time = datetime.now()
                
                if self.failures >= self.failure_threshold:
                    self._to_open()
                
                if self.state == CircuitState.HALF_OPEN:
                    self._to_open()
            
            logger.error(f"Circuit breaker operation failed: {str(e)}")
            return self._handle_open_circuit(fallback, *args, **kwargs)
    
    def _execute_with_timeout(
        self,
        operation: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Ejecuta una operación con timeout.
        
        Args:
            operation: Función a ejecutar
            *args: Argumentos
            **kwargs: Argumentos nombrados
            
        Returns:
            Any: Resultado de la operación
            
        Raises:
            TimeoutError: Si la operación excede el timeout
        """
        result = None
        exception = None
        completed = False
        
        def target():
            nonlocal result, exception, completed
            try:
                result = operation(*args, **kwargs)
                completed = True
            except Exception as e:
                exception = e
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout=self.operation_timeout)
        
        if not completed:
            raise TimeoutError(
                f"Operation timed out after {self.operation_timeout}s"
            )
        
        if exception:
            raise exception
        
        return result
    
    def _to_open(self) -> None:
        """Cambia al estado abierto."""
        self.state = CircuitState.OPEN
        self.last_failure_time = datetime.now()
        logger.warning("Circuit breaker opened")
    
    def _to_half_open(self) -> None:
        """Cambia al estado semi-abierto."""
        self.state = CircuitState.HALF_OPEN
        self.half_open_start = datetime.now()
        logger.info("Circuit breaker half-opened")
    
    def _to_closed(self) -> None:
        """Cambia al estado cerrado."""
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.last_failure_time = None
        self.half_open_start = None
        logger.info("Circuit breaker closed")
    
    def _should_reset(self) -> bool:
        """Verifica si debe reintentar.
        
        Returns:
            bool: True si debe reintentar
        """
        if not self.last_failure_time:
            return True
            
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        return elapsed >= self.reset_timeout
    
    def _half_open_expired(self) -> bool:
        """Verifica si expiró el tiempo en half-open.
        
        Returns:
            bool: True si expiró
        """
        if not self.half_open_start:
            return False
            
        elapsed = (datetime.now() - self.half_open_start).total_seconds()
        return elapsed >= self.half_open_timeout
    
    def _handle_open_circuit(
        self,
        fallback: Optional[Callable],
        *args,
        **kwargs
    ) -> Any:
        """Maneja el circuito abierto.
        
        Args:
            fallback: Función de respaldo
            *args: Argumentos
            **kwargs: Argumentos nombrados
            
        Returns:
            Any: Resultado del fallback
            
        Raises:
            Exception: Si no hay fallback
        """
        if fallback:
            return fallback(*args, **kwargs)
        raise Exception("Circuit is open") 