"""Sistema de rate limiting para el scraper."""

import logging
import time
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass
import json

from .metrics import MetricsManager
from .cache import CacheManager

logger = logging.getLogger(__name__)

@dataclass
class RateLimitRule:
    """Regla de rate limiting."""
    requests: int  # Número de requests permitidos
    period: int   # Período en segundos
    penalty: int  # Penalización en segundos por exceder el límite

@dataclass
class RateLimitState:
    """Estado de rate limiting para un dominio."""
    requests: List[float]  # Timestamps de requests
    blocked_until: Optional[float] = None  # Timestamp hasta el que está bloqueado

class RateLimiter:
    """Limitador de tasa de requests."""
    
    def __init__(
        self,
        metrics: Optional[MetricsManager] = None,
        cache: Optional[CacheManager] = None
    ):
        """Inicializa el rate limiter.
        
        Args:
            metrics: Gestor de métricas opcional
            cache: Gestor de caché opcional
        """
        self.metrics = metrics
        self.cache = cache
        self.rules: Dict[str, RateLimitRule] = {
            'default': RateLimitRule(
                requests=60,    # 60 requests
                period=60,      # por minuto
                penalty=300     # 5 minutos de penalización
            ),
            'search': RateLimitRule(
                requests=30,    # 30 requests
                period=60,      # por minuto
                penalty=600     # 10 minutos de penalización
            ),
            'scrape': RateLimitRule(
                requests=10,    # 10 requests
                period=60,      # por minuto
                penalty=900     # 15 minutos de penalización
            )
        }
        self.states: Dict[str, Dict[str, RateLimitState]] = {}
        self.lock = threading.Lock()
    
    def add_rule(self, name: str, rule: RateLimitRule) -> None:
        """Añade una nueva regla.
        
        Args:
            name: Nombre de la regla
            rule: Regla a añadir
        """
        with self.lock:
            self.rules[name] = rule
            logger.info(f"Added rate limit rule: {name}")
    
    def remove_rule(self, name: str) -> None:
        """Elimina una regla.
        
        Args:
            name: Nombre de la regla a eliminar
        """
        with self.lock:
            if name in self.rules and name != 'default':
                del self.rules[name]
                logger.info(f"Removed rate limit rule: {name}")
    
    def is_allowed(self, domain: str, rule_name: str = 'default') -> bool:
        """Verifica si se permite un request.
        
        Args:
            domain: Dominio del request
            rule_name: Nombre de la regla a aplicar
            
        Returns:
            bool: True si se permite el request
        """
        with self.lock:
            now = time.time()
            
            # Obtener regla
            rule = self.rules.get(rule_name, self.rules['default'])
            
            # Inicializar estado si no existe
            if rule_name not in self.states:
                self.states[rule_name] = {}
            if domain not in self.states[rule_name]:
                self.states[rule_name][domain] = RateLimitState(requests=[])
            
            state = self.states[rule_name][domain]
            
            # Verificar si está bloqueado
            if state.blocked_until and now < state.blocked_until:
                if self.metrics:
                    self.metrics.record_rate_limit_block(domain, rule_name)
                return False
            
            # Limpiar requests antiguos
            cutoff = now - rule.period
            state.requests = [t for t in state.requests if t > cutoff]
            
            # Verificar límite
            if len(state.requests) >= rule.requests:
                state.blocked_until = now + rule.penalty
                if self.metrics:
                    self.metrics.record_rate_limit_violation(domain, rule_name)
                return False
            
            # Registrar request
            state.requests.append(now)
            if self.metrics:
                self.metrics.record_rate_limit_request(domain, rule_name)
            
            return True
    
    def wait_if_needed(self, domain: str, rule_name: str = 'default') -> float:
        """Espera si es necesario y retorna el tiempo de espera.
        
        Args:
            domain: Dominio del request
            rule_name: Nombre de la regla a aplicar
            
        Returns:
            float: Tiempo de espera en segundos
        """
        with self.lock:
            if self.is_allowed(domain, rule_name):
                return 0
            
            state = self.states[rule_name][domain]
            if state.blocked_until:
                wait_time = state.blocked_until - time.time()
                if wait_time > 0:
                    logger.info(
                        f"Rate limit reached for {domain} ({rule_name}). "
                        f"Waiting {wait_time:.1f} seconds"
                    )
                    time.sleep(wait_time)
                    return wait_time
            
            return 0
    
    def get_state(self, domain: str, rule_name: str = 'default') -> Dict[str, Any]:
        """Obtiene el estado actual para un dominio.
        
        Args:
            domain: Dominio a consultar
            rule_name: Nombre de la regla
            
        Returns:
            Dict[str, Any]: Estado actual
        """
        with self.lock:
            if rule_name not in self.states or domain not in self.states[rule_name]:
                return {
                    'requests': 0,
                    'blocked': False,
                    'remaining_time': 0
                }
            
            state = self.states[rule_name][domain]
            now = time.time()
            
            return {
                'requests': len(state.requests),
                'blocked': bool(state.blocked_until and now < state.blocked_until),
                'remaining_time': max(0, state.blocked_until - now) if state.blocked_until else 0
            }
    
    def reset(self, domain: str, rule_name: str = 'default') -> None:
        """Resetea el estado para un dominio.
        
        Args:
            domain: Dominio a resetear
            rule_name: Nombre de la regla
        """
        with self.lock:
            if rule_name in self.states and domain in self.states[rule_name]:
                self.states[rule_name][domain] = RateLimitState(requests=[])
                logger.info(f"Reset rate limit state for {domain} ({rule_name})")
    
    def save_state(self, file_path: str) -> None:
        """Guarda el estado actual en un archivo.
        
        Args:
            file_path: Ruta del archivo
        """
        try:
            with self.lock:
                state_data = {}
                for rule_name, domains in self.states.items():
                    state_data[rule_name] = {}
                    for domain, state in domains.items():
                        state_data[rule_name][domain] = {
                            'requests': state.requests,
                            'blocked_until': state.blocked_until
                        }
            
            with open(file_path, 'w') as f:
                json.dump(state_data, f)
            
            logger.info(f"Rate limit state saved to {file_path}")
            
        except Exception as e:
            logger.error(f"Error saving rate limit state: {str(e)}", exc_info=True)
    
    def load_state(self, file_path: str) -> None:
        """Carga el estado desde un archivo.
        
        Args:
            file_path: Ruta del archivo
        """
        try:
            with open(file_path, 'r') as f:
                state_data = json.load(f)
            
            with self.lock:
                self.states.clear()
                for rule_name, domains in state_data.items():
                    self.states[rule_name] = {}
                    for domain, state in domains.items():
                        self.states[rule_name][domain] = RateLimitState(
                            requests=state['requests'],
                            blocked_until=state['blocked_until']
                        )
            
            logger.info(f"Rate limit state loaded from {file_path}")
            
        except Exception as e:
            logger.error(f"Error loading rate limit state: {str(e)}", exc_info=True)
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del rate limiter."""
        with self.lock:
            stats = {
                'rules': len(self.rules),
                'domains': sum(len(domains) for domains in self.states.values()),
                'blocked_domains': sum(
                    1 for domains in self.states.values()
                    for state in domains.values()
                    if state.blocked_until and time.time() < state.blocked_until
                ),
                'total_requests': sum(
                    len(state.requests)
                    for domains in self.states.values()
                    for state in domains.values()
                )
            }
            
            # Estadísticas por regla
            stats['rules_stats'] = {}
            for rule_name, domains in self.states.items():
                stats['rules_stats'][rule_name] = {
                    'domains': len(domains),
                    'blocked': sum(
                        1 for state in domains.values()
                        if state.blocked_until and time.time() < state.blocked_until
                    ),
                    'requests': sum(len(state.requests) for state in domains.values())
                }
            
            return stats 