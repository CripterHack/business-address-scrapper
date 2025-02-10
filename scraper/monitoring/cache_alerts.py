"""Sistema de alertas para la caché distribuida."""

import logging
import time
import threading
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
import json
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from ..config import CacheConfig
from .cache_monitor import CacheMonitor

logger = logging.getLogger(__name__)

class AlertRule:
    """Regla de alerta para la caché."""
    
    def __init__(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        message_template: str,
        severity: str = 'warning',
        notification_channels: List[str] = None,
        cooldown: int = 300,
        aggregation_window: int = 60
    ):
        """Inicializa la regla de alerta.
        
        Args:
            name: Nombre de la regla
            condition: Función que evalúa la condición
            message_template: Plantilla para el mensaje
            severity: Severidad de la alerta
            notification_channels: Canales de notificación
            cooldown: Tiempo de espera entre alertas en segundos
            aggregation_window: Ventana de agregación en segundos
        """
        self.name = name
        self.condition = condition
        self.message_template = message_template
        self.severity = severity
        self.notification_channels = notification_channels or ['log']
        self.cooldown = cooldown
        self.aggregation_window = aggregation_window
        
        self.last_triggered: Optional[datetime] = None
        self.triggered_count = 0
    
    def should_trigger(self, stats: Dict[str, Any]) -> bool:
        """Verifica si la alerta debe dispararse.
        
        Args:
            stats: Estadísticas del sistema
            
        Returns:
            bool: True si la alerta debe dispararse
        """
        if self.last_triggered:
            time_since_last = (
                datetime.now() - self.last_triggered
            ).total_seconds()
            if time_since_last < self.cooldown:
                return False
        
        return self.condition(stats)
    
    def format_message(self, stats: Dict[str, Any]) -> str:
        """Formatea el mensaje de la alerta.
        
        Args:
            stats: Estadísticas del sistema
            
        Returns:
            str: Mensaje formateado
        """
        return self.message_template.format(**stats)

class AlertManager:
    """Gestor de alertas para la caché distribuida."""
    
    def __init__(
        self,
        monitor: CacheMonitor,
        config: CacheConfig,
        check_interval: int = 60,
        alert_history_file: str = 'alerts/cache_alerts.json',
        email_config: Optional[Dict[str, Any]] = None,
        slack_config: Optional[Dict[str, Any]] = None
    ):
        """Inicializa el gestor de alertas.
        
        Args:
            monitor: Monitor de caché
            config: Configuración de la caché
            check_interval: Intervalo de chequeo en segundos
            alert_history_file: Archivo para guardar historial
            email_config: Configuración de email
            slack_config: Configuración de Slack
        """
        self.monitor = monitor
        self.config = config
        self.check_interval = check_interval
        self.alert_history_file = alert_history_file
        self.email_config = email_config
        self.slack_config = slack_config
        
        self.rules: List[AlertRule] = []
        self.alert_history: List[Dict[str, Any]] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        
        self._setup_default_rules()
        self._load_history()
    
    def _setup_default_rules(self) -> None:
        """Configura reglas por defecto."""
        # Alerta de disponibilidad del sistema
        self.add_rule(
            name='system_availability',
            condition=lambda stats: stats['system_availability'] < 90,
            message_template=(
                'System availability is below 90%: '
                'Current availability: {system_availability:.2f}%'
            ),
            severity='critical',
            notification_channels=['email', 'slack']
        )
        
        # Alerta de nodos caídos
        self.add_rule(
            name='unhealthy_nodes',
            condition=lambda stats: (
                stats['healthy_nodes'] / stats['total_nodes'] * 100 < 80
            ),
            message_template=(
                'Too many unhealthy nodes: '
                '{healthy_nodes}/{total_nodes} nodes are healthy'
            ),
            severity='critical',
            notification_channels=['email', 'slack']
        )
        
        # Alerta de latencia alta
        self.add_rule(
            name='high_latency',
            condition=lambda stats: stats['avg_latency'] > 100,
            message_template=(
                'High system latency detected: '
                'Average latency is {avg_latency:.2f}ms'
            ),
            severity='warning',
            notification_channels=['slack']
        )
        
        # Alerta de tasa de error
        self.add_rule(
            name='error_rate',
            condition=lambda stats: stats['error_rate'] > 5,
            message_template=(
                'High error rate detected: '
                'Error rate is {error_rate:.2f}%'
            ),
            severity='warning',
            notification_channels=['slack']
        )
    
    def add_rule(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        message_template: str,
        severity: str = 'warning',
        notification_channels: List[str] = None,
        cooldown: int = 300,
        aggregation_window: int = 60
    ) -> None:
        """Añade una nueva regla.
        
        Args:
            name: Nombre de la regla
            condition: Función que evalúa la condición
            message_template: Plantilla para el mensaje
            severity: Severidad de la alerta
            notification_channels: Canales de notificación
            cooldown: Tiempo de espera entre alertas
            aggregation_window: Ventana de agregación
        """
        rule = AlertRule(
            name=name,
            condition=condition,
            message_template=message_template,
            severity=severity,
            notification_channels=notification_channels,
            cooldown=cooldown,
            aggregation_window=aggregation_window
        )
        self.rules.append(rule)
    
    def start(self) -> None:
        """Inicia el monitoreo de alertas."""
        if self.running:
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._alert_loop)
        self.thread.daemon = True
        self.thread.start()
        
        logger.info("Alert manager started")
    
    def stop(self) -> None:
        """Detiene el monitoreo de alertas."""
        self.running = False
        if self.thread:
            self.thread.join()
            self.thread = None
        
        logger.info("Alert manager stopped")
    
    def _alert_loop(self) -> None:
        """Loop principal de alertas."""
        while self.running:
            try:
                self._check_alerts()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in alert loop: {str(e)}")
                time.sleep(self.check_interval)
    
    def _check_alerts(self) -> None:
        """Verifica las alertas."""
        system_health = self.monitor.get_system_health()
        
        for rule in self.rules:
            if rule.should_trigger(system_health):
                alert = {
                    'timestamp': datetime.now().isoformat(),
                    'rule_name': rule.name,
                    'severity': rule.severity,
                    'message': rule.format_message(system_health)
                }
                
                self._handle_alert(alert, rule.notification_channels)
                rule.last_triggered = datetime.now()
                rule.triggered_count += 1
                
                with self.lock:
                    self.alert_history.append(alert)
                    self._save_history()
    
    def _handle_alert(
        self,
        alert: Dict[str, Any],
        channels: List[str]
    ) -> None:
        """Maneja una alerta.
        
        Args:
            alert: Datos de la alerta
            channels: Canales de notificación
        """
        for channel in channels:
            try:
                if channel == 'log':
                    self._log_alert(alert)
                elif channel == 'email':
                    self._send_email_alert(alert)
                elif channel == 'slack':
                    self._send_slack_alert(alert)
            except Exception as e:
                logger.error(
                    f"Error sending alert through {channel}: {str(e)}"
                )
    
    def _log_alert(self, alert: Dict[str, Any]) -> None:
        """Registra una alerta en el log.
        
        Args:
            alert: Datos de la alerta
        """
        log_message = (
            f"[{alert['severity'].upper()}] {alert['rule_name']}: "
            f"{alert['message']}"
        )
        
        if alert['severity'] == 'critical':
            logger.critical(log_message)
        else:
            logger.warning(log_message)
    
    def _send_email_alert(self, alert: Dict[str, Any]) -> None:
        """Envía una alerta por email.
        
        Args:
            alert: Datos de la alerta
        """
        if not self.email_config:
            return
            
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['from']
            msg['To'] = self.email_config['to']
            msg['Subject'] = (
                f"Cache Alert: [{alert['severity'].upper()}] "
                f"{alert['rule_name']}"
            )
            
            body = (
                f"Time: {alert['timestamp']}\n"
                f"Severity: {alert['severity']}\n"
                f"Rule: {alert['rule_name']}\n"
                f"Message: {alert['message']}"
            )
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(
                self.email_config['smtp_host'],
                self.email_config['smtp_port']
            ) as server:
                if self.email_config.get('use_tls'):
                    server.starttls()
                
                if self.email_config.get('username'):
                    server.login(
                        self.email_config['username'],
                        self.email_config['password']
                    )
                
                server.send_message(msg)
                
        except Exception as e:
            logger.error(f"Error sending email alert: {str(e)}")
    
    def _send_slack_alert(self, alert: Dict[str, Any]) -> None:
        """Envía una alerta a Slack.
        
        Args:
            alert: Datos de la alerta
        """
        if not self.slack_config:
            return
            
        try:
            import requests
            
            color = (
                'danger' if alert['severity'] == 'critical'
                else 'warning'
            )
            
            payload = {
                'attachments': [{
                    'color': color,
                    'title': (
                        f"Cache Alert: [{alert['severity'].upper()}] "
                        f"{alert['rule_name']}"
                    ),
                    'text': alert['message'],
                    'fields': [
                        {
                            'title': 'Time',
                            'value': alert['timestamp'],
                            'short': True
                        },
                        {
                            'title': 'Severity',
                            'value': alert['severity'],
                            'short': True
                        }
                    ]
                }]
            }
            
            response = requests.post(
                self.slack_config['webhook_url'],
                json=payload
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error(f"Error sending Slack alert: {str(e)}")
    
    def _save_history(self) -> None:
        """Guarda el historial de alertas."""
        try:
            os.makedirs(os.path.dirname(self.alert_history_file), exist_ok=True)
            with open(self.alert_history_file, 'w') as f:
                json.dump(self.alert_history, f, indent=4)
        except Exception as e:
            logger.error(f"Error saving alert history: {str(e)}")
    
    def _load_history(self) -> None:
        """Carga el historial de alertas."""
        try:
            if os.path.exists(self.alert_history_file):
                with open(self.alert_history_file, 'r') as f:
                    self.alert_history = json.load(f)
        except Exception as e:
            logger.error(f"Error loading alert history: {str(e)}")
    
    def get_alert_history(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        severity: Optional[str] = None,
        rule_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Obtiene el historial de alertas filtrado.
        
        Args:
            start_time: Tiempo inicial
            end_time: Tiempo final
            severity: Severidad a filtrar
            rule_name: Nombre de regla a filtrar
            
        Returns:
            List[Dict[str, Any]]: Alertas filtradas
        """
        with self.lock:
            filtered = self.alert_history.copy()
            
            if start_time:
                filtered = [
                    alert for alert in filtered
                    if datetime.fromisoformat(alert['timestamp']) >= start_time
                ]
            
            if end_time:
                filtered = [
                    alert for alert in filtered
                    if datetime.fromisoformat(alert['timestamp']) <= end_time
                ]
            
            if severity:
                filtered = [
                    alert for alert in filtered
                    if alert['severity'] == severity
                ]
            
            if rule_name:
                filtered = [
                    alert for alert in filtered
                    if alert['rule_name'] == rule_name
                ]
            
            return filtered 