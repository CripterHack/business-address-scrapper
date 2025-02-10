"""Sistema de autenticación para la caché distribuida."""

import logging
import time
from typing import Dict, Any, Optional, Set, List
from datetime import datetime, timedelta
import threading
import hashlib
import hmac
import os
import json
import re
from pathlib import Path

logger = logging.getLogger(__name__)

class Permission:
    """Permisos disponibles."""
    READ = 'read'
    WRITE = 'write'
    DELETE = 'delete'
    ADMIN = 'admin'

class Role:
    """Roles predefinidos."""
    ADMIN = 'admin'
    WRITER = 'writer'
    READER = 'reader'

class RateLimiter:
    """Limitador de tasa de peticiones."""
    
    def __init__(
        self,
        max_requests: int = 10,
        time_window: int = 60  # segundos
    ):
        """Inicializa el limitador.
        
        Args:
            max_requests: Máximo de peticiones permitidas
            time_window: Ventana de tiempo en segundos
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: Dict[str, List[datetime]] = {}
        self._lock = threading.Lock()
    
    def is_allowed(self, key: str) -> bool:
        """Verifica si se permite una petición.
        
        Args:
            key: Identificador de la petición
            
        Returns:
            bool: True si se permite
        """
        with self._lock:
            now = datetime.now()
            
            # Limpiar peticiones antiguas
            if key in self.requests:
                self.requests[key] = [
                    req_time for req_time in self.requests[key]
                    if now - req_time < timedelta(seconds=self.time_window)
                ]
            else:
                self.requests[key] = []
            
            # Verificar límite
            if len(self.requests[key]) >= self.max_requests:
                return False
            
            # Registrar petición
            self.requests[key].append(now)
            return True

class PasswordValidator:
    """Validador de contraseñas."""
    
    def __init__(
        self,
        min_length: int = 8,
        require_numbers: bool = True,
        require_special: bool = True,
        require_uppercase: bool = True,
        require_lowercase: bool = True
    ):
        """Inicializa el validador.
        
        Args:
            min_length: Longitud mínima
            require_numbers: Requerir números
            require_special: Requerir caracteres especiales
            require_uppercase: Requerir mayúsculas
            require_lowercase: Requerir minúsculas
        """
        self.min_length = min_length
        self.require_numbers = require_numbers
        self.require_special = require_special
        self.require_uppercase = require_uppercase
        self.require_lowercase = require_lowercase
    
    def validate(self, password: str) -> tuple[bool, str]:
        """Valida una contraseña.
        
        Args:
            password: Contraseña a validar
            
        Returns:
            tuple[bool, str]: (válida, mensaje)
        """
        if len(password) < self.min_length:
            return False, f"Password must be at least {self.min_length} characters"
        
        if self.require_numbers and not re.search(r"\d", password):
            return False, "Password must contain at least one number"
        
        if self.require_special and not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
            return False, "Password must contain at least one special character"
        
        if self.require_uppercase and not re.search(r"[A-Z]", password):
            return False, "Password must contain at least one uppercase letter"
        
        if self.require_lowercase and not re.search(r"[a-z]", password):
            return False, "Password must contain at least one lowercase letter"
        
        return True, "Password is valid"

class AuthManager:
    """Gestor de autenticación y autorización."""
    
    def __init__(
        self,
        auth_file: Optional[str] = None,
        token_expiry: int = 3600,  # 1 hora
        max_failed_attempts: int = 5,
        lockout_duration: int = 300,  # 5 minutos
        rate_limit_max: int = 10,
        rate_limit_window: int = 60
    ):
        """Inicializa el gestor de autenticación.
        
        Args:
            auth_file: Archivo de configuración opcional
            token_expiry: Tiempo de expiración de tokens en segundos
            max_failed_attempts: Máximo de intentos fallidos
            lockout_duration: Duración del bloqueo en segundos
            rate_limit_max: Máximo de peticiones por ventana
            rate_limit_window: Ventana de tiempo para rate limit
        """
        self.auth_file = auth_file or 'config/auth.json'
        self.token_expiry = token_expiry
        self.max_failed_attempts = max_failed_attempts
        self.lockout_duration = lockout_duration
        
        # Estado interno
        self.users: Dict[str, Dict[str, Any]] = {}
        self.tokens: Dict[str, Dict[str, Any]] = {}
        self.failed_attempts: Dict[str, List[datetime]] = {}
        self.lockouts: Dict[str, datetime] = {}
        self._lock = threading.Lock()
        
        # Rate limiting
        self.rate_limiter = RateLimiter(rate_limit_max, rate_limit_window)
        
        # Validación de contraseñas
        self.password_validator = PasswordValidator()
        
        # Roles predefinidos
        self.role_permissions = {
            Role.ADMIN: {
                Permission.READ,
                Permission.WRITE,
                Permission.DELETE,
                Permission.ADMIN
            },
            Role.WRITER: {
                Permission.READ,
                Permission.WRITE,
                Permission.DELETE
            },
            Role.READER: {
                Permission.READ
            }
        }
        
        # Cargar configuración
        self._load_config()
    
    def _load_config(self) -> None:
        """Carga la configuración de autenticación."""
        try:
            config_path = Path(self.auth_file)
            if config_path.exists():
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    self.users = config.get('users', {})
            else:
                # Crear configuración por defecto
                self._create_default_config()
                
        except Exception as e:
            logger.error(f"Error loading auth config: {str(e)}")
            # Crear configuración por defecto
            self._create_default_config()
    
    def _create_default_config(self) -> None:
        """Crea configuración por defecto."""
        try:
            # Crear usuario admin por defecto
            admin_password = os.urandom(16).hex()
            self.create_user(
                username='admin',
                password=admin_password,
                role=Role.ADMIN
            )
            
            logger.info(
                f"Created default admin user with password: {admin_password}"
            )
            
        except Exception as e:
            logger.error(f"Error creating default config: {str(e)}")
    
    def _save_config(self) -> None:
        """Guarda la configuración de autenticación."""
        try:
            config_path = Path(self.auth_file)
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(config_path, 'w') as f:
                json.dump({'users': self.users}, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving auth config: {str(e)}")
    
    def create_user(
        self,
        username: str,
        password: str,
        role: str = Role.READER
    ) -> None:
        """Crea un nuevo usuario.
        
        Args:
            username: Nombre de usuario
            password: Contraseña
            role: Rol del usuario
        """
        # Validar contraseña
        valid, message = self.password_validator.validate(password)
        if not valid:
            raise ValueError(message)
        
        with self._lock:
            if username in self.users:
                raise ValueError(f"User {username} already exists")
            
            # Generar salt
            salt = os.urandom(16)
            
            # Hashear contraseña
            password_hash = self._hash_password(password, salt)
            
            # Crear usuario
            self.users[username] = {
                'password_hash': password_hash,
                'salt': salt.hex(),
                'role': role,
                'created_at': datetime.now().isoformat(),
                'last_password_change': datetime.now().isoformat()
            }
            
            # Guardar configuración
            self._save_config()
    
    def authenticate(
        self,
        username: str,
        password: str
    ) -> Optional[str]:
        """Autentica un usuario.
        
        Args:
            username: Nombre de usuario
            password: Contraseña
            
        Returns:
            Optional[str]: Token de acceso o None si falla
        """
        # Verificar rate limit
        if not self.rate_limiter.is_allowed(f"auth_{username}"):
            logger.warning(f"Rate limit exceeded for user {username}")
            return None
        
        with self._lock:
            # Verificar bloqueo
            if self._is_locked_out(username):
                logger.warning(f"User {username} is locked out")
                return None
            
            # Verificar usuario
            user = self.users.get(username)
            if not user:
                self._record_failed_attempt(username)
                return None
            
            # Verificar contraseña
            salt = bytes.fromhex(user['salt'])
            if user['password_hash'] != self._hash_password(password, salt):
                self._record_failed_attempt(username)
                return None
            
            # Generar token
            token = os.urandom(32).hex()
            self.tokens[token] = {
                'username': username,
                'created_at': datetime.now(),
                'expires_at': datetime.now() + timedelta(
                    seconds=self.token_expiry
                )
            }
            
            # Limpiar intentos fallidos
            self.failed_attempts.pop(username, None)
            self.lockouts.pop(username, None)
            
            return token
    
    def validate_token(self, token: str) -> bool:
        """Valida un token de acceso.
        
        Args:
            token: Token a validar
            
        Returns:
            bool: True si el token es válido
        """
        with self._lock:
            token_data = self.tokens.get(token)
            if not token_data:
                return False
            
            # Verificar expiración
            if datetime.now() > token_data['expires_at']:
                del self.tokens[token]
                return False
            
            return True
    
    def get_user_permissions(self, token: str) -> Set[str]:
        """Obtiene permisos de un usuario.
        
        Args:
            token: Token de acceso
            
        Returns:
            Set[str]: Conjunto de permisos
        """
        with self._lock:
            token_data = self.tokens.get(token)
            if not token_data:
                return set()
            
            user = self.users.get(token_data['username'])
            if not user:
                return set()
            
            return self.role_permissions.get(user['role'], set())
    
    def has_permission(
        self,
        token: str,
        permission: str
    ) -> bool:
        """Verifica si un usuario tiene un permiso.
        
        Args:
            token: Token de acceso
            permission: Permiso a verificar
            
        Returns:
            bool: True si tiene el permiso
        """
        return permission in self.get_user_permissions(token)
    
    def _hash_password(self, password: str, salt: bytes) -> str:
        """Hashea una contraseña.
        
        Args:
            password: Contraseña
            salt: Salt para el hash
            
        Returns:
            str: Hash de la contraseña
        """
        return hashlib.pbkdf2_hmac(
            'sha256',
            password.encode(),
            salt,
            100000
        ).hex()
    
    def _record_failed_attempt(self, username: str) -> None:
        """Registra un intento fallido.
        
        Args:
            username: Nombre de usuario
        """
        now = datetime.now()
        
        if username not in self.failed_attempts:
            self.failed_attempts[username] = []
        
        # Limpiar intentos antiguos
        self.failed_attempts[username] = [
            attempt for attempt in self.failed_attempts[username]
            if now - attempt < timedelta(minutes=30)
        ]
        
        # Registrar nuevo intento
        self.failed_attempts[username].append(now)
        
        # Verificar bloqueo
        if len(self.failed_attempts[username]) >= self.max_failed_attempts:
            self.lockouts[username] = now
            logger.warning(f"User {username} has been locked out")
    
    def _is_locked_out(self, username: str) -> bool:
        """Verifica si un usuario está bloqueado.
        
        Args:
            username: Nombre de usuario
            
        Returns:
            bool: True si está bloqueado
        """
        lockout_time = self.lockouts.get(username)
        if not lockout_time:
            return False
        
        # Verificar si expiró el bloqueo
        if datetime.now() - lockout_time > timedelta(
            seconds=self.lockout_duration
        ):
            self.lockouts.pop(username)
            return False
        
        return True
    
    def get_user_info(self, username: str) -> Optional[Dict[str, Any]]:
        """Obtiene información de un usuario.
        
        Args:
            username: Nombre de usuario
            
        Returns:
            Optional[Dict[str, Any]]: Información del usuario
        """
        user = self.users.get(username)
        if not user:
            return None
        
        return {
            'username': username,
            'role': user['role'],
            'created_at': user['created_at'],
            'is_locked': self._is_locked_out(username),
            'failed_attempts': len(
                self.failed_attempts.get(username, [])
            )
        }
    
    def get_active_tokens(self) -> List[Dict[str, Any]]:
        """Obtiene tokens activos.
        
        Returns:
            List[Dict[str, Any]]: Lista de tokens
        """
        now = datetime.now()
        active_tokens = []
        
        for token, data in self.tokens.items():
            if data['expires_at'] > now:
                active_tokens.append({
                    'token': token[:8] + '...',  # Truncar por seguridad
                    'username': data['username'],
                    'created_at': data['created_at'].isoformat(),
                    'expires_at': data['expires_at'].isoformat()
                })
        
        return active_tokens
    
    def change_password(
        self,
        username: str,
        old_password: str,
        new_password: str
    ) -> bool:
        """Cambia la contraseña de un usuario.
        
        Args:
            username: Nombre de usuario
            old_password: Contraseña actual
            new_password: Nueva contraseña
            
        Returns:
            bool: True si el cambio fue exitoso
        """
        # Validar nueva contraseña
        valid, message = self.password_validator.validate(new_password)
        if not valid:
            raise ValueError(message)
        
        with self._lock:
            user = self.users.get(username)
            if not user:
                return False
            
            # Verificar contraseña actual
            salt = bytes.fromhex(user['salt'])
            if user['password_hash'] != self._hash_password(old_password, salt):
                return False
            
            # Generar nuevo salt y hash
            new_salt = os.urandom(16)
            new_hash = self._hash_password(new_password, new_salt)
            
            # Actualizar usuario
            user['password_hash'] = new_hash
            user['salt'] = new_salt.hex()
            user['last_password_change'] = datetime.now().isoformat()
            
            # Invalidar tokens existentes
            self._invalidate_user_tokens(username)
            
            # Guardar configuración
            self._save_config()
            
            return True
    
    def _invalidate_user_tokens(self, username: str) -> None:
        """Invalida todos los tokens de un usuario.
        
        Args:
            username: Nombre de usuario
        """
        invalid_tokens = [
            token for token, data in self.tokens.items()
            if data['username'] == username
        ]
        for token in invalid_tokens:
            self.tokens.pop(token, None)
    
    def require_password_change(self, username: str) -> None:
        """Marca un usuario para cambio obligatorio de contraseña.
        
        Args:
            username: Nombre de usuario
        """
        with self._lock:
            user = self.users.get(username)
            if user:
                user['require_password_change'] = True
                self._save_config()
    
    def get_security_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de seguridad.
        
        Returns:
            Dict[str, Any]: Estadísticas
        """
        return {
            'total_users': len(self.users),
            'active_tokens': len(self.tokens),
            'locked_users': len(self.lockouts),
            'failed_attempts': {
                username: len(attempts)
                for username, attempts in self.failed_attempts.items()
            }
        } 