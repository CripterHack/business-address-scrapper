"""Sistema de encriptación para la caché distribuida."""

import logging
import os
from typing import Any, Optional, Dict
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import pickle
import json

logger = logging.getLogger(__name__)

class EncryptionManager:
    """Gestor de encriptación de datos."""
    
    def __init__(
        self,
        secret_key: Optional[str] = None,
        salt: Optional[bytes] = None,
        iterations: int = 100000
    ):
        """Inicializa el gestor de encriptación.
        
        Args:
            secret_key: Clave secreta opcional
            salt: Salt opcional para derivación de clave
            iterations: Iteraciones para derivación de clave
        """
        self.secret_key = secret_key or os.getenv('CACHE_ENCRYPTION_KEY')
        if not self.secret_key:
            self.secret_key = self._generate_key()
            
        self.salt = salt or os.urandom(16)
        self.iterations = iterations
        self.fernet = self._initialize_fernet()
        
        # Registro de claves encriptadas
        self._encrypted_keys: Dict[str, bool] = {}
    
    def _generate_key(self) -> str:
        """Genera una nueva clave secreta.
        
        Returns:
            str: Clave secreta generada
        """
        return base64.urlsafe_b64encode(os.urandom(32)).decode()
    
    def _initialize_fernet(self) -> Fernet:
        """Inicializa Fernet con la clave derivada.
        
        Returns:
            Fernet: Instancia de Fernet
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self.salt,
            iterations=self.iterations
        )
        key = base64.urlsafe_b64encode(
            kdf.derive(self.secret_key.encode())
        )
        return Fernet(key)
    
    def should_encrypt(self, key: str, value: Any) -> bool:
        """Determina si un valor debe ser encriptado.
        
        Args:
            key: Clave del valor
            value: Valor a evaluar
            
        Returns:
            bool: True si el valor debe ser encriptado
        """
        # Patrones de claves sensibles
        sensitive_patterns = [
            'password', 'secret', 'token', 'key',
            'auth', 'credential', 'private'
        ]
        
        # Verificar patrones en la clave
        if any(pattern in key.lower() for pattern in sensitive_patterns):
            return True
        
        # Verificar si ya estaba encriptado
        if key in self._encrypted_keys:
            return True
        
        return False
    
    def encrypt(self, value: Any) -> bytes:
        """Encripta un valor.
        
        Args:
            value: Valor a encriptar
            
        Returns:
            bytes: Valor encriptado
        """
        try:
            # Serializar valor
            serialized = pickle.dumps(value)
            
            # Encriptar
            encrypted = self.fernet.encrypt(serialized)
            
            return encrypted
            
        except Exception as e:
            logger.error(f"Error encrypting value: {str(e)}")
            raise
    
    def decrypt(self, encrypted_value: bytes) -> Any:
        """Desencripta un valor.
        
        Args:
            encrypted_value: Valor encriptado
            
        Returns:
            Any: Valor desencriptado
        """
        try:
            # Desencriptar
            decrypted = self.fernet.decrypt(encrypted_value)
            
            # Deserializar
            value = pickle.loads(decrypted)
            
            return value
            
        except Exception as e:
            logger.error(f"Error decrypting value: {str(e)}")
            raise
    
    def encrypt_key(self, key: str) -> str:
        """Encripta una clave.
        
        Args:
            key: Clave a encriptar
            
        Returns:
            str: Clave encriptada
        """
        try:
            # Encriptar
            encrypted = self.fernet.encrypt(key.encode())
            
            # Codificar para usar como clave
            return base64.urlsafe_b64encode(encrypted).decode()
            
        except Exception as e:
            logger.error(f"Error encrypting key: {str(e)}")
            raise
    
    def decrypt_key(self, encrypted_key: str) -> str:
        """Desencripta una clave.
        
        Args:
            encrypted_key: Clave encriptada
            
        Returns:
            str: Clave original
        """
        try:
            # Decodificar
            encrypted = base64.urlsafe_b64decode(encrypted_key.encode())
            
            # Desencriptar
            key = self.fernet.decrypt(encrypted).decode()
            
            return key
            
        except Exception as e:
            logger.error(f"Error decrypting key: {str(e)}")
            raise
    
    def mark_as_encrypted(self, key: str) -> None:
        """Marca una clave como encriptada.
        
        Args:
            key: Clave a marcar
        """
        self._encrypted_keys[key] = True
    
    def is_encrypted(self, key: str) -> bool:
        """Verifica si una clave está encriptada.
        
        Args:
            key: Clave a verificar
            
        Returns:
            bool: True si la clave está encriptada
        """
        return key in self._encrypted_keys
    
    def rotate_key(self) -> None:
        """Rota la clave de encriptación."""
        try:
            # Generar nueva clave
            new_key = self._generate_key()
            new_salt = os.urandom(16)
            
            # Inicializar nuevo Fernet
            old_fernet = self.fernet
            self.secret_key = new_key
            self.salt = new_salt
            self.fernet = self._initialize_fernet()
            
            logger.info("Encryption key rotated successfully")
            
        except Exception as e:
            logger.error(f"Error rotating encryption key: {str(e)}")
            # Restaurar estado anterior
            self.fernet = old_fernet
            raise
    
    def get_encryption_info(self) -> Dict[str, Any]:
        """Obtiene información de encriptación.
        
        Returns:
            Dict[str, Any]: Información de encriptación
        """
        return {
            'encrypted_keys': len(self._encrypted_keys),
            'salt': base64.b64encode(self.salt).decode(),
            'iterations': self.iterations
        } 