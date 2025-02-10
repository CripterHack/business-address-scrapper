"""Configuración centralizada para el sistema de caché distribuida."""

from typing import Dict, Any, Optional, List
from pathlib import Path
import os
import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

class NodeConfig(BaseModel):
    """Configuración de un nodo de caché."""
    id: str
    type: str = Field(default="redis")
    host: str = Field(default="localhost")
    port: int = Field(default=6379)
    db: int = Field(default=0)
    password: Optional[str] = None
    timeout: int = Field(default=5)
    
    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Valida el tipo de nodo."""
        valid_types = ["redis", "memcached"]
        if v not in valid_types:
            raise ValueError(f"El tipo debe ser uno de: {valid_types}")
        return v
    
    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Valida el puerto."""
        if not (1 <= v <= 65535):
            raise ValueError("El puerto debe estar entre 1 y 65535")
        return v
    
    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        """Valida el timeout."""
        if v < 0:
            raise ValueError("El timeout debe ser positivo")
        return v

class PartitioningConfig(BaseModel):
    """Configuración de particionamiento."""
    strategy: str = Field(default="consistent_hash")
    virtual_nodes: int = Field(default=256)
    num_partitions: int = Field(default=1024)
    
    @field_validator("strategy")
    @classmethod
    def validate_strategy(cls, v: str) -> str:
        """Valida la estrategia de particionamiento."""
        valid_strategies = ["consistent_hash", "range"]
        if v not in valid_strategies:
            raise ValueError(f"La estrategia debe ser una de: {valid_strategies}")
        return v
    
    @field_validator("virtual_nodes", "num_partitions")
    @classmethod
    def validate_positive(cls, v: int) -> int:
        """Valida números positivos."""
        if v <= 0:
            raise ValueError("El valor debe ser positivo")
        return v

class CompressionConfig(BaseModel):
    """Configuración de compresión."""
    enabled: bool = Field(default=True)
    algorithm: str = Field(default="zlib")
    min_size: int = Field(default=1024)  # 1KB
    max_size: int = Field(default=100 * 1024 * 1024)  # 100MB
    threshold: float = Field(default=0.9)  # 90%
    level: int = Field(default=6)
    
    @field_validator("algorithm")
    @classmethod
    def validate_algorithm(cls, v: str) -> str:
        """Valida el algoritmo de compresión."""
        valid_algorithms = ["zlib", "lz4"]
        if v not in valid_algorithms:
            raise ValueError(f"El algoritmo debe ser uno de: {valid_algorithms}")
        return v
        
    @model_validator(mode='after')
    def validate_level(self) -> 'CompressionConfig':
        """Valida el nivel de compresión según el algoritmo."""
        v = self.level
        if self.algorithm == "zlib" and not (1 <= v <= 9):
            raise ValueError("El nivel para zlib debe estar entre 1 y 9")
        elif self.algorithm == "lz4" and not (1 <= v <= 16):
            raise ValueError("El nivel para lz4 debe estar entre 1 y 16")
        return self
        
    @field_validator("threshold")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        """Valida el umbral de compresión."""
        if not (0.0 < v <= 1.0):
            raise ValueError("El umbral debe estar entre 0.0 y 1.0")
        return v
        
    @field_validator("min_size", "max_size")
    @classmethod
    def validate_sizes(cls, v: int) -> int:
        """Valida los tamaños mínimo y máximo."""
        if v <= 0:
            raise ValueError("El valor debe ser mayor que 0")
        return v
        
    @model_validator(mode='after')
    def validate_max_size(self) -> 'CompressionConfig':
        """Valida que max_size sea mayor que min_size."""
        if self.max_size <= self.min_size:
            raise ValueError("max_size debe ser mayor que min_size")
        return self

class SecurityConfig(BaseModel):
    """Configuración de seguridad."""
    encryption_enabled: bool = Field(default=True)
    encryption_key: Optional[str] = None
    auth_file: str = Field(default="config/auth.json")
    token_expiry: int = Field(default=3600)  # 1 hora
    max_failed_attempts: int = Field(default=5)
    lockout_duration: int = Field(default=300)  # 5 minutos
    
    @field_validator("token_expiry", "max_failed_attempts", "lockout_duration")
    @classmethod
    def validate_positive(cls, v: int) -> int:
        """Valida números positivos."""
        if v <= 0:
            raise ValueError("El valor debe ser positivo")
        return v
    
    @model_validator(mode='after')
    def validate_encryption_key(self) -> 'SecurityConfig':
        """Valida la clave de encriptación."""
        if self.encryption_enabled and not self.encryption_key:
            raise ValueError("Se requiere clave de encriptación cuando está habilitada")
        return self

class MetricsConfig(BaseModel):
    """Configuración de métricas."""
    enabled: bool = Field(default=True)
    log_dir: str = Field(default="logs")
    max_size_mb: int = Field(default=100)
    backup_count: int = Field(default=5)
    collection_interval: int = Field(default=60)
    
    @field_validator("max_size_mb", "backup_count", "collection_interval")
    @classmethod
    def validate_positive(cls, v: int) -> int:
        """Valida números positivos."""
        if v <= 0:
            raise ValueError("El valor debe ser positivo")
        return v

class CacheConfig(BaseModel):
    """Configuración principal de la caché."""
    enabled: bool = Field(default=True)
    nodes: List[NodeConfig]
    replication_factor: int = Field(default=2)
    consistency_level: str = Field(default="quorum")
    retry_attempts: int = Field(default=3)
    retry_delay: int = Field(default=1)
    partitioning: PartitioningConfig = Field(default_factory=PartitioningConfig)
    compression: CompressionConfig = Field(default_factory=CompressionConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    
    @field_validator("consistency_level")
    @classmethod
    def validate_consistency(cls, v: str) -> str:
        """Valida el nivel de consistencia."""
        valid_levels = ["one", "quorum", "all"]
        if v not in valid_levels:
            raise ValueError(f"El nivel de consistencia debe ser uno de: {valid_levels}")
        return v
    
    @model_validator(mode='after')
    def validate_replication(self) -> 'CacheConfig':
        """Valida el factor de replicación."""
        if self.replication_factor > len(self.nodes):
            raise ValueError(
                f"El factor de replicación ({self.replication_factor}) no puede ser mayor que "
                f"el número de nodos ({len(self.nodes)})"
            )
        return self
    
    @field_validator("retry_attempts", "retry_delay")
    @classmethod
    def validate_positive(cls, v: int) -> int:
        """Valida números positivos."""
        if v <= 0:
            raise ValueError("El valor debe ser positivo")
        return v

def load_config(config_path: Optional[str] = None) -> CacheConfig:
    """Carga la configuración desde archivo.
    
    Args:
        config_path: Ruta al archivo de configuración
        
    Returns:
        CacheConfig: Configuración cargada
        
    Raises:
        ValueError: Si hay error al cargar la configuración
    """
    # Usar archivo por defecto si no se especifica
    if not config_path:
        config_path = os.getenv('CACHE_CONFIG_PATH', 'config/cache.yaml')
    
    try:
        # Cargar archivo
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Validar y crear configuración
        return CacheConfig(**config_data)
        
    except FileNotFoundError:
        # Crear configuración por defecto
        return _create_default_config()
    except Exception as e:
        raise ValueError(f"Error cargando configuración: {str(e)}")

def _create_default_config() -> CacheConfig:
    """Crea una configuración por defecto.
    
    Returns:
        CacheConfig: Configuración por defecto
    """
    return CacheConfig(
        nodes=[
            NodeConfig(
                id="redis-1",
                type="redis",
                host="localhost",
                port=6379
            ),
            NodeConfig(
                id="redis-2",
                type="redis",
                host="localhost",
                port=6380
            )
        ]
    )

def save_config(config: CacheConfig, config_path: Optional[str] = None) -> None:
    """Guarda la configuración a archivo.
    
    Args:
        config: Configuración a guardar
        config_path: Ruta al archivo de configuración
        
    Raises:
        ValueError: Si hay error al guardar la configuración
    """
    if not config_path:
        config_path = os.getenv('CACHE_CONFIG_PATH', 'config/cache.yaml')
    
    try:
        # Crear directorio si no existe
        Path(config_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Guardar configuración
        with open(config_path, 'w') as f:
            yaml.dump(config.dict(), f, indent=2)
            
    except Exception as e:
        raise ValueError(f"Error guardando configuración: {str(e)}")

def validate_config(config: CacheConfig) -> List[str]:
    """Valida una configuración.
    
    Args:
        config: Configuración a validar
        
    Returns:
        List[str]: Lista de errores encontrados
    """
    errors = []
    
    # Validar nodos
    if not config.nodes:
        errors.append("Se requiere al menos un nodo")
    
    # Validar factor de replicación
    if config.replication_factor > len(config.nodes):
        errors.append(
            f"El factor de replicación ({config.replication_factor}) no puede ser "
            f"mayor que el número de nodos ({len(config.nodes)})"
        )
    
    # Validar compresión
    if config.compression.enabled:
        try:
            config.compression.validate_algorithm(config.compression.algorithm)
            config.compression.validate_level(
                config.compression.level,
                {"algorithm": config.compression.algorithm}
            )
            config.compression.validate_threshold(config.compression.threshold)
            config.compression.validate_sizes(config.compression.min_size, Field(name="min_size"))
            config.compression.validate_sizes(config.compression.max_size, Field(name="max_size"))
            config.compression.validate_max_size(
                config.compression.max_size,
                {"min_size": config.compression.min_size}
            )
        except ValueError as e:
            errors.append(f"Error en configuración de compresión: {str(e)}")
    
    # Validar seguridad
    if config.security.encryption_enabled and not config.security.encryption_key:
        errors.append("Se requiere clave de encriptación cuando la encriptación está habilitada")
    
    return errors 