"""Sistema de backup para la caché distribuida."""

import logging
import threading
import time
from typing import Dict, Any, Optional, Set, Union, List
from datetime import datetime
import json
import os
from pathlib import Path

from .events import EventManager, EventType, EventPriority, CacheEvent
from .compression import CompressionManager

logger = logging.getLogger(__name__)

class BackupManager:
    """Gestor de backups de caché."""
    
    def __init__(
        self,
        cache,  # DistributedCache
        backup_dir: str = 'backups/cache',
        backup_interval: int = 3600,  # 1 hora
        max_backups: int = 24,  # Mantener últimas 24 horas
        compression_manager: Optional[CompressionManager] = None,
        event_manager: Optional[EventManager] = None
    ):
        """Inicializa el gestor de backups.
        
        Args:
            cache: Instancia de caché distribuida
            backup_dir: Directorio para backups
            backup_interval: Intervalo entre backups en segundos
            max_backups: Número máximo de backups a mantener
            compression_manager: Gestor de compresión opcional
            event_manager: Gestor de eventos opcional
        """
        self.cache = cache
        self.backup_dir = Path(backup_dir)
        self.backup_interval = backup_interval
        self.max_backups = max_backups
        self.compression_manager = compression_manager
        self.event_manager = event_manager
        
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._modified_keys: Set[str] = set()
        
        # Crear directorio de backups
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Suscribirse a eventos si está disponible
        if self.event_manager:
            self.event_manager.subscribe(EventType.SET, self._on_cache_modified)
            self.event_manager.subscribe(EventType.DELETE, self._on_cache_modified)
    
    def start(self) -> None:
        """Inicia el proceso de backup."""
        if self.running:
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._backup_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info("Backup manager started")
    
    def stop(self) -> None:
        """Detiene el proceso de backup."""
        self.running = False
        if self.thread:
            self.thread.join()
            self.thread = None
        logger.info("Backup manager stopped")
    
    def _backup_loop(self) -> None:
        """Loop principal de backup."""
        while self.running:
            try:
                self._perform_backup()
                time.sleep(self.backup_interval)
            except Exception as e:
                logger.error(f"Error in backup loop: {str(e)}")
                if self.event_manager:
                    self.event_manager.publish(
                        EventType.ERROR,
                        metadata={
                            'operation': 'backup_loop',
                            'error': str(e)
                        },
                        priority=EventPriority.HIGH
                    )
                time.sleep(60)  # Esperar antes de reintentar
    
    def _perform_backup(self) -> None:
        """Ejecuta el backup de la caché."""
        try:
            # Generar nombre de archivo
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"cache_backup_{timestamp}.json"
            if self.compression_manager:
                filename += '.compressed'
            
            backup_path = self.backup_dir / filename
            
            # Obtener datos de la caché
            data = {
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    'total_keys': len(self.cache.get_keys()),
                    'modified_keys': len(self._modified_keys),
                    'compressed': bool(self.compression_manager)
                },
                'data': {}
            }
            
            # Guardar solo claves modificadas si hay alguna
            keys_to_backup = self._modified_keys if self._modified_keys else self.cache.get_keys()
            
            for key in keys_to_backup:
                try:
                    value = self.cache.get(key)
                    if value is not None:
                        data['data'][key] = value
                except Exception as e:
                    logger.error(f"Error backing up key {key}: {str(e)}")
            
            # Comprimir datos si está disponible
            if self.compression_manager:
                compressed_data = self.compression_manager.compress('backup', data)
                if compressed_data:
                    with open(backup_path, 'wb') as f:
                        f.write(compressed_data)
                else:
                    with open(backup_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2)
            else:
                with open(backup_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
            
            # Limpiar claves modificadas
            self._modified_keys.clear()
            
            # Eliminar backups antiguos
            self._cleanup_old_backups()
            
            # Publicar evento de backup exitoso
            if self.event_manager:
                self.event_manager.publish(
                    EventType.BACKUP,
                    metadata={
                        'operation': 'backup',
                        'filename': filename,
                        'total_keys': len(data['data']),
                        'size': os.path.getsize(backup_path)
                    },
                    priority=EventPriority.MEDIUM
                )
            
            logger.info(f"Backup completed: {filename}")
            
        except Exception as e:
            logger.error(f"Error performing backup: {str(e)}")
            if self.event_manager:
                self.event_manager.publish(
                    EventType.ERROR,
                    metadata={
                        'operation': 'backup',
                        'error': str(e)
                    },
                    priority=EventPriority.HIGH
                )
    
    def restore(self, filename: str) -> None:
        """Restaura un backup.
        
        Args:
            filename: Nombre del archivo de backup
        """
        try:
            backup_path = self.backup_dir / filename
            if not backup_path.exists():
                raise FileNotFoundError(f"Backup file not found: {filename}")
            
            # Cargar datos del backup
            if self.compression_manager and filename.endswith('.compressed'):
                with open(backup_path, 'rb') as f:
                    compressed_data = f.read()
                data = self.compression_manager.decompress(compressed_data)
            else:
                with open(backup_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            
            # Restaurar datos
            restored_count = 0
            for key, value in data['data'].items():
                try:
                    self.cache.set(key, value)
                    restored_count += 1
                except Exception as e:
                    logger.error(f"Error restoring key {key}: {str(e)}")
            
            # Publicar evento de restauración exitosa
            if self.event_manager:
                self.event_manager.publish(
                    EventType.RESTORE,
                    metadata={
                        'operation': 'restore',
                        'filename': filename,
                        'restored_keys': restored_count
                    },
                    priority=EventPriority.MEDIUM
                )
            
            logger.info(
                f"Restore completed from {filename}: "
                f"restored {restored_count} keys"
            )
            
        except Exception as e:
            error_msg = f"Error restoring backup: {str(e)}"
            logger.error(error_msg)
            if self.event_manager:
                self.event_manager.publish(
                    EventType.ERROR,
                    metadata={
                        'operation': 'restore',
                        'filename': filename,
                        'error': str(e)
                    },
                    priority=EventPriority.HIGH
                )
            raise RuntimeError(error_msg)
    
    def _cleanup_old_backups(self) -> None:
        """Elimina backups antiguos."""
        try:
            backups = sorted(
                self.backup_dir.glob('cache_backup_*.*'),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            
            # Mantener solo los últimos max_backups
            for backup in backups[self.max_backups:]:
                backup.unlink()
                logger.debug(f"Deleted old backup: {backup.name}")
                
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {str(e)}")
    
    def _on_cache_modified(self, event: CacheEvent) -> None:
        """Maneja eventos de modificación de caché.
        
        Args:
            event: Evento de caché
        """
        if event.key and event.type in {EventType.SET, EventType.DELETE}:
            self._modified_keys.add(event.key)
    
    def get_backup_list(self) -> List[Dict[str, Any]]:
        """Obtiene lista de backups disponibles.
        
        Returns:
            List[Dict[str, Any]]: Lista de backups
        """
        backups = []
        
        for backup_file in self.backup_dir.glob('cache_backup_*.*'):
            try:
                stats = backup_file.stat()
                
                if str(backup_file).endswith('.compressed'):
                    if self.compression_manager:
                        with open(backup_file, 'rb') as f:
                            compressed_data = f.read()
                        data = self.compression_manager.decompress(compressed_data)
                        metadata = data.get('metadata', {})
                    else:
                        logger.warning(
                            f"Found compressed backup but no compression manager: {backup_file}"
                        )
                        metadata = {}
                else:
                    with open(backup_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f).get('metadata', {})
                
                backups.append({
                    'filename': backup_file.name,
                    'size': stats.st_size,
                    'created_at': datetime.fromtimestamp(
                        stats.st_mtime
                    ).isoformat(),
                    'compressed': backup_file.suffix == '.compressed',
                    'metadata': metadata
                })
                
            except Exception as e:
                logger.error(
                    f"Error reading backup {backup_file}: {str(e)}"
                )
        
        return sorted(
            backups,
            key=lambda x: x['created_at'],
            reverse=True
        )
    
    def get_backup_size(self) -> int:
        """Obtiene tamaño total de backups en bytes.
        
        Returns:
            int: Tamaño total en bytes
        """
        return sum(
            f.stat().st_size
            for f in self.backup_dir.glob('cache_backup_*.*')
        ) 