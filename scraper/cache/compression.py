"""Sistema de compresión para la caché."""

import logging
import zlib
import json
import pickle
from typing import Any, Dict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CompressionStats:
    """Estadísticas de compresión."""

    original_size: int
    compressed_size: int
    ratio: float


class CompressionManager:
    """Gestor de compresión básico."""

    def __init__(self, compression_level: int = 6):
        """Inicializa el gestor de compresión.

        Args:
            compression_level: Nivel de compresión (1-9)
        """
        self.compression_level = min(max(compression_level, 1), 9)
        self.stats: Dict[str, CompressionStats] = {}

    def compress(self, key: str, value: Any) -> bytes:
        """Comprime un valor.

        Args:
            key: Clave del valor
            value: Valor a comprimir

        Returns:
            bytes: Valor comprimido
        """
        serialized = self._serialize(value)
        compressed = zlib.compress(serialized, self.compression_level)

        # Registrar estadísticas
        self.stats[key] = CompressionStats(
            original_size=len(serialized),
            compressed_size=len(compressed),
            ratio=len(compressed) / len(serialized),
        )

        return compressed

    def decompress(self, data: bytes) -> Any:
        """Descomprime un valor.

        Args:
            data: Datos comprimidos

        Returns:
            Any: Valor descomprimido
        """
        decompressed = zlib.decompress(data)
        return self._deserialize(decompressed)

    def _serialize(self, value: Any) -> bytes:
        """Serializa un valor."""
        if isinstance(value, (str, int, float, bool)):
            return json.dumps(value).encode()
        return pickle.dumps(value)

    def _deserialize(self, data: bytes) -> Any:
        """Deserializa un valor."""
        try:
            return json.loads(data.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return pickle.loads(data)

    def get_stats(self) -> Dict[str, CompressionStats]:
        """Obtiene las estadísticas de compresión."""
        return self.stats.copy()

    def clear_stats(self) -> None:
        """Limpia las estadísticas."""
        self.stats.clear()
