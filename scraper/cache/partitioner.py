"""Sistema de particionamiento para la caché distribuida."""

import hashlib
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class Partition:
    """Representa una partición de la caché."""
    id: str
    start_range: int
    end_range: int
    node_id: str
    replica_nodes: List[str]

class ConsistentHashPartitioner:
    """Implementa particionamiento mediante consistent hashing."""
    
    def __init__(self, nodes: List[Dict[str, Any]], virtual_nodes: int = 256):
        """Inicializa el particionador.
        
        Args:
            nodes: Lista de nodos de caché
            virtual_nodes: Número de nodos virtuales por nodo real
        """
        self.nodes = nodes
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self._build_ring()
    
    def _build_ring(self) -> None:
        """Construye el anillo de hashing."""
        self.ring.clear()
        for node in self.nodes:
            for i in range(self.virtual_nodes):
                hash_key = f"{node['id']}:{i}"
                hash_value = self._hash(hash_key)
                self.ring[hash_value] = node['id']
        
        logger.info(f"Built consistent hash ring with {len(self.ring)} points")
    
    def _hash(self, key: str) -> int:
        """Calcula el hash de una clave.
        
        Args:
            key: Clave a hashear
            
        Returns:
            int: Valor hash
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def get_partition(self, key: str) -> Partition:
        """Obtiene la partición para una clave.
        
        Args:
            key: Clave
            
        Returns:
            Partition: Partición asignada
        """
        if not self.ring:
            raise ValueError("Hash ring is empty")
        
        hash_value = self._hash(key)
        
        # Encontrar el siguiente punto en el anillo
        for point in sorted(self.ring.keys()):
            if hash_value <= point:
                node_id = self.ring[point]
                # Obtener nodos réplica
                replica_nodes = self._get_replica_nodes(node_id)
                return Partition(
                    id=f"partition-{hash_value}",
                    start_range=hash_value,
                    end_range=point,
                    node_id=node_id,
                    replica_nodes=replica_nodes
                )
        
        # Si no se encontró un punto mayor, usar el primero
        first_point = min(self.ring.keys())
        node_id = self.ring[first_point]
        replica_nodes = self._get_replica_nodes(node_id)
        return Partition(
            id=f"partition-{hash_value}",
            start_range=hash_value,
            end_range=first_point,
            node_id=node_id,
            replica_nodes=replica_nodes
        )
    
    def _get_replica_nodes(self, primary_node_id: str) -> List[str]:
        """Obtiene los nodos réplica para un nodo primario.
        
        Args:
            primary_node_id: ID del nodo primario
            
        Returns:
            List[str]: Lista de IDs de nodos réplica
        """
        node_ids = [node['id'] for node in self.nodes]
        primary_index = node_ids.index(primary_node_id)
        replicas = []
        
        # Tomar los siguientes 2 nodos como réplicas
        for i in range(1, 3):
            replica_index = (primary_index + i) % len(node_ids)
            replicas.append(node_ids[replica_index])
        
        return replicas
    
    def rebalance(self) -> Dict[str, List[str]]:
        """Rebalancea las particiones.
        
        Returns:
            Dict[str, List[str]]: Mapa de movimientos de datos
        """
        old_ring = self.ring.copy()
        self._build_ring()
        
        # Identificar particiones que necesitan moverse
        movements: Dict[str, List[str]] = {}
        
        for key in sorted(old_ring.keys()):
            old_node = old_ring[key]
            new_node = self.ring.get(key)
            
            if new_node and old_node != new_node:
                if old_node not in movements:
                    movements[old_node] = []
                movements[old_node].append(new_node)
        
        return movements

class RangePartitioner:
    """Implementa particionamiento por rangos."""
    
    def __init__(self, nodes: List[Dict[str, Any]], num_partitions: int = 1024):
        """Inicializa el particionador.
        
        Args:
            nodes: Lista de nodos de caché
            num_partitions: Número de particiones
        """
        self.nodes = nodes
        self.num_partitions = num_partitions
        self.partitions: List[Partition] = []
        self._create_partitions()
    
    def _create_partitions(self) -> None:
        """Crea las particiones."""
        range_size = int((1 << 128) / self.num_partitions)  # Usar 128 bits
        node_count = len(self.nodes)
        
        for i in range(self.num_partitions):
            start_range = i * range_size
            end_range = (i + 1) * range_size - 1
            
            # Asignar nodo primario
            primary_index = i % node_count
            node_id = self.nodes[primary_index]['id']
            
            # Asignar réplicas
            replica_nodes = []
            for j in range(1, 3):  # 2 réplicas
                replica_index = (primary_index + j) % node_count
                replica_nodes.append(self.nodes[replica_index]['id'])
            
            self.partitions.append(Partition(
                id=f"partition-{i}",
                start_range=start_range,
                end_range=end_range,
                node_id=node_id,
                replica_nodes=replica_nodes
            ))
        
        logger.info(f"Created {len(self.partitions)} partitions")
    
    def get_partition(self, key: str) -> Partition:
        """Obtiene la partición para una clave.
        
        Args:
            key: Clave
            
        Returns:
            Partition: Partición asignada
        """
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        
        for partition in self.partitions:
            if partition.start_range <= hash_value <= partition.end_range:
                return partition
        
        # Si no se encuentra (no debería ocurrir)
        raise ValueError(f"No partition found for key: {key}")
    
    def rebalance(self) -> Dict[str, List[str]]:
        """Rebalancea las particiones.
        
        Returns:
            Dict[str, List[str]]: Mapa de movimientos de datos
        """
        old_partitions = self.partitions.copy()
        self._create_partitions()
        
        # Identificar movimientos necesarios
        movements: Dict[str, List[str]] = {}
        
        for old_part, new_part in zip(old_partitions, self.partitions):
            if old_part.node_id != new_part.node_id:
                if old_part.node_id not in movements:
                    movements[old_part.node_id] = []
                movements[old_part.node_id].append(new_part.node_id)
        
        return movements

def create_partitioner(
    strategy: str,
    nodes: List[Dict[str, Any]],
    **kwargs: Any
) -> ConsistentHashPartitioner | RangePartitioner:
    """Crea un particionador según la estrategia especificada.
    
    Args:
        strategy: Estrategia de particionamiento ('consistent_hash' o 'range')
        nodes: Lista de nodos
        **kwargs: Argumentos adicionales
        
    Returns:
        Particionador configurado
    """
    if strategy == 'consistent_hash':
        return ConsistentHashPartitioner(
            nodes,
            virtual_nodes=kwargs.get('virtual_nodes', 256)
        )
    elif strategy == 'range':
        return RangePartitioner(
            nodes,
            num_partitions=kwargs.get('num_partitions', 1024)
        )
    else:
        raise ValueError(f"Unknown partitioning strategy: {strategy}") 