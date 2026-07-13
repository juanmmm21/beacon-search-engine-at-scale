"""Tipos de datos de la fase 5: particionado de shards para servir consultas
distribuidas (`ShardIndexPipeline`) y réplica de un shard escalable en
infraestructura real (`ShardReplicaService`)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ShardIndexPipelineConfig:
    """Configuración de una ejecución de `ShardIndexPipeline.run()`.

    Job por lotes único, mismo criterio que `IndexingPipelineConfig`/
    `PageRankPipelineConfig` en fases 3 y 4: no hay `worker_id` ni consumidor
    de cola, se ejecuta una vez, después de que fase 3 haya dejado su índice
    global (comprimido o no) listo en `source_index_prefix` (ver
    `ARCHITECTURE.md`, fase 5).
    """

    bucket: str = "beacon-scale-dev"
    source_index_prefix: str = "search-index-compressed"
    shard_index_prefix: str = "shard-index"
    num_shards: int = 3

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket no puede estar vacío")
        if not self.source_index_prefix:
            raise ValueError("source_index_prefix no puede estar vacío")
        if not self.shard_index_prefix:
            raise ValueError("shard_index_prefix no puede estar vacío")
        if self.num_shards <= 0:
            raise ValueError("num_shards debe ser positivo")


@dataclass(frozen=True, slots=True)
class ShardIndexingStats:
    """Resumen final de una ejecución de `ShardIndexPipeline.run()`."""

    num_shards: int
    source_files_downloaded: int
    shard_files_uploaded: int


@dataclass(frozen=True, slots=True)
class ShardReplicaConfig:
    """Configuración de una réplica escalable de un shard (`ShardReplicaService`).

    Un `shard_id` puede tener varias réplicas vivas a la vez (distinto
    `replica_id` cada una) -- exactamente la redundancia que permite que la
    caída de una réplica no tumbe esa partición del índice completa (ver
    `ARCHITECTURE.md`, fase 5, sección "Réplicas por shard").
    """

    shard_id: int
    replica_id: str
    bucket: str = "beacon-scale-dev"
    shard_index_prefix: str = "shard-index"
    service_name: str = "beacon-scale-shard"
    host: str = "0.0.0.0"
    port: int = 9300
    announce_host: str | None = None
    ttl_seconds: float = 15.0
    heartbeat_interval_seconds: float = 5.0
    health_check_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.shard_id < 0:
            raise ValueError("shard_id no puede ser negativo")
        if not self.replica_id:
            raise ValueError("replica_id no puede estar vacío")
        if not self.bucket:
            raise ValueError("bucket no puede estar vacío")
        if not self.shard_index_prefix:
            raise ValueError("shard_index_prefix no puede estar vacío")
        if not self.service_name:
            raise ValueError("service_name no puede estar vacío")
        if not (0 < self.port <= 65535):
            raise ValueError(f"port fuera de rango: {self.port}")
        if self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds debe ser positivo")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds debe ser positivo")
        if self.heartbeat_interval_seconds >= self.ttl_seconds:
            raise ValueError(
                "heartbeat_interval_seconds debe ser menor que ttl_seconds, o el TTL "
                "expiraría entre dos heartbeats consecutivos"
            )

    @property
    def shard_object_prefix(self) -> str:
        return f"{self.shard_index_prefix}/shard-{self.shard_id}"

    @property
    def service_id(self) -> str:
        return f"{self.service_name}-shard-{self.shard_id}-{self.replica_id}"
