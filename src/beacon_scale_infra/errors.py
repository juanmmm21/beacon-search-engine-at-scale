"""Excepciones tipadas propias del sustrato compartido.

Ninguna implementación real (S3/MinIO, Redis Streams, Consul) deja escapar la
excepción cruda de su SDK: el llamador que ya vive en `distributed-index-sharding`
o en una futura fase de crawl distribuido no debería tener que saber si el
backend detrás de `ObjectStorage` es MinIO o S3 real, solo que
`ObjectStorageError` significa "esta operación de almacenamiento falló" (ver
`~/Desarrollo/beacon-search-engine/CLAUDE.md`, regla de I/O: todo error se
captura, se registra y se propaga como excepción tipada del propio módulo).
"""

from __future__ import annotations


class BeaconScaleInfraError(Exception):
    """Raíz común de todas las excepciones de este paquete."""


class ObjectStorageError(BeaconScaleInfraError):
    """Fallo al leer, escribir, listar o borrar un objeto."""


class ObjectNotFoundError(ObjectStorageError):
    """La clave solicitada no existe en el bucket."""


class MessageQueueError(BeaconScaleInfraError):
    """Fallo al publicar, consumir o confirmar un mensaje."""


class ServiceRegistryError(BeaconScaleInfraError):
    """Fallo al registrar, dar de baja o descubrir instancias de un servicio."""


class SharedDeduplicatorError(BeaconScaleInfraError):
    """Fallo al reclamar o consultar una URL en el deduplicador compartido."""


class CoordinatedRateLimiterError(BeaconScaleInfraError):
    """Fallo al adquirir o liberar un hueco del rate limiter coordinado."""


class IndexingError(BeaconScaleInfraError):
    """Fallo del pipeline de indexación distribuida (fase 3): una partición
    ilegible, un rango de `doc_id` inconsistente con el manifiesto de fase 2,
    o una colisión de `doc_id` entre particiones tras el remapeo -- nunca se
    deja escapar la excepción cruda de `inverted-index-builder` ni de
    `index-compression-codec` sin envolver (ver `ARCHITECTURE.md`, fase 3)."""


class PageRankPhaseError(BeaconScaleInfraError):
    """Fallo del pipeline de PageRank distribuido (fase 4): imposible
    descargar `search-index/documents.jsonl` de fase 3, o el grafo de enlaces
    materializado queda vacío pese a que `crawl-pages/` no lo está -- nunca se
    deja escapar la excepción cruda de `pagerank-link-analysis` sin envolver
    (ver `ARCHITECTURE.md`, fase 4)."""


class ShardIndexingError(BeaconScaleInfraError):
    """Fallo del job de particionado de shards (fase 5): no existe el índice
    global de fase 3 en el bucket esperado, o `distributed_index_sharding
    .partitioning.partition_index` no puede leerlo -- nunca se deja escapar
    la excepción cruda de `distributed-index-sharding` sin envolver (ver
    `ARCHITECTURE.md`, fase 5)."""


class QueryServingError(BeaconScaleInfraError):
    """Fallo de la capa de orquestación de query serving distribuido (fase
    5): ninguna réplica viva registrada para un `shard_id` conocido, una
    instancia descubierta sin metadata `shard_id` válida, o un fallo al
    registrar/dar de baja una réplica de shard en el `ServiceRegistry` --
    nunca confundido con `ServiceRegistryError` (que es del propio registro),
    ni con un `ShardOutcome` degradado de `distributed-index-sharding` (que
    es tolerancia a fallo esperada, no un error de esta capa; ver
    `ARCHITECTURE.md`, fase 5)."""


class CacheError(BeaconScaleInfraError):
    """Fallo al leer o escribir una entrada de la caché compartida (fase 6):
    conexión Redis rechazada, timeout, o un fallo del backend -- nunca se deja
    escapar la excepción cruda del SDK de Redis sin envolver. La capa de
    consola trata este error como degradación (sirve la búsqueda sin caché,
    registrándolo), nunca como fallo de la búsqueda misma (ver
    `ARCHITECTURE.md`, fase 6)."""


class ConsoleServingError(BeaconScaleInfraError):
    """Fallo de la capa de orquestación de la consola (fase 6): artefactos
    del índice ausentes o incompletos en el almacenamiento de objetos (¿han
    corrido 'build-index'/'shard-index'/'train-reranker'?), o un marcador de
    versión de índice ilegible -- nunca confundido con la degradación por
    shard caído, que es tolerancia a fallo esperada y viaja como datos en la
    respuesta de la API (`degraded`/`shard_statuses`), no como excepción."""
