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
