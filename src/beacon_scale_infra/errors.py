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
