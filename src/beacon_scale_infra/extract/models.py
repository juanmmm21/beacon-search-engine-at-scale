"""Tipos de datos de la extracción distribuida (fase 2).

`ExtractJob` es el contrato serializado que viaja por el `MessageQueue`
compartido entre `CrawlWorker` (fase 1, que lo publica tras escribir cada
página) y `ExtractWorker` (fase 2, que lo consume) -- un `dict` plano
`{"bucket": ..., "key": ...}` referenciando la página ya escrita en el
almacenamiento de objetos compartido, nunca el HTML en sí: cargar el cuerpo
completo de la página en la cola de mensajes desperdiciaría el ancho de
banda de Redis en datos que ya viven en el almacenamiento de objetos hecho
justo para eso (ver `~/Desarrollo/beacon-search-engine/CLAUDE.md`, regla de
serialización entre repos).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from html_content_extractor.models import DiscardReason, ExtractionConfig


@dataclass(frozen=True, slots=True)
class ExtractJob:
    """Una página crawleada, pendiente de extracción, tal y como viaja por
    el `MessageQueue` compartido (`QueueMessage.payload`)."""

    bucket: str
    key: str

    def to_payload(self) -> dict[str, Any]:
        return {"bucket": self.bucket, "key": self.key}

    @staticmethod
    def from_payload(payload: Mapping[str, Any]) -> ExtractJob:
        return ExtractJob(bucket=str(payload["bucket"]), key=str(payload["key"]))


@dataclass(frozen=True, slots=True)
class ExtractWorkerConfig:
    """Configuración de una instancia de `ExtractWorker`.

    A diferencia de `CrawlWorkerConfig`, no hay deduplicador ni rate limiter
    coordinados entre workers: extraer una página ya descargada no hace
    ninguna petición de red saliente ni comparte ningún recurso externo que
    proteger de sobrecarga entre réplicas -- el único estado compartido que
    esta fase necesita es el `MessageQueue` de fase 0, exactamente igual que
    la frontera de crawl (ver `ARCHITECTURE.md`, fase 2). `max_pages` es un
    tope *por worker*, mismo criterio y misma razón que en
    `CrawlWorkerConfig.max_pages`.
    """

    worker_id: str
    stream: str = "beacon-scale-extract-frontier"
    group: str = "beacon-scale-extract-workers"
    bucket: str = "beacon-scale-dev"
    object_key_prefix: str = "extracted-documents"
    extraction_config: ExtractionConfig = field(default_factory=ExtractionConfig)
    flush_every_pages: int = 50
    max_pages: int | None = None
    batch_size: int = 10
    poll_block_ms: int = 5000
    idle_polls_before_shutdown: int | None = 6

    def __post_init__(self) -> None:
        if not self.worker_id:
            raise ValueError("worker_id no puede estar vacío")
        if self.flush_every_pages <= 0:
            raise ValueError("flush_every_pages debe ser positivo")
        if self.max_pages is not None and self.max_pages <= 0:
            raise ValueError("max_pages debe ser positivo si se especifica")
        if self.batch_size <= 0:
            raise ValueError("batch_size debe ser positivo")
        if self.idle_polls_before_shutdown is not None and self.idle_polls_before_shutdown <= 0:
            raise ValueError("idle_polls_before_shutdown debe ser positivo si se especifica")


@dataclass(slots=True)
class ExtractWorkerStats:
    """Resumen final de una ejecución de `ExtractWorker.run()`.

    Análogo distribuido de `html_content_extractor.models.ExtractionStats`,
    con un campo adicional (`pages_missing`) que no tiene equivalente en el
    proceso único original: ahí el `pages.jsonl` de entrada y el proceso que
    lo lee son la misma máquina en el mismo instante, así que una página
    referenciada nunca puede faltar; aquí el mensaje de la cola y el objeto
    en el almacenamiento compartido pueden desincronizarse (ver
    `ARCHITECTURE.md`, fase 2, "Limitaciones conocidas").
    """

    pages_processed: int = 0
    documents_extracted: int = 0
    pages_discarded: int = 0
    pages_missing: int = 0
    discard_counts: dict[DiscardReason, int] = field(default_factory=dict)
