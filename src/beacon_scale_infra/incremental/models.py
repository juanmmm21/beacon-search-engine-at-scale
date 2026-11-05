"""Contratos de la indexación incremental.

`IndexUpdate` es el mensaje que viaja por el `MessageQueue` hacia el
`IncrementalIndexer`. Una alta lleva el documento extraído completo (el
formato JSON de `html-content-extractor`, el mismo que consume
`inverted-index-builder`), no una referencia a un objeto: el indexador no
tiene que leer el almacenamiento por cada mensaje, y un documento extraído
es texto ya limpio, del orden de kilobytes — al contrario que el HTML crudo
que `ExtractJob` deliberadamente no lleva.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class IndexUpdateOperation(StrEnum):
    UPSERT = "upsert"
    DELETE = "delete"


@dataclass(frozen=True, slots=True)
class IndexUpdate:
    operation: IndexUpdateOperation
    url: str
    document: Mapping[str, Any] | None = None
    """Documento extraído; obligatorio en `UPSERT`, ausente en `DELETE`."""

    def __post_init__(self) -> None:
        if not self.url:
            raise ValueError("url no puede estar vacía")
        if self.operation is IndexUpdateOperation.UPSERT:
            if self.document is None:
                raise ValueError("una alta necesita el documento extraído")
            if str(self.document.get("url")) != self.url:
                raise ValueError("la url del documento no coincide con la de la actualización")
        elif self.document is not None:
            raise ValueError("una baja no lleva documento")

    @staticmethod
    def upsert(document: Mapping[str, Any]) -> IndexUpdate:
        return IndexUpdate(IndexUpdateOperation.UPSERT, str(document["url"]), dict(document))

    @staticmethod
    def delete(url: str) -> IndexUpdate:
        return IndexUpdate(IndexUpdateOperation.DELETE, url)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"operation": self.operation.value, "url": self.url}
        if self.document is not None:
            payload["document"] = dict(self.document)
        return payload

    @staticmethod
    def from_payload(payload: Mapping[str, Any]) -> IndexUpdate:
        document = payload.get("document")
        if document is not None and not isinstance(document, Mapping):
            raise ValueError("el campo document debe ser un objeto JSON")
        return IndexUpdate(
            operation=IndexUpdateOperation(str(payload["operation"])),
            url=str(payload["url"]),
            document=None if document is None else dict(document),
        )


@dataclass(frozen=True, slots=True)
class IncrementalIndexerConfig:
    """Configuración de un `IncrementalIndexer`.

    Hay **un solo indexador por índice**: el índice segmentado admite un
    único escritor (ver `inverted_index_builder.segments`). Escalar la
    escritura se hace con un índice por shard, no con réplicas del mismo
    indexador — el `group` existe para que un reinicio retome los mensajes
    pendientes sin ack, no para repartirlos."""

    worker_id: str
    local_root: Path
    """Copia de trabajo local del índice segmentado (un volumen persistente
    en Docker). Si no existe, se restaura desde el almacenamiento."""
    stream: str = "beacon-scale-index-updates"
    group: str = "beacon-scale-incremental-indexer"
    bucket: str = "beacon-scale-dev"
    index_prefix: str = "search-index-segmented"
    flush_every_updates: int = 500
    """Tamaño máximo de lote: cuántas actualizaciones se juntan en un
    segmento. Un lote se cierra antes si la cola se queda vacía."""
    batch_size: int = 100
    poll_block_ms: int = 2000
    idle_polls_before_shutdown: int | None = 3
    merge_segments_per_tier: int = 4
    merge_max_deleted_ratio: float = 0.3
    max_updates: int | None = None
    document_store_prefix: str | None = None
    """Si se indica, el documento completo de cada alta se guarda además en
    `<prefijo>/<sha256(url)>.json` (y se borra con la baja): el índice
    invertido solo guarda URL, título y longitud, y quien sirve resultados
    necesita el texto para snippets, el rerank y `fetch_document`."""

    def __post_init__(self) -> None:
        if not self.worker_id:
            raise ValueError("worker_id no puede estar vacío")
        if self.flush_every_updates <= 0:
            raise ValueError("flush_every_updates debe ser positivo")
        if self.batch_size <= 0:
            raise ValueError("batch_size debe ser positivo")
        if self.idle_polls_before_shutdown is not None and self.idle_polls_before_shutdown <= 0:
            raise ValueError("idle_polls_before_shutdown debe ser positivo si se especifica")
        if self.merge_segments_per_tier < 2:
            raise ValueError("merge_segments_per_tier debe ser >= 2")
        if not 0.0 < self.merge_max_deleted_ratio <= 1.0:
            raise ValueError("merge_max_deleted_ratio debe estar en (0, 1]")
        if self.max_updates is not None and self.max_updates <= 0:
            raise ValueError("max_updates debe ser positivo si se especifica")


@dataclass(slots=True)
class IncrementalRunStats:
    updates_received: int = 0
    invalid_updates: int = 0
    documents_upserted: int = 0
    documents_deleted: int = 0
    segments_written: int = 0
    merges: int = 0
    snapshots_published: int = 0
    live_documents: int = 0
    last_index_version: str | None = None
    batch_seconds: list[float] = field(default_factory=list)
    """Duración de cada lote, de recibir el primer mensaje a tener la
    instantánea publicada: la frescura que ve un lector."""
