"""Indexador incremental: consume `IndexUpdate` de la cola y los aplica al
índice segmentado sin reconstruirlo.

## Ciclo de un lote

1. Juntar mensajes hasta `flush_every_updates` o hasta que la cola se quede
   vacía (lo que pase antes): con la cola en calma, cada cambio se publica
   en cuanto llega en vez de esperar a llenar un lote.
2. Coalescer por URL: gana la última operación del lote. Altas y bajas de
   URLs distintas son independientes entre sí, así que tras coalescer el
   orden ya no importa y basta un `add_documents` y un `delete_urls`.
3. Escribir el segmento y los tombstones (`inverted-index-builder`), aplicar
   la `TieredMergePolicy` y publicar la instantánea (`snapshot.py`).
4. Solo entonces hacer `ack` de los mensajes del lote.

El `ack` tras publicar da entrega *al menos una vez*: si el proceso muere
entre 3 y 4, los mensajes se redeliveran y se aplican otra vez. Es seguro
porque reaplicar una alta deja el mismo contenido vivo (con otro `doc_id`) y
reaplicar una baja es idempotente.

Un mensaje que no se puede interpretar se registra, se cuenta y se confirma:
reintentarlo no lo arreglaría y bloquearía la cola para siempre.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time

from inverted_index_builder.segments import SegmentedIndex, SegmentedIndexError, TieredMergePolicy

from beacon_scale_infra.errors import IncrementalIndexingError
from beacon_scale_infra.incremental.models import (
    IncrementalIndexerConfig,
    IncrementalRunStats,
    IndexUpdate,
    IndexUpdateOperation,
)
from beacon_scale_infra.incremental.snapshot import SnapshotPublisher, restore_snapshot
from beacon_scale_infra.models import QueueMessage
from beacon_scale_infra.protocols import MessageQueue, ObjectStorage

logger = logging.getLogger(__name__)


def document_key(prefix: str, url: str) -> str:
    """Clave del documento de `url` en el almacén de documentos: hash y no la
    URL tal cual, porque una URL puede no ser una clave de objeto válida."""
    return f"{prefix.strip('/')}/{hashlib.sha256(url.encode('utf-8')).hexdigest()}.json"


def coalesce_updates(updates: list[IndexUpdate]) -> tuple[list[IndexUpdate], list[str]]:
    """`(altas, URLs a borrar)` tras quedarse con la última operación de
    cada URL, en orden de última aparición (determinista)."""
    latest: dict[str, IndexUpdate] = {}
    for update in updates:
        latest.pop(update.url, None)
        latest[update.url] = update
    upserts = [u for u in latest.values() if u.operation is IndexUpdateOperation.UPSERT]
    deletes = [u.url for u in latest.values() if u.operation is IndexUpdateOperation.DELETE]
    return upserts, deletes


class IncrementalIndexer:
    def __init__(
        self,
        config: IncrementalIndexerConfig,
        *,
        queue: MessageQueue,
        storage: ObjectStorage,
    ) -> None:
        self._config = config
        self._queue = queue
        self._storage = storage
        self._publisher = SnapshotPublisher(storage, config.bucket, config.index_prefix)
        self._policy = TieredMergePolicy(
            merge_factor=config.merge_segments_per_tier,
            max_deleted_ratio=config.merge_max_deleted_ratio,
        )
        self._stats = IncrementalRunStats()
        self._index: SegmentedIndex | None = None

    @property
    def stats(self) -> IncrementalRunStats:
        return self._stats

    async def open(self) -> SegmentedIndex:
        """Abre la copia local; si no existe, restaura la última instantánea
        publicada, y si tampoco hay ninguna, crea un índice vacío."""
        if self._index is not None:
            return self._index
        root = self._config.local_root
        try:
            if not (root / "segments.json").exists():
                restored = await restore_snapshot(
                    self._storage, self._config.bucket, self._config.index_prefix, root
                )
                logger.info(
                    "indexer=%s %s",
                    self._config.worker_id,
                    "instantánea restaurada" if restored else "índice nuevo",
                )
            self._index = await asyncio.to_thread(SegmentedIndex.open_or_create, root)
        except SegmentedIndexError as error:
            raise IncrementalIndexingError(f"no se puede abrir {root}: {error}") from error
        return self._index

    async def run(self) -> IncrementalRunStats:
        index = await self.open()
        await self._queue.ensure_group(self._config.stream, self._config.group)
        idle_polls = 0
        while not self._reached_max_updates():
            batch = await self._collect_batch()
            if not batch:
                idle_polls += 1
                limit = self._config.idle_polls_before_shutdown
                if limit is not None and idle_polls >= limit:
                    break
                continue
            idle_polls = 0
            await self._apply_batch(index, batch)
        self._stats.live_documents = index.manifest.live_documents
        return self._stats

    def _reached_max_updates(self) -> bool:
        limit = self._config.max_updates
        return limit is not None and self._stats.updates_received >= limit

    async def _collect_batch(self) -> list[QueueMessage]:
        batch: list[QueueMessage] = []
        while len(batch) < self._config.flush_every_updates:
            remaining = self._config.flush_every_updates - len(batch)
            messages = await self._queue.consume(
                self._config.stream,
                self._config.group,
                self._config.worker_id,
                count=min(self._config.batch_size, remaining),
                # Solo el primer sondeo de un lote espera: en cuanto hay algo
                # que publicar, una cola vacía cierra el lote.
                block_ms=self._config.poll_block_ms if not batch else 1,
            )
            if not messages:
                break
            batch.extend(messages)
        return batch

    async def _apply_batch(self, index: SegmentedIndex, batch: list[QueueMessage]) -> None:
        started = time.perf_counter()
        updates: list[IndexUpdate] = []
        for message in batch:
            self._stats.updates_received += 1
            try:
                updates.append(IndexUpdate.from_payload(message.payload))
            except (KeyError, ValueError) as error:
                self._stats.invalid_updates += 1
                logger.warning(
                    "indexer=%s actualización inválida %s descartada: %s",
                    self._config.worker_id,
                    message.message_id,
                    error,
                )
        upserts, deletes = coalesce_updates(updates)
        store = self._config.document_store_prefix
        # Textos antes que el índice: una instantánea publicada nunca apunta a
        # un documento que todavía no está en el almacén.
        if store is not None:
            for update in upserts:
                await self._storage.put_object(
                    self._config.bucket,
                    document_key(store, update.url),
                    json.dumps(dict(update.document or {}), ensure_ascii=False).encode("utf-8"),
                    content_type="application/json",
                )
        try:
            if upserts:
                documents = [dict(u.document or {}) for u in upserts]
                await asyncio.to_thread(index.add_documents, documents)
                self._stats.documents_upserted += len(documents)
                self._stats.segments_written += 1
            if deletes:
                self._stats.documents_deleted += await asyncio.to_thread(index.delete_urls, deletes)
            reports = await asyncio.to_thread(index.maybe_merge, self._policy)
        except (SegmentedIndexError, OSError, KeyError, ValueError) as error:
            raise IncrementalIndexingError(f"no se pudo aplicar el lote: {error}") from error
        self._stats.merges += len(reports)

        version = await self._publisher.publish(index.root)
        self._stats.snapshots_published += 1
        self._stats.last_index_version = version
        if store is not None:
            for url in deletes:
                if index.live_doc_id(url) is None:
                    await self._storage.delete_object(self._config.bucket, document_key(store, url))
        for message in batch:
            await self._queue.ack(self._config.stream, self._config.group, message.message_id)
        elapsed = time.perf_counter() - started
        self._stats.batch_seconds.append(elapsed)
        logger.info(
            "indexer=%s lote de %d: +%d -%d, %d fusión(es), versión %s en %.3fs",
            self._config.worker_id,
            len(batch),
            len(upserts),
            len(deletes),
            len(reports),
            version[:12],
            elapsed,
        )


async def publish_updates(queue: MessageQueue, stream: str, updates: list[IndexUpdate]) -> int:
    """Encola `updates` en orden. Devuelve cuántas se publicaron."""
    for update in updates:
        await queue.publish(stream, update.to_payload())
    return len(updates)
