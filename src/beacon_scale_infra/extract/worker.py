"""Orquestador de un worker de extracción distribuida (fase 2).

Consume la cola compartida de páginas listas para extraer -- publicada por
`CrawlWorker` tras escribir cada página al almacenamiento de objetos
compartido (ver `crawl/worker.py`, `_write_page`) -- a medida que el crawler
las produce, nunca en un lote al final: cada `ExtractJob` de la cola
referencia una página que ya vive en el almacenamiento compartido en el
momento en que se publica, así que un `ExtractWorker` puede arrancar,
procesar mensajes y pararse en cualquier momento del crawl, no solo al
terminar.

**El paso más fácil de paralelizar de todo el pipeline.** Cada página es
completamente independiente de las demás -- a diferencia de fase 1, donde
`SharedDeduplicator` y `CoordinatedRateLimiter` existen precisamente porque
varias URLs compiten por el mismo dominio o el mismo derecho a
descargarse una sola vez, extraer una página ya descargada no comparte
ningún recurso ni URL con la extracción de cualquier otra: no hace falta
ningún deduplicador ni rate limiter equivalente en este módulo. El único
estado compartido entre réplicas es el `MessageQueue` de fase 0, con la
misma semántica de grupos de consumidores que ya reparte la frontera de
crawl -- N `ExtractWorker` con el mismo `group` se reparten los mensajes del
stream sin duplicarse entre sí, sin necesitar ninguna otra coordinación (ver
`ARCHITECTURE.md`, fase 2).

Reutiliza como dependencia de paquete real la lógica de extracción de
contenido de `html-content-extractor` a través de `extract_single_page`
(`page_extractor.py`) -- ver ese módulo para el porqué de no reutilizar
`ExtractionPipeline` directamente.
"""

from __future__ import annotations

import json
import logging

from html_content_extractor.models import DiscardedPage, ExtractedDocument

from beacon_scale_infra.errors import ObjectNotFoundError
from beacon_scale_infra.extract.manifest import PartitionManifestEntry, write_partition_manifest
from beacon_scale_infra.extract.models import ExtractJob, ExtractWorkerConfig, ExtractWorkerStats
from beacon_scale_infra.extract.page_extractor import extract_single_page
from beacon_scale_infra.extract.partitioning import (
    object_key_for_discarded_part,
    object_key_for_document_part,
)
from beacon_scale_infra.protocols import MessageQueue, ObjectStorage

logger = logging.getLogger(__name__)


class ExtractWorker:
    """Consume la frontera de extracción compartida hasta agotar el trabajo
    disponible (o alcanzar `config.max_pages`), extrayendo cada página
    referenciada y escribiendo los documentos resultantes -- particionados
    por `worker_id`, nunca compartiendo un fichero de salida con otra
    réplica -- al almacenamiento de objetos compartido.

    Todas sus dependencias son protocolos (`MessageQueue`, `ObjectStorage`)
    -- el worker no construye ninguna implementación concreta él mismo,
    mismo patrón que `CrawlWorker` (ver `crawl/worker.py`); quien lo conecta
    (la CLI en `__main__.py`) decide si cada pieza es la implementación
    local de desarrollo o la real.
    """

    def __init__(
        self,
        config: ExtractWorkerConfig,
        *,
        queue: MessageQueue,
        storage: ObjectStorage,
    ) -> None:
        self._config = config
        self._queue = queue
        self._storage = storage
        self._stats = ExtractWorkerStats()
        self._document_buffer: list[ExtractedDocument] = []
        self._discarded_buffer: list[DiscardedPage] = []
        self._part_seq = 0
        self._total_documents_written = 0
        self._total_discarded_written = 0

    async def run(self) -> ExtractWorkerStats:
        """Consume trabajo hasta `max_pages` o hasta
        `idle_polls_before_shutdown` sondeos consecutivos sin mensajes
        nuevos (`None` para no detenerse nunca, propio de un servicio de
        `docker-compose` de larga duración). Siempre vacía el búfer
        pendiente antes de devolver el control, incluso si se detiene a
        media partición."""
        await self._queue.ensure_group(self._config.stream, self._config.group)

        idle_polls = 0
        while not self._reached_max_pages():
            messages = await self._queue.consume(
                self._config.stream,
                self._config.group,
                self._config.worker_id,
                count=self._config.batch_size,
                block_ms=self._config.poll_block_ms,
            )
            if not messages:
                idle_polls += 1
                if (
                    self._config.idle_polls_before_shutdown is not None
                    and idle_polls >= self._config.idle_polls_before_shutdown
                ):
                    logger.info(
                        "worker=%s sin trabajo nuevo tras %d sondeo(s), deteniéndose",
                        self._config.worker_id,
                        idle_polls,
                    )
                    break
                continue
            idle_polls = 0

            for message in messages:
                if self._reached_max_pages():
                    break
                job = ExtractJob.from_payload(message.payload)
                await self._process_job(job)
                await self._queue.ack(self._config.stream, self._config.group, message.message_id)

        await self._flush()
        return self._stats

    def _reached_max_pages(self) -> bool:
        return (
            self._config.max_pages is not None
            and self._stats.pages_processed >= self._config.max_pages
        )

    async def _process_job(self, job: ExtractJob) -> None:
        try:
            raw = await self._storage.get_object(job.bucket, job.key)
        except ObjectNotFoundError:
            # La página referenciada por este mensaje ya no existe en el
            # almacenamiento compartido -- un `publish` que sí llegó a la cola
            # tras un `put_object` que en realidad falló, o el objeto borrado
            # entre la publicación y el consumo. Sin reconciliación
            # automática en esta fase (ver `ARCHITECTURE.md`, fase 2,
            # "Limitaciones conocidas"): se registra y se sigue con el resto
            # de la cola, nunca se aborta el worker por una página perdida.
            logger.warning(
                "worker=%s página referenciada por la cola ya no existe: %s/%s",
                self._config.worker_id,
                job.bucket,
                job.key,
            )
            self._stats.pages_missing += 1
            return

        record = json.loads(raw)
        result = extract_single_page(record, self._config.extraction_config)
        self._stats.pages_processed += 1

        if isinstance(result, ExtractedDocument):
            self._document_buffer.append(result)
            self._stats.documents_extracted += 1
        else:
            self._discarded_buffer.append(result)
            self._stats.pages_discarded += 1
            self._stats.discard_counts[result.reason] = (
                self._stats.discard_counts.get(result.reason, 0) + 1
            )

        buffered = len(self._document_buffer) + len(self._discarded_buffer)
        if buffered >= self._config.flush_every_pages:
            await self._flush()

    async def _flush(self) -> None:
        """Escribe el búfer acumulado como nuevos ficheros de parte
        (nunca sobrescribe ficheros de partes anteriores, ver
        `partitioning.py`) y actualiza el fragmento de manifiesto de esta
        partición con los totales acumulados. No-op si el búfer está vacío,
        para que llamarlo al final de `run()` sea siempre seguro."""
        if not self._document_buffer and not self._discarded_buffer:
            return

        if self._document_buffer:
            key = object_key_for_document_part(
                self._config.worker_id, self._part_seq, prefix=self._config.object_key_prefix
            )
            body = (
                "\n".join(
                    json.dumps(document.to_json_dict(), ensure_ascii=False)
                    for document in self._document_buffer
                )
                + "\n"
            ).encode("utf-8")
            await self._storage.put_object(
                self._config.bucket, key, body, content_type="application/jsonl"
            )
            self._total_documents_written += len(self._document_buffer)
            self._document_buffer.clear()

        if self._discarded_buffer:
            key = object_key_for_discarded_part(
                self._config.worker_id, self._part_seq, prefix=self._config.object_key_prefix
            )
            body = (
                "\n".join(
                    json.dumps(discarded.to_json_dict(), ensure_ascii=False)
                    for discarded in self._discarded_buffer
                )
                + "\n"
            ).encode("utf-8")
            await self._storage.put_object(
                self._config.bucket, key, body, content_type="application/jsonl"
            )
            self._total_discarded_written += len(self._discarded_buffer)
            self._discarded_buffer.clear()

        self._part_seq += 1
        await write_partition_manifest(
            self._storage,
            self._config.bucket,
            self._config.object_key_prefix,
            PartitionManifestEntry(
                partition_key=self._config.worker_id,
                document_count=self._total_documents_written,
                discarded_count=self._total_discarded_written,
                part_file_count=self._part_seq,
            ),
        )
