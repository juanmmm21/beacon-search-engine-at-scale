"""Orquestador de un worker de crawl distribuido.

Reutiliza como dependencia de paquete real la lógica de dominio ya construida
y pulida en `web-crawler-scheduler` -- descarga con reintentos y backoff
(`AiohttpFetcher`), cumplimiento de `robots.txt` (`RobotsCache`) y extracción
de enlaces salientes (`extract_outlinks`) -- exactamente el mismo patrón de
integración que `distributed-index-sharding` ya aplica con
`bm25-ranking-engine` (ver `ARCHITECTURE.md`, sección "Por qué los diez
repos originales no se tocan"). Lo que este módulo añade es la orquestación
entre varios workers, que en `web_crawler_scheduler.pipeline.CrawlPipeline`
es deliberadamente de un solo proceso: `Frontier` y `Deduplicator` ahí son
protocolos *síncronos* respaldados por una cola de prioridad y un `set` en
memoria, un diseño correcto y pulido para ese repo pero que no puede
compartirse entre procesos sin dejar de ser correcto. Aquí la frontera es el
`MessageQueue` de fase 0 (Redis Streams), la deduplicación es
`SharedDeduplicator` (fase 1, `dedup.py`) y el rate limiting es
`CoordinatedRateLimiter` (fase 1, `rate_limiter.py`) -- las tres piezas que
sí necesitan coordinación real entre workers.

**Deduplicación: se reclama al consumir, no al encolar.** A diferencia de
`CrawlPipeline._enqueue_outlinks`, que hace un `seen()` de solo lectura antes
de encolar para no hinchar la frontera en memoria de un proceso,
`_enqueue_outlinks` aquí publica directamente sin comprobar de antemano: la
única operación de deduplicación es la reclamación atómica
(`SharedDeduplicator.try_claim`) al consumir el mensaje, en `_process_job`.
Publicar primero y deduplicar al consumir nunca pierde una URL en silencio
(si la reclamación fallara antes de publicar y luego el `publish` fallara por
un problema de red, esa URL jamás volvería a encolarse); aceptar algo de
bloat en la cola compartida por descubrimientos duplicados de una misma URL
es un coste barato comparado con perder páginas del corpus para siempre, y
sigue garantizando que como mucho un worker llega a descargar cada URL.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from web_crawler_scheduler.fetcher import FetchError
from web_crawler_scheduler.link_extractor import extract_outlinks
from web_crawler_scheduler.protocols import PageFetcher, RobotsPolicy

from beacon_scale_infra.crawl.dedup import SharedDeduplicator
from beacon_scale_infra.crawl.models import (
    CrawledPageRecord,
    CrawlWorkerConfig,
    FrontierJob,
    WorkerStats,
)
from beacon_scale_infra.crawl.partitioning import object_key_for_page
from beacon_scale_infra.crawl.rate_limiter import CoordinatedRateLimiter
from beacon_scale_infra.protocols import MessageQueue, ObjectStorage

logger = logging.getLogger(__name__)


class CrawlWorker:
    """Consume la frontera compartida hasta agotar el trabajo disponible (o
    alcanzar `config.max_pages`) y escribe cada página crawleada al
    almacenamiento de objetos compartido.

    Todas sus dependencias son protocolos (`MessageQueue`, `ObjectStorage`,
    `SharedDeduplicator`, `CoordinatedRateLimiter`, y los del propio
    `web-crawler-scheduler`: `PageFetcher`, `RobotsPolicy`) -- el worker no
    construye ninguna implementación concreta él mismo, siguiendo el mismo
    patrón protocolo-plus-implementaciones del resto del sustrato; quien lo
    conecta (la CLI en `__main__.py`) decide si cada pieza es la
    implementación local de desarrollo o la real.
    """

    def __init__(
        self,
        config: CrawlWorkerConfig,
        *,
        queue: MessageQueue,
        storage: ObjectStorage,
        dedup: SharedDeduplicator,
        rate_limiter: CoordinatedRateLimiter,
        fetcher: PageFetcher,
        robots: RobotsPolicy,
    ) -> None:
        self._config = config
        self._queue = queue
        self._storage = storage
        self._dedup = dedup
        self._rate_limiter = rate_limiter
        self._fetcher = fetcher
        self._robots = robots
        self._stats = WorkerStats()

    async def run(self) -> WorkerStats:
        """Siembra la frontera y consume trabajo hasta `max_pages` o hasta
        `idle_polls_before_shutdown` sondeos consecutivos sin mensajes
        nuevos (`None` para no detenerse nunca, propio de un servicio de
        `docker-compose` de larga duración)."""
        await self._queue.ensure_group(self._config.stream, self._config.group)
        await self._seed_frontier()

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
                job = FrontierJob.from_payload(message.payload)
                await self._process_job(job)
                await self._queue.ack(self._config.stream, self._config.group, message.message_id)

        return self._stats

    def _reached_max_pages(self) -> bool:
        return (
            self._config.max_pages is not None
            and self._stats.pages_crawled >= self._config.max_pages
        )

    async def _seed_frontier(self) -> None:
        """Publica las URLs semilla en la frontera compartida.

        Se llama sin coordinación previa entre workers -- si varios workers
        arrancan a la vez, todos publican las mismas semillas, produciendo
        entradas duplicadas en el stream. Eso es intencionado y seguro: la
        reclamación atómica en `_process_job` garantiza que como mucho un
        worker llega a descargar cada semilla, exactamente igual que con
        cualquier otra URL descubierta durante el crawl. Esto evita necesitar
        un paso de siembra separado fuera del ciclo de vida normal del
        worker (ver README, sección "Lanzar varios workers").
        """
        for seed_url in self._config.seed_urls:
            job = FrontierJob(url=seed_url, depth=0, discovered_from=None)
            await self._queue.publish(self._config.stream, job.to_payload())

    async def _process_job(self, job: FrontierJob) -> None:
        claimed = await self._dedup.try_claim(job.url)
        if not claimed:
            self._stats.urls_skipped_duplicate += 1
            return

        min_delay: float | None = None
        if self._config.obey_robots_txt:
            allowed = await self._robots.is_allowed(job.url, self._config.user_agent)
            if not allowed:
                logger.info(
                    "worker=%s url=%s descartada: bloqueada por robots.txt",
                    self._config.worker_id,
                    job.url,
                )
                self._stats.urls_discarded += 1
                return
            min_delay = await self._robots.crawl_delay(job.url, self._config.user_agent)

        lease_token = await self._rate_limiter.acquire(job.url, min_delay)
        try:
            result = await self._fetcher.fetch(job.url, self._config.request_timeout_seconds)
        except FetchError as exc:
            logger.warning(
                "worker=%s url=%s descartada tras %d intento(s): %s",
                self._config.worker_id,
                job.url,
                exc.attempts,
                exc.reason,
            )
            self._stats.urls_discarded += 1
            return
        finally:
            await self._rate_limiter.release(job.url, lease_token)

        outlinks = self._extract_outlinks_safely(result.body, result.final_url)
        page = CrawledPageRecord(
            url=job.url,
            final_url=result.final_url,
            status_code=result.status_code,
            headers=result.headers,
            html=result.body,
            content_type=result.content_type,
            depth=job.depth,
            fetched_at=datetime.now(UTC),
            outlinks=tuple(outlinks),
            fetched_by_worker=self._config.worker_id,
        )
        await self._write_page(page)
        self._stats.pages_crawled += 1

        if job.depth < self._config.max_depth:
            await self._enqueue_outlinks(outlinks, parent_url=job.url, child_depth=job.depth + 1)

    def _extract_outlinks_safely(self, html: str, final_url: str) -> list[str]:
        try:
            # web_crawler_scheduler no publica py.typed (ver pyproject.toml);
            # mypy ve extract_outlinks como Any pese a devolver list[str] en
            # tiempo de ejecución -- mismo patrón que bm25-ranking-engine en
            # distributed-index-sharding.
            return extract_outlinks(html, final_url)  # type: ignore[no-any-return]
        except Exception:  # noqa: BLE001 - aísla un fallo de parseo de una página del resto del crawl
            logger.warning(
                "worker=%s no se pudieron extraer enlaces de %s",
                self._config.worker_id,
                final_url,
                exc_info=True,
            )
            return []

    async def _enqueue_outlinks(
        self, outlinks: list[str], *, parent_url: str, child_depth: int
    ) -> None:
        for outlink in outlinks:
            job = FrontierJob(url=outlink, depth=child_depth, discovered_from=parent_url)
            await self._queue.publish(self._config.stream, job.to_payload())

    async def _write_page(self, page: CrawledPageRecord) -> None:
        key = object_key_for_page(
            page.url,
            page.fetched_at,
            prefix=self._config.object_key_prefix,
            num_hash_shards=self._config.num_hash_shards,
        )
        body = json.dumps(page.to_json_dict(), ensure_ascii=False).encode("utf-8")
        await self._storage.put_object(
            self._config.bucket, key, body, content_type="application/json"
        )
