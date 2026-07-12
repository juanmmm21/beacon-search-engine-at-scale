"""Tipos de datos del crawl distribuido.

Estos modelos son el contrato serializado que viaja por la cola de mensajes
compartida (frontera de crawl) y por el almacenamiento de objetos (páginas
crudas) entre workers que corren en procesos -- y potencialmente máquinas --
distintas. Nunca se pasa una instancia de estas dataclasses directamente
entre workers: siempre se serializan a JSON en la frontera, mismo criterio
que el resto del ecosistema nunca comparte imports de modelos de dominio
entre repos (ver `~/Desarrollo/beacon-search-engine/CLAUDE.md`).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class FrontierJob:
    """Una URL pendiente de crawlear, tal y como viaja por el `MessageQueue`
    compartido (`QueueMessage.payload`) -- el equivalente distribuido de
    `web_crawler_scheduler.models.FrontierEntry`, sin campo `priority`: Redis
    Streams entrega en orden FIFO de llegada, no por prioridad (ver
    `ARCHITECTURE.md`, fase 1, sección "Frontera compartida" para el porqué
    de aceptar ese trade-off en vez de reimplementar una cola de prioridad
    distribuida)."""

    url: str
    depth: int
    discovered_from: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {"url": self.url, "depth": self.depth, "discovered_from": self.discovered_from}

    @staticmethod
    def from_payload(payload: Mapping[str, Any]) -> FrontierJob:
        discovered_from = payload.get("discovered_from")
        return FrontierJob(
            url=str(payload["url"]),
            depth=int(payload["depth"]),
            discovered_from=None if discovered_from is None else str(discovered_from),
        )


@dataclass(frozen=True, slots=True)
class CrawledPageRecord:
    """Página crawleada con éxito, lista para escribirse en el almacenamiento
    de objetos compartido.

    Combina lo que en `web-crawler-scheduler` son dos ficheros JSONL
    separados (`pages.jsonl` y `link_graph.jsonl`) en un único objeto por
    página -- introducir un segundo esquema de particionado en el
    almacenamiento compartido solo para el grafo de enlaces no está
    justificado a esta fase; `outlinks` viaja junto a la página misma.
    """

    url: str
    final_url: str
    status_code: int
    headers: Mapping[str, str]
    html: str
    content_type: str | None
    depth: int
    fetched_at: datetime
    outlinks: tuple[str, ...]
    fetched_by_worker: str

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "final_url": self.final_url,
            "status_code": self.status_code,
            "headers": dict(self.headers),
            "html": self.html,
            "content_type": self.content_type,
            "depth": self.depth,
            "fetched_at": self.fetched_at.isoformat(),
            "outlinks": list(self.outlinks),
            "fetched_by_worker": self.fetched_by_worker,
        }


@dataclass(frozen=True, slots=True)
class CrawlWorkerConfig:
    """Configuración de una instancia de `CrawlWorker`.

    Análogo distribuido de `web_crawler_scheduler.models.CrawlConfig`, sin los
    campos que dejan de tener sentido cuando la frontera y la deduplicación
    son compartidas entre procesos: no hay checkpoint local que volcar a
    disco porque el estado pendiente ya vive en la cola compartida y en el
    deduplicador compartido, no en memoria de este proceso. `max_pages` es un
    tope *por worker*, no un presupuesto global del crawl -- limitar el total
    across todos los workers exigiría un contador atómico global consultado
    en cada página, un coste desproporcionado frente al beneficio para esta
    fase (ver `ARCHITECTURE.md`, fase 1).
    """

    worker_id: str
    seed_urls: tuple[str, ...]
    stream: str = "beacon-scale-crawl-frontier"
    group: str = "beacon-scale-crawl-workers"
    bucket: str = "beacon-scale-dev"
    object_key_prefix: str = "crawl-pages"
    extract_stream: str = "beacon-scale-extract-frontier"
    num_hash_shards: int = 16
    max_depth: int = 3
    max_pages: int | None = None
    max_concurrent_per_domain: int = 2
    default_min_delay_seconds: float = 1.0
    request_timeout_seconds: float = 15.0
    max_retries: int = 3
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    user_agent: str = (
        "BeaconScaleCrawler/0.1 (+https://github.com/juanmmm21/beacon-search-engine-at-scale)"
    )
    obey_robots_txt: bool = True
    batch_size: int = 10
    poll_block_ms: int = 5000
    idle_polls_before_shutdown: int | None = 6

    def __post_init__(self) -> None:
        if not self.worker_id:
            raise ValueError("worker_id no puede estar vacío")
        if not self.seed_urls:
            raise ValueError("CrawlWorkerConfig requiere al menos una URL semilla")
        if self.num_hash_shards <= 0:
            raise ValueError("num_hash_shards debe ser positivo")
        if self.max_depth < 0:
            raise ValueError("max_depth no puede ser negativo")
        if self.max_pages is not None and self.max_pages <= 0:
            raise ValueError("max_pages debe ser positivo si se especifica")
        if self.max_concurrent_per_domain <= 0:
            raise ValueError("max_concurrent_per_domain debe ser positivo")
        if self.default_min_delay_seconds < 0:
            raise ValueError("default_min_delay_seconds no puede ser negativo")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds debe ser positivo")
        if self.max_retries < 0:
            raise ValueError("max_retries no puede ser negativo")
        if self.backoff_base_seconds <= 0:
            raise ValueError("backoff_base_seconds debe ser positivo")
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds no puede ser menor que backoff_base_seconds")
        if self.batch_size <= 0:
            raise ValueError("batch_size debe ser positivo")
        if self.idle_polls_before_shutdown is not None and self.idle_polls_before_shutdown <= 0:
            raise ValueError("idle_polls_before_shutdown debe ser positivo si se especifica")


@dataclass(slots=True)
class WorkerStats:
    """Resumen final de una ejecución de `CrawlWorker.run()`.

    Es la única estructura mutable del módulo -- se actualiza en vivo a
    medida que el worker procesa entradas de la frontera, igual que
    `web_crawler_scheduler.models.CheckpointState` es la única mutable en el
    crawler de un solo proceso, y por la misma razón: su propósito es mutar.
    """

    pages_crawled: int = 0
    urls_discarded: int = 0
    urls_skipped_duplicate: int = 0
