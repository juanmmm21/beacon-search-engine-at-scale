"""Tests de `CrawlWorker` con dobles en memoria de toda su infraestructura
(cola, almacenamiento, deduplicador, rate limiter) y dobles del propio
`web-crawler-scheduler` (`PageFetcher`, `RobotsPolicy`) -- nunca contra red
real ni servicios externos, en línea con el resto del sustrato (ver
`CLAUDE.md`, sección de testing: "las implementaciones locales... se testean
sin mocks... nunca se omiten sus tests solo porque requieren infraestructura
externa"). El caso más importante (`test_two_workers_share_the_frontier_...`)
es justo la propiedad que motiva esta fase: varios `CrawlWorker` que
comparten cola, deduplicador y rate limiter se reparten la frontera sin
descargar la misma página dos veces.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from web_crawler_scheduler.fetcher import FetchError
from web_crawler_scheduler.models import FetchResult

from beacon_scale_infra.crawl.dedup import InMemorySharedDeduplicator
from beacon_scale_infra.crawl.models import CrawlWorkerConfig
from beacon_scale_infra.crawl.rate_limiter import InMemoryCoordinatedRateLimiter
from beacon_scale_infra.crawl.worker import CrawlWorker
from beacon_scale_infra.protocols import ObjectStorage
from beacon_scale_infra.queue.memory import InMemoryMessageQueue
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_ROOT = "https://example.com/"
_A = "https://example.com/a"
_B = "https://example.com/b"
_C = "https://example.com/c"
_BUCKET = "test-bucket"
_PREFIX = "crawl-pages"


def _html_with_links(*hrefs: str) -> str:
    anchors = "".join(f'<a href="{href}">link</a>' for href in hrefs)
    return f"<html><body>{anchors}</body></html>"


def _make_site() -> dict[str, FetchResult]:
    """Un grafo pequeño con un nodo compartido (`/c`, enlazado tanto desde
    `/a` como desde `/b`) para ejercitar la deduplicación."""
    return {
        _ROOT: FetchResult(
            final_url=_ROOT,
            status_code=200,
            headers={},
            body=_html_with_links(_A, _B),
            content_type="text/html",
        ),
        _A: FetchResult(
            final_url=_A,
            status_code=200,
            headers={},
            body=_html_with_links(_C),
            content_type="text/html",
        ),
        _B: FetchResult(
            final_url=_B,
            status_code=200,
            headers={},
            body=_html_with_links(_C),
            content_type="text/html",
        ),
        _C: FetchResult(
            final_url=_C,
            status_code=200,
            headers={},
            body=_html_with_links(),
            content_type="text/html",
        ),
    }


class _FakeFetcher:
    """Doble de `PageFetcher`: sirve páginas de un sitio fijo en memoria y
    puede simular fallos de red para URLs concretas, sin abrir ningún socket."""

    def __init__(
        self, pages: dict[str, FetchResult], *, fail_urls: frozenset[str] = frozenset()
    ) -> None:
        self._pages = pages
        self._fail_urls = fail_urls
        self.fetched_urls: list[str] = []

    async def fetch(self, url: str, timeout_seconds: float) -> FetchResult:
        self.fetched_urls.append(url)
        if url in self._fail_urls:
            raise FetchError(url, "fallo de red simulado", attempts=3)
        return self._pages[url]


class _AllowAllRobots:
    """Doble de `RobotsPolicy` que nunca restringe nada ni impone demora."""

    async def is_allowed(self, url: str, user_agent: str) -> bool:
        return True

    async def crawl_delay(self, url: str, user_agent: str) -> float | None:
        return None


class _DisallowRobots:
    """Doble de `RobotsPolicy` que bloquea un conjunto fijo de URLs."""

    def __init__(self, disallowed: frozenset[str]) -> None:
        self._disallowed = disallowed

    async def is_allowed(self, url: str, user_agent: str) -> bool:
        return url not in self._disallowed

    async def crawl_delay(self, url: str, user_agent: str) -> float | None:
        return None


def _config(worker_id: str, **overrides: Any) -> CrawlWorkerConfig:
    defaults: dict[str, Any] = {
        "worker_id": worker_id,
        "seed_urls": (_ROOT,),
        "bucket": _BUCKET,
        "object_key_prefix": _PREFIX,
        "max_depth": 2,
        "default_min_delay_seconds": 0.0,
        "poll_block_ms": 30,
        "idle_polls_before_shutdown": 2,
    }
    defaults.update(overrides)
    return CrawlWorkerConfig(**defaults)


async def _stored_pages(storage: ObjectStorage) -> list[dict[str, Any]]:
    pages = []
    async for entry in storage.list_objects(_BUCKET, prefix=_PREFIX):
        body = await storage.get_object(_BUCKET, entry.key)
        pages.append(json.loads(body))
    return pages


def _build_worker(
    worker_id: str, storage: ObjectStorage, **overrides: Any
) -> tuple[CrawlWorker, _FakeFetcher]:
    fetcher = _FakeFetcher(_make_site(), fail_urls=overrides.pop("fail_urls", frozenset()))
    robots = overrides.pop("robots", _AllowAllRobots())
    queue = overrides.pop("queue", InMemoryMessageQueue())
    dedup = overrides.pop("dedup", InMemorySharedDeduplicator())
    rate_limiter = overrides.pop(
        "rate_limiter",
        InMemoryCoordinatedRateLimiter(max_concurrent_per_domain=5, default_min_delay_seconds=0),
    )
    worker = CrawlWorker(
        _config(worker_id, **overrides),
        queue=queue,
        storage=storage,
        dedup=dedup,
        rate_limiter=rate_limiter,
        fetcher=fetcher,
        robots=robots,
    )
    return worker, fetcher


async def test_single_worker_crawls_the_whole_site_writing_pages_to_storage(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    worker, fetcher = _build_worker("w1", storage)

    stats = await worker.run()

    assert stats.pages_crawled == 4
    assert sorted(fetcher.fetched_urls) == sorted([_ROOT, _A, _B, _C])
    stored = await _stored_pages(storage)
    assert {page["url"] for page in stored} == {_ROOT, _A, _B, _C}
    assert all(page["fetched_by_worker"] == "w1" for page in stored)


async def test_shared_url_discovered_from_two_pages_is_fetched_only_once(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    worker, fetcher = _build_worker("w1", storage)

    stats = await worker.run()

    assert fetcher.fetched_urls.count(_C) == 1
    assert stats.urls_skipped_duplicate >= 1  # /c descubierta desde /a y desde /b


async def test_max_depth_stops_outlink_discovery(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    worker, fetcher = _build_worker("w1", storage, max_depth=1)

    stats = await worker.run()

    assert stats.pages_crawled == 3  # raíz + /a + /b, nunca /c (profundidad 2)
    assert _C not in fetcher.fetched_urls


async def test_url_disallowed_by_robots_is_discarded_without_fetching(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    worker, fetcher = _build_worker("w1", storage, robots=_DisallowRobots(frozenset({_A})))

    stats = await worker.run()

    assert _A not in fetcher.fetched_urls
    assert stats.urls_discarded >= 1
    stored_urls = {page["url"] for page in await _stored_pages(storage)}
    assert _A not in stored_urls


async def test_fetch_error_discards_the_url_without_crashing_the_worker(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    worker, fetcher = _build_worker("w1", storage, fail_urls=frozenset({_A}))

    stats = await worker.run()

    assert stats.urls_discarded >= 1
    stored_urls = {page["url"] for page in await _stored_pages(storage)}
    assert _A not in stored_urls
    # el resto del sitio se sigue crawleando con normalidad tras el fallo
    assert _ROOT in stored_urls
    assert _B in stored_urls


async def test_two_workers_share_the_frontier_without_duplicate_downloads(tmp_path: Path) -> None:
    """La propiedad central de la fase 1: dos `CrawlWorker` que comparten
    cola, deduplicador y rate limiter se reparten el trabajo -- entre los
    dos crawlean el sitio completo exactamente una vez cada página, sin
    coordinarse más que a través de ese estado compartido."""
    storage = LocalFilesystemObjectStorage(tmp_path)
    shared_queue = InMemoryMessageQueue()
    shared_dedup = InMemorySharedDeduplicator()
    shared_rate_limiter = InMemoryCoordinatedRateLimiter(
        max_concurrent_per_domain=5, default_min_delay_seconds=0
    )
    shared = {
        "queue": shared_queue,
        "dedup": shared_dedup,
        "rate_limiter": shared_rate_limiter,
    }

    worker_a, fetcher_a = _build_worker("worker-a", storage, **shared)
    worker_b, fetcher_b = _build_worker("worker-b", storage, **shared)

    stats_a, stats_b = await asyncio.gather(worker_a.run(), worker_b.run())

    assert stats_a.pages_crawled + stats_b.pages_crawled == 4
    all_fetched = fetcher_a.fetched_urls + fetcher_b.fetched_urls
    assert sorted(all_fetched) == sorted([_ROOT, _A, _B, _C])  # ninguna URL descargada dos veces

    stored = await _stored_pages(storage)
    assert {page["url"] for page in stored} == {_ROOT, _A, _B, _C}
