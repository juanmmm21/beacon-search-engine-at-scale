"""Tests de `ExtractWorker` con dobles en memoria de toda su infraestructura (cola,
almacenamiento) -- nunca contra red real ni servicios externos, mismo criterio que
`test_crawl_worker.py` (ver su propio docstring). El caso más importante
(`test_two_workers_share_the_queue_without_duplicate_processing`) es la propiedad central de
esta fase: varios `ExtractWorker` que comparten cola se reparten las páginas publicadas por
la fase 1 sin procesar la misma página dos veces y sin pisar la partición de otro worker."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from html_content_extractor.models import DiscardReason

from beacon_scale_infra.extract.manifest import read_manifest
from beacon_scale_infra.extract.models import ExtractWorkerConfig
from beacon_scale_infra.extract.worker import ExtractWorker
from beacon_scale_infra.protocols import MessageQueue, ObjectStorage
from beacon_scale_infra.queue.memory import InMemoryMessageQueue
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_PREFIX = "extracted-documents"
_STREAM = "extract-frontier"
_GROUP = "extract-workers"

_LONG_ARTICLE_HTML = """
<html lang="en"><head><title>Long Article</title>
<meta property="og:title" content="A Long Article About Search"></head>
<body>
<header><nav><a href="/">Home</a></nav></header>
<article class="post-content">
<h1>A Long Article About Search</h1>
<p>This paragraph contains a substantial amount of real prose about how search engines
work, why inverted indexes matter, and how relevance ranking combines multiple signals
together to produce a useful ordering of results for the end user.</p>
<p>This second paragraph continues with more depth on tokenization, tolerant HTML
parsing, and the general engineering trade-offs involved in building a search engine
completely from scratch without leaning on any third-party indexing library.</p>
</article>
<footer><p>Copyright Example Corp.</p></footer>
</body></html>
"""


def _page_record(url: str, **overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "url": url,
        "final_url": url,
        "status_code": 200,
        "headers": {"Content-Type": "text/html; charset=utf-8"},
        "html": _LONG_ARTICLE_HTML,
        "fetched_at": "2026-07-08T09:00:00+00:00",
        "depth": 0,
        "content_type": "text/html",
    }
    record.update(overrides)
    return record


async def _store_page(storage: ObjectStorage, key: str, record: dict[str, object]) -> None:
    body = json.dumps(record, ensure_ascii=False).encode("utf-8")
    await storage.put_object(_BUCKET, key, body, content_type="application/json")


async def _publish_job(queue: MessageQueue, bucket: str, key: str) -> None:
    await queue.publish(_STREAM, {"bucket": bucket, "key": key})


def _config(worker_id: str, **overrides: Any) -> ExtractWorkerConfig:
    defaults: dict[str, Any] = {
        "worker_id": worker_id,
        "bucket": _BUCKET,
        "object_key_prefix": _PREFIX,
        "stream": _STREAM,
        "group": _GROUP,
        "poll_block_ms": 30,
        "idle_polls_before_shutdown": 2,
    }
    defaults.update(overrides)
    return ExtractWorkerConfig(**defaults)


async def _read_jsonl_lines(storage: ObjectStorage, key: str) -> list[dict[str, Any]]:
    raw = await storage.get_object(_BUCKET, key)
    return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]


async def _all_document_urls(storage: ObjectStorage) -> list[str]:
    urls: list[str] = []
    async for entry in storage.list_objects(_BUCKET, prefix=_PREFIX):
        if "/documents-" not in entry.key:
            continue
        for record in await _read_jsonl_lines(storage, entry.key):
            urls.append(str(record["url"]))
    return urls


async def test_single_worker_extracts_all_published_pages(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    queue = InMemoryMessageQueue()
    urls = [f"https://example.com/article-{i}" for i in range(3)]
    for i, url in enumerate(urls):
        key = f"crawl-pages/page-{i}.json"
        await _store_page(storage, key, _page_record(url))
        await _publish_job(queue, _BUCKET, key)

    worker = ExtractWorker(_config("worker-a"), queue=queue, storage=storage)
    stats = await worker.run()

    assert stats.pages_processed == 3
    assert stats.documents_extracted == 3
    assert stats.pages_discarded == 0
    assert sorted(await _all_document_urls(storage)) == sorted(urls)

    manifest = await read_manifest(storage, _BUCKET, _PREFIX)
    assert manifest.total_documents == 3
    assert manifest.partitions[0].partition_key == "worker-a"


async def test_discarded_pages_are_counted_and_written_separately(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    queue = InMemoryMessageQueue()
    key = "crawl-pages/pdf.json"
    await _store_page(
        storage,
        key,
        _page_record(
            "https://example.com/brochure.pdf",
            headers={"Content-Type": "application/pdf"},
            html="%PDF-1.4 ...",
        ),
    )
    await _publish_job(queue, _BUCKET, key)

    worker = ExtractWorker(_config("worker-a"), queue=queue, storage=storage)
    stats = await worker.run()

    assert stats.documents_extracted == 0
    assert stats.pages_discarded == 1
    assert stats.discard_counts[DiscardReason.NON_HTML_CONTENT] == 1

    discarded_key = f"{_PREFIX}/partition=worker-a/discarded-000000.jsonl"
    discarded = await _read_jsonl_lines(storage, discarded_key)
    assert discarded[0]["reason"] == "non_html_content"

    manifest = await read_manifest(storage, _BUCKET, _PREFIX)
    assert manifest.partitions[0].discarded_count == 1
    assert manifest.partitions[0].document_count == 0


async def test_missing_page_object_is_counted_without_crashing_the_worker(
    tmp_path: Path,
) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    queue = InMemoryMessageQueue()
    # Job publicado pero cuya página nunca se llegó a escribir (o se borró) --
    # nunca debe abortar el worker, solo contarse y seguir con el resto.
    await _publish_job(queue, _BUCKET, "crawl-pages/missing.json")
    good_key = "crawl-pages/good.json"
    await _store_page(storage, good_key, _page_record("https://example.com/good"))
    await _publish_job(queue, _BUCKET, good_key)

    worker = ExtractWorker(_config("worker-a"), queue=queue, storage=storage)
    stats = await worker.run()

    assert stats.pages_missing == 1
    assert stats.documents_extracted == 1
    assert await _all_document_urls(storage) == ["https://example.com/good"]


async def test_flush_every_pages_boundary_produces_multiple_part_files(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    queue = InMemoryMessageQueue()
    urls = [f"https://example.com/article-{i}" for i in range(5)]
    for i, url in enumerate(urls):
        key = f"crawl-pages/page-{i}.json"
        await _store_page(storage, key, _page_record(url))
        await _publish_job(queue, _BUCKET, key)

    worker = ExtractWorker(_config("worker-a", flush_every_pages=2), queue=queue, storage=storage)
    stats = await worker.run()

    assert stats.documents_extracted == 5
    part_files = [
        entry.key
        async for entry in storage.list_objects(_BUCKET, prefix=_PREFIX)
        if "/documents-" in entry.key
    ]
    # 2 + 2 + 1 (el flush final al terminar run(), con el resto pendiente) = 3 partes.
    assert len(part_files) == 3
    manifest = await read_manifest(storage, _BUCKET, _PREFIX)
    assert manifest.partitions[0].document_count == 5
    assert manifest.partitions[0].part_file_count == 3


async def test_two_workers_share_the_queue_without_duplicate_processing(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    shared_queue = InMemoryMessageQueue()
    urls = [f"https://example.com/article-{i}" for i in range(8)]
    for i, url in enumerate(urls):
        key = f"crawl-pages/page-{i}.json"
        await _store_page(storage, key, _page_record(url))
        await _publish_job(shared_queue, _BUCKET, key)

    # batch_size=1 fuerza que ambos workers se turnen sondeando la cola en vez de que uno
    # se lleve las 8 entradas ya publicadas en su primer `consume()` -- sin esto, con el
    # batch_size por defecto (10), una única llamada a `consume()` podría agotar la cola
    # entera antes de que el otro worker llegue a sondear, dejándolo sin ningún trabajo.
    worker_a = ExtractWorker(_config("worker-a", batch_size=1), queue=shared_queue, storage=storage)
    worker_b = ExtractWorker(_config("worker-b", batch_size=1), queue=shared_queue, storage=storage)

    stats_a, stats_b = await asyncio.gather(worker_a.run(), worker_b.run())

    assert stats_a.documents_extracted + stats_b.documents_extracted == 8
    all_urls = await _all_document_urls(storage)
    assert sorted(all_urls) == sorted(urls)  # ninguna página extraída dos veces

    manifest = await read_manifest(storage, _BUCKET, _PREFIX)
    assert {entry.partition_key for entry in manifest.partitions} == {"worker-a", "worker-b"}
    assert manifest.total_documents == 8


async def test_idle_shutdown_stops_the_worker_when_no_more_jobs_arrive(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    queue = InMemoryMessageQueue()
    key = "crawl-pages/only.json"
    await _store_page(storage, key, _page_record("https://example.com/only"))
    await _publish_job(queue, _BUCKET, key)

    worker = ExtractWorker(
        _config("worker-a", idle_polls_before_shutdown=1, poll_block_ms=20),
        queue=queue,
        storage=storage,
    )
    stats = await worker.run()

    assert stats.documents_extracted == 1


async def test_max_pages_stops_the_worker_early(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    queue = InMemoryMessageQueue()
    for i in range(4):
        key = f"crawl-pages/page-{i}.json"
        await _store_page(storage, key, _page_record(f"https://example.com/article-{i}"))
        await _publish_job(queue, _BUCKET, key)

    worker = ExtractWorker(_config("worker-a", max_pages=2), queue=queue, storage=storage)
    stats = await worker.run()

    assert stats.pages_processed == 2
