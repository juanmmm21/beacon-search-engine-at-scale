"""Tests del buscador vertical: fuentes curadas, rastreo con ámbito y
presupuesto contra un servidor HTTP local real (aiohttp), y exportación del
bundle que consume la consola."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from html_content_extractor.models import ExtractionConfig

from beacon_scale_infra.incremental.indexer import (
    IncrementalIndexer,
    document_key,
    publish_updates,
)
from beacon_scale_infra.incremental.models import IncrementalIndexerConfig, IndexUpdate
from beacon_scale_infra.queue.memory import InMemoryMessageQueue
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage
from beacon_scale_infra.vertical.bundle import CURRENT_LINK, current_bundle_version, export_bundle
from beacon_scale_infra.vertical.pipeline import (
    VerticalCrawlConfig,
    crawl_vertical,
    updates_from_pages,
)
from beacon_scale_infra.vertical.sources import (
    DocSource,
    VerticalSources,
    VerticalSourcesError,
    load_sources,
)

_PROSE = (
    "This page explains a feature of the language in enough detail for the extractor "
    "to keep it: iterators, ownership, borrowing and lifetimes, with examples that show "
    "how the compiler checks every reference before the program ever runs."
)


def _source(source_id: str, base: str, **overrides: Any) -> DocSource:
    fields: dict[str, Any] = {
        "source_id": source_id,
        "name": source_id.title(),
        "seeds": (f"{base}/docs/",),
        "allowed_prefixes": (f"{base}/docs/",),
        "max_pages": 10,
        "license": "MIT",
        "license_url": f"{base}/license",
        "attribution": "Test authors",
    }
    fields.update(overrides)
    return DocSource(**fields)


def test_repository_sources_file_is_valid() -> None:
    sources = load_sources(Path(__file__).parents[1] / "deploy" / "vertical-sources.json")
    assert {s.source_id for s in sources.enabled} == {"python", "rust", "mdn"}
    assert all(s.license and s.license_url for s in sources.sources)
    assert sum(sources.budgets().values()) <= sources.total_page_budget


def test_budgets_are_cut_in_declaration_order() -> None:
    sources = VerticalSources(
        total_page_budget=15,
        sources=(
            _source("a", "http://a.test", max_pages=10),
            _source("b", "http://b.test", max_pages=10),
            _source("c", "http://c.test", max_pages=10),
        ),
    )
    assert sources.budgets() == {"a": 10, "b": 5, "c": 0}


def test_invalid_sources_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(VerticalSourcesError):
        _source("a", "http://a.test", license="")
    with pytest.raises(VerticalSourcesError):
        _source("a", "http://a.test", seeds=("http://elsewhere.test/",))
    with pytest.raises(VerticalSourcesError):
        _source("a", "http://a.test", enabled=False)
    path = tmp_path / "sources.json"
    path.write_text(json.dumps({"format_version": 2, "sources": []}))
    with pytest.raises(VerticalSourcesError):
        load_sources(path)


def test_crawl_config_refuses_impolite_settings(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        VerticalCrawlConfig(work_dir=tmp_path, min_delay_seconds=0.1)
    with pytest.raises(ValueError):
        VerticalCrawlConfig(work_dir=tmp_path, max_concurrent_per_domain=10)


def _page(title: str, links: list[str]) -> str:
    anchors = "".join(f'<a href="{link}">{link}</a> ' for link in links)
    return (
        f"<html><head><title>{title}</title></head><body><article><h1>{title}</h1>"
        f"<p>{_PROSE}</p><p>{_PROSE}</p></article><nav>{anchors}</nav></body></html>"
    )


async def test_crawl_respects_scope_robots_and_budget(tmp_path: Path) -> None:
    requested: list[str] = []

    async def handler(request: web.Request) -> web.Response:
        requested.append(request.path)
        if request.path == "/robots.txt":
            return web.Response(text="User-agent: *\nDisallow: /docs/private\n")
        pages = {
            "/docs/": _page("Index", ["/docs/a", "/docs/b", "/docs/private", "/blog/x"]),
            "/docs/a": _page("Page A", ["/docs/c"]),
            "/docs/b": _page("Page B", []),
            "/docs/c": _page("Page C", []),
        }
        if request.path in pages:
            return web.Response(text=pages[request.path], content_type="text/html")
        return web.Response(status=404)

    app = web.Application()
    app.router.add_get("/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    base = f"http://127.0.0.1:{port}"
    try:
        sources = VerticalSources(
            total_page_budget=3, sources=(_source("docs", base, max_pages=10),)
        )
        updates, reports = await crawl_vertical(
            sources, VerticalCrawlConfig(work_dir=tmp_path / "work", min_delay_seconds=0.5)
        )
    finally:
        await runner.cleanup()

    assert reports[0].pages_crawled == 3
    assert "/blog/x" not in requested
    assert "/docs/private" not in requested
    assert len(updates) == 3
    assert all(u.document is not None and u.document["source_id"] == "docs" for u in updates)


def test_tagged_documents_drop_the_url_fragment(tmp_path: Path) -> None:
    pages = tmp_path / "pages.jsonl"
    url = "https://docs.python.org/3/library/asyncio-task.html#asyncio.timeout"
    pages.write_text(
        json.dumps(
            {
                "url": url,
                "final_url": url,
                "status_code": 200,
                "headers": {"Content-Type": "text/html"},
                "html": _page("Coroutines and tasks", []),
                "fetched_at": "2026-11-05T09:00:00+00:00",
                "depth": 1,
            }
        )
        + "\n"
    )
    source = _source(
        "python",
        "https://docs.python.org",
        allowed_prefixes=("https://docs.python.org/3/",),
        seeds=("https://docs.python.org/3/",),
    )
    updates, _, _ = updates_from_pages(pages, source, ExtractionConfig())
    assert updates[0].url == "https://docs.python.org/3/library/asyncio-task.html"
    assert updates[0].document is not None
    assert updates[0].document["final_url"] == updates[0].url


def _doc(url: str, text: str) -> dict[str, Any]:
    return {
        "url": url,
        "final_url": url,
        "title": url.rsplit("/", 1)[-1],
        "main_text": text,
        "fetched_at": "2026-11-05T09:00:00+00:00",
        "source_id": "rust",
    }


async def test_export_bundle_writes_documents_sources_and_swaps_current(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    queue = InMemoryMessageQueue()
    config = IncrementalIndexerConfig(
        worker_id="t",
        local_root=tmp_path / "work",
        bucket="b",
        index_prefix="vertical-index",
        document_store_prefix="vertical-documents",
        poll_block_ms=5,
        idle_polls_before_shutdown=1,
    )
    sources = VerticalSources(
        total_page_budget=10,
        sources=(
            _source("rust", "https://doc.rust-lang.org"),
            _source("swift", "https://docs.swift.org", enabled=False, disabled_reason="DocC"),
        ),
    )
    await publish_updates(
        queue,
        config.stream,
        [
            IndexUpdate.upsert(_doc("https://doc.rust-lang.org/docs/a", "ownership rules")),
            IndexUpdate.upsert(_doc("https://doc.rust-lang.org/docs/b", "borrowing rules")),
        ],
    )
    await IncrementalIndexer(config, queue=queue, storage=storage).run()
    root = tmp_path / "bundles"
    first = await export_bundle(storage, "b", "vertical-index", "vertical-documents", sources, root)
    assert first is not None and first.documents == 2 and not first.reused
    assert current_bundle_version(root) == first.index_version
    again = await export_bundle(storage, "b", "vertical-index", "vertical-documents", sources, root)
    assert again is not None and again.reused

    await publish_updates(
        queue, config.stream, [IndexUpdate.delete("https://doc.rust-lang.org/docs/a")]
    )
    await IncrementalIndexer(config, queue=queue, storage=storage).run()
    second = await export_bundle(
        storage, "b", "vertical-index", "vertical-documents", sources, root
    )
    assert second is not None and second.documents == 1
    current = root / CURRENT_LINK
    assert current.is_symlink() and current.resolve() == second.path.resolve()
    lines = [json.loads(line) for line in (current / "documents.jsonl").read_text().splitlines()]
    assert [line["url"] for line in lines] == ["https://doc.rust-lang.org/docs/b"]
    assert lines[0]["main_text"] == "borrowing rules" and lines[0]["source_id"] == "rust"
    published = json.loads((current / "sources.json").read_text())
    assert [s["source_id"] for s in published["sources"]] == ["rust"]
    assert published["disabled"] == [{"source_id": "swift", "reason": "DocC"}]
    assert first.path.exists(), "el bundle anterior se conserva para lectores en curso"
    deleted_key = document_key("vertical-documents", "https://doc.rust-lang.org/docs/a")
    assert not await storage.object_exists("b", deleted_key)


async def test_export_without_snapshot_returns_none(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    sources = VerticalSources(total_page_budget=1, sources=(_source("a", "http://a.test"),))
    assert await export_bundle(storage, "b", "none", "docs", sources, tmp_path / "out") is None
