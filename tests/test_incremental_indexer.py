"""Tests de la indexación incremental con la cola en memoria y el
almacenamiento en disco local, sin mocks (mismo criterio que el resto de
backends locales). La propiedad central: aplicar altas, actualizaciones y
bajas por la cola deja publicado un índice cuyo contenido vivo es el mismo
que reconstruirlo desde cero con los documentos finales."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from inverted_index_builder.pipeline import IndexBuilder
from inverted_index_builder.segments import SegmentedIndex

from beacon_scale_infra.errors import IncrementalIndexingError
from beacon_scale_infra.extract.models import ExtractWorkerConfig
from beacon_scale_infra.extract.worker import ExtractWorker
from beacon_scale_infra.incremental.indexer import (
    IncrementalIndexer,
    coalesce_updates,
    publish_updates,
)
from beacon_scale_infra.incremental.models import (
    IncrementalIndexerConfig,
    IndexUpdate,
    IndexUpdateOperation,
)
from beacon_scale_infra.incremental.recrawl_feed import updates_from_recrawl
from beacon_scale_infra.incremental.snapshot import restore_snapshot
from beacon_scale_infra.index.index_version import parse_index_version_marker
from beacon_scale_infra.queue.memory import InMemoryMessageQueue
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_PREFIX = "search-index-segmented"
_STREAM = "index-updates"

_ARTICLE_HTML = """
<html lang="en"><head><title>{title}</title></head><body>
<article><h1>{title}</h1>
<p>{body} This paragraph adds enough real prose about search engines, inverted
indexes and ranking so that the content extractor keeps it as main content and
does not discard the page as boilerplate or as an empty page.</p>
<p>A second paragraph keeps going about tokenization, compression of posting
lists and the trade-offs of building a search engine completely from scratch.</p>
</article></body></html>
"""


def _document(url: str, text: str) -> dict[str, Any]:
    return {
        "url": url,
        "final_url": url,
        "title": url.rsplit("/", 1)[-1],
        "main_text": text,
        "language": "en",
        "author": None,
        "published_at": None,
        "fetched_at": "2026-11-05T09:00:00+00:00",
        "extracted_at": "2026-11-05T09:00:01+00:00",
        "encoding_used": "utf-8",
        "depth": 0,
        "word_count": len(text.split()),
    }


def _config(tmp_path: Path, **overrides: Any) -> IncrementalIndexerConfig:
    defaults: dict[str, Any] = {
        "worker_id": "indexer-test",
        "local_root": tmp_path / "work",
        "stream": _STREAM,
        "bucket": _BUCKET,
        "index_prefix": _PREFIX,
        "poll_block_ms": 10,
        "idle_polls_before_shutdown": 1,
    }
    defaults.update(overrides)
    return IncrementalIndexerConfig(**defaults)


def _live_texts(root: Path) -> dict[str, str]:
    """`url -> texto indexado` de los documentos vivos, vía `compact`."""
    compacted = SegmentedIndex.open(root).compact(root.parent / f"{root.name}-compact")
    texts: dict[str, str] = {}
    for record in compacted.documents.values():
        texts[record.url] = record.title or ""
    return texts


def _terms_of(root: Path) -> dict[str, list[str]]:
    """`término -> urls vivas que lo contienen`, ordenadas: el contenido
    observable del índice, independiente de los `doc_id` asignados."""
    compacted = SegmentedIndex.open(root).compact(root.parent / f"{root.name}-terms")
    url_by_id = {doc_id: record.url for doc_id, record in compacted.documents.items()}
    return {
        term: sorted(url_by_id[p.doc_id] for p in postings.postings)
        for term, postings in compacted.postings_lists.items()
    }


def test_update_payload_round_trip_and_validation() -> None:
    upsert = IndexUpdate.upsert(_document("https://a.test/x", "alpha"))
    assert IndexUpdate.from_payload(json.loads(json.dumps(upsert.to_payload()))) == upsert
    delete = IndexUpdate.delete("https://a.test/x")
    assert IndexUpdate.from_payload(delete.to_payload()) == delete
    with pytest.raises(ValueError):
        IndexUpdate(IndexUpdateOperation.UPSERT, "https://a.test/x")
    with pytest.raises(ValueError):
        IndexUpdate(IndexUpdateOperation.UPSERT, "https://a.test/x", {"url": "https://b.test"})
    with pytest.raises(ValueError):
        IndexUpdate.from_payload({"operation": "rename", "url": "https://a.test/x"})


def test_coalesce_keeps_last_operation_per_url() -> None:
    first = IndexUpdate.upsert(_document("https://a.test/1", "one"))
    second = IndexUpdate.upsert(_document("https://a.test/1", "one again"))
    other = IndexUpdate.upsert(_document("https://a.test/2", "two"))
    gone = IndexUpdate.delete("https://a.test/2")
    upserts, deletes = coalesce_updates([first, other, second, gone])
    assert upserts == [second]
    assert deletes == ["https://a.test/2"]


async def test_updates_are_applied_published_and_equal_a_fresh_build(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    queue = InMemoryMessageQueue()
    await publish_updates(
        queue,
        _STREAM,
        [
            IndexUpdate.upsert(_document(f"https://a.test/{i}", f"page {i} search"))
            for i in range(5)
        ],
    )
    indexer = IncrementalIndexer(_config(tmp_path), queue=queue, storage=storage)
    first = await indexer.run()
    assert first.documents_upserted == 5
    assert first.live_documents == 5
    marker = await storage.get_object(_BUCKET, f"{_PREFIX}/index_version.json")
    first_version = parse_index_version_marker(marker)
    assert first_version == first.last_index_version

    await publish_updates(
        queue,
        _STREAM,
        [
            IndexUpdate.upsert(_document("https://a.test/1", "page one rewritten ranking")),
            IndexUpdate.delete("https://a.test/3"),
            IndexUpdate.upsert(_document("https://a.test/9", "brand new page search")),
        ],
    )
    second = await IncrementalIndexer(_config(tmp_path), queue=queue, storage=storage).run()
    assert second.documents_upserted == 2
    assert second.documents_deleted == 1
    assert second.last_index_version != first_version

    final_documents = [
        _document("https://a.test/0", "page 0 search"),
        _document("https://a.test/1", "page one rewritten ranking"),
        _document("https://a.test/2", "page 2 search"),
        _document("https://a.test/4", "page 4 search"),
        _document("https://a.test/9", "brand new page search"),
    ]
    fresh_root = tmp_path / "fresh"
    SegmentedIndex.create(fresh_root, IndexBuilder()).add_documents(final_documents)

    restored_root = tmp_path / "restored"
    assert await restore_snapshot(storage, _BUCKET, _PREFIX, restored_root)
    assert _terms_of(restored_root) == _terms_of(fresh_root)
    assert _terms_of(tmp_path / "work") == _terms_of(fresh_root)


async def test_restart_without_local_copy_restores_and_keeps_doc_ids_growing(
    tmp_path: Path,
) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    queue = InMemoryMessageQueue()
    await publish_updates(queue, _STREAM, [IndexUpdate.upsert(_document("https://a.test/a", "x"))])
    await IncrementalIndexer(_config(tmp_path), queue=queue, storage=storage).run()

    # Otra máquina, sin copia local: arranca de la instantánea publicada.
    other_root = tmp_path / "other-machine"
    await publish_updates(queue, _STREAM, [IndexUpdate.upsert(_document("https://a.test/b", "y"))])
    stats = await IncrementalIndexer(
        _config(tmp_path, local_root=other_root), queue=queue, storage=storage
    ).run()
    assert stats.live_documents == 2
    index = SegmentedIndex.open(other_root)
    assert index.live_doc_id("https://a.test/a") == 0
    assert index.live_doc_id("https://a.test/b") == 1


async def test_merges_run_and_obsolete_segments_leave_storage_after_grace(
    tmp_path: Path,
) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    queue = InMemoryMessageQueue()
    config = _config(tmp_path, merge_segments_per_tier=2)
    indexer = IncrementalIndexer(config, queue=queue, storage=storage)
    for i in range(4):
        await publish_updates(
            queue, _STREAM, [IndexUpdate.upsert(_document(f"https://a.test/{i}", f"doc {i}"))]
        )
        await indexer.run()
    assert indexer.stats.merges >= 1

    manifest = SegmentedIndex.open(config.local_root).manifest
    live_segments = {segment.name for segment in manifest.segments}
    published_segments: set[str] = set()
    async for item in storage.list_objects(_BUCKET, f"{_PREFIX}/seg-"):
        published_segments.add(item.key.split("/")[1])
    # Lo publicado es lo vivo más, como mucho, lo de la generación anterior.
    assert live_segments <= published_segments
    assert len(published_segments) < 4


async def test_invalid_messages_are_counted_acked_and_do_not_block(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    queue = InMemoryMessageQueue()
    await queue.publish(_STREAM, {"operation": "upsert", "url": "https://a.test/x"})
    await publish_updates(queue, _STREAM, [IndexUpdate.upsert(_document("https://a.test/y", "y"))])
    stats = await IncrementalIndexer(_config(tmp_path), queue=queue, storage=storage).run()
    assert stats.invalid_updates == 1
    assert stats.documents_upserted == 1
    again = await IncrementalIndexer(_config(tmp_path), queue=queue, storage=storage).run()
    assert again.updates_received == 0


async def test_restore_refuses_a_non_empty_directory(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    queue = InMemoryMessageQueue()
    await publish_updates(queue, _STREAM, [IndexUpdate.upsert(_document("https://a.test/a", "x"))])
    await IncrementalIndexer(_config(tmp_path), queue=queue, storage=storage).run()
    target = tmp_path / "busy"
    target.mkdir()
    (target / "something").write_text("x")
    with pytest.raises(IncrementalIndexingError):
        await restore_snapshot(storage, _BUCKET, _PREFIX, target)
    assert not await restore_snapshot(storage, _BUCKET, "nothing-here", tmp_path / "empty")


def test_recrawl_outputs_become_upserts_and_deletes(tmp_path: Path) -> None:
    output = tmp_path / "recrawl-001"
    output.mkdir()
    page = {
        "url": "https://a.test/changed",
        "final_url": "https://a.test/changed",
        "status_code": 200,
        "headers": {"Content-Type": "text/html; charset=utf-8"},
        "html": _ARTICLE_HTML.format(title="Changed", body="Fresh content."),
        "fetched_at": "2026-11-05T09:00:00+00:00",
        "depth": 0,
        "content_type": "text/html",
    }
    emptied = dict(page, url="https://a.test/emptied", final_url="https://a.test/emptied")
    emptied["html"] = "<html><body><p>tiny</p></body></html>"
    (output / "pages.jsonl").write_text(json.dumps(page) + "\n" + json.dumps(emptied) + "\n")
    (output / "removed.jsonl").write_text(
        json.dumps({"url": "https://a.test/gone", "status_code": 410}) + "\n"
    )
    updates, summary = updates_from_recrawl(output)
    assert [(u.operation, u.url) for u in updates] == [
        (IndexUpdateOperation.UPSERT, "https://a.test/changed"),
        (IndexUpdateOperation.DELETE, "https://a.test/emptied"),
        (IndexUpdateOperation.DELETE, "https://a.test/gone"),
    ]
    assert summary.upserts == 1 and summary.no_longer_indexable == 1 and summary.removed_pages == 1
    assert updates[0].document is not None and updates[0].document["simhash"]


async def test_extract_worker_publishes_upserts_when_configured(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    queue = InMemoryMessageQueue()
    record = {
        "url": "https://a.test/article",
        "final_url": "https://a.test/article",
        "status_code": 200,
        "headers": {"Content-Type": "text/html; charset=utf-8"},
        "html": _ARTICLE_HTML.format(title="Article", body="Body."),
        "fetched_at": "2026-11-05T09:00:00+00:00",
        "depth": 0,
        "content_type": "text/html",
    }
    await storage.put_object(_BUCKET, "pages/a.json", json.dumps(record).encode())
    await queue.publish("extract", {"bucket": _BUCKET, "key": "pages/a.json"})
    worker = ExtractWorker(
        ExtractWorkerConfig(
            worker_id="w",
            stream="extract",
            group="g",
            bucket=_BUCKET,
            poll_block_ms=10,
            idle_polls_before_shutdown=1,
            index_updates_stream=_STREAM,
        ),
        queue=queue,
        storage=storage,
    )
    await worker.run()
    stats = await IncrementalIndexer(_config(tmp_path), queue=queue, storage=storage).run()
    assert stats.documents_upserted == 1
    assert _live_texts(tmp_path / "work") == {"https://a.test/article": "Article"}


async def test_same_shape_different_content_gets_a_different_version(tmp_path: Path) -> None:
    versions = []
    for name, text in (("a", "alpha search"), ("b", "beta ranking")):
        storage = LocalFilesystemObjectStorage(tmp_path / f"storage-{name}")
        queue = InMemoryMessageQueue()
        await publish_updates(
            queue, _STREAM, [IndexUpdate.upsert(_document("https://a.test/1", text))]
        )
        stats = await IncrementalIndexer(
            _config(tmp_path, local_root=tmp_path / f"work-{name}"), queue=queue, storage=storage
        ).run()
        pointer = json.loads(await storage.get_object(_BUCKET, f"{_PREFIX}/snapshot.json"))
        assert pointer["index_version"] == stats.last_index_version
        versions.append(stats.last_index_version)
    assert versions[0] != versions[1]


async def test_restore_detects_a_manifest_swapped_mid_download(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    queue = InMemoryMessageQueue()
    await publish_updates(queue, _STREAM, [IndexUpdate.upsert(_document("https://a.test/a", "x"))])
    await IncrementalIndexer(_config(tmp_path), queue=queue, storage=storage).run()
    with pytest.raises(IncrementalIndexingError):
        await restore_snapshot(
            storage, _BUCKET, _PREFIX, tmp_path / "r", expected_manifest_sha256="0" * 64
        )
