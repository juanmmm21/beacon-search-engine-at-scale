"""Tests de `materialize_link_graph`: sobre `LocalFilesystemObjectStorage`
real (sin mocks), verifica que las páginas se materializan en la forma
`{"url": ..., "outlinks": [...]}` que
`pagerank_link_analysis.link_graph_reader.read_link_graph_entries` espera,
que la concurrencia acotada no cambia el resultado, y que una página ausente
o malformada se cuenta sin abortar el resto del escaneo (ver
`ARCHITECTURE.md`, fase 4, sección 3)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

from beacon_scale_infra.errors import ObjectNotFoundError
from beacon_scale_infra.models import ObjectMetadata
from beacon_scale_infra.pagerank.link_graph_materializer import materialize_link_graph
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_PREFIX = "crawl-pages"


def _page(url: str, outlinks: list[str]) -> bytes:
    return json.dumps({"final_url": url, "outlinks": outlinks}).encode("utf-8")


async def test_materializes_pages_into_link_graph_jsonl_shape(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await storage.put_object(
        _BUCKET, f"{_PREFIX}/0.json", _page("https://example.com/a", ["https://example.com/b"])
    )
    await storage.put_object(_BUCKET, f"{_PREFIX}/1.json", _page("https://example.com/b", []))

    destination = tmp_path / "link_graph.jsonl"
    stats = await materialize_link_graph(
        storage, _BUCKET, _PREFIX, destination, max_concurrent_reads=8
    )

    assert stats.pages_materialized == 2
    assert stats.pages_missing == 0
    assert stats.pages_skipped_malformed == 0

    lines = [json.loads(line) for line in destination.read_text(encoding="utf-8").splitlines()]
    by_url = {entry["url"]: entry["outlinks"] for entry in lines}
    assert by_url == {
        "https://example.com/a": ["https://example.com/b"],
        "https://example.com/b": [],
    }


async def test_malformed_page_is_counted_and_skipped(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await storage.put_object(_BUCKET, f"{_PREFIX}/0.json", _page("https://example.com/a", []))
    await storage.put_object(_BUCKET, f"{_PREFIX}/1.json", b"not valid json")
    await storage.put_object(
        _BUCKET,
        f"{_PREFIX}/2.json",
        json.dumps({"final_url": "https://example.com/c"}).encode("utf-8"),  # falta "outlinks"
    )

    destination = tmp_path / "link_graph.jsonl"
    stats = await materialize_link_graph(
        storage, _BUCKET, _PREFIX, destination, max_concurrent_reads=2
    )

    assert stats.pages_materialized == 1
    assert stats.pages_missing == 0
    assert stats.pages_skipped_malformed == 2
    lines = destination.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1


async def test_concurrency_bound_does_not_change_the_result(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    urls = [f"https://example.com/{i}" for i in range(25)]
    for index, url in enumerate(urls):
        await storage.put_object(_BUCKET, f"{_PREFIX}/{index}.json", _page(url, []))

    materialized_url_sets = []
    for max_concurrent_reads in (1, 4, 100):
        destination = tmp_path / f"link_graph-{max_concurrent_reads}.jsonl"
        stats = await materialize_link_graph(
            storage, _BUCKET, _PREFIX, destination, max_concurrent_reads=max_concurrent_reads
        )
        assert stats.pages_materialized == 25
        materialized_url_sets.append(
            {
                json.loads(line)["url"]
                for line in destination.read_text(encoding="utf-8").splitlines()
            }
        )

    assert materialized_url_sets[0] == materialized_url_sets[1] == materialized_url_sets[2]
    assert materialized_url_sets[0] == set(urls)


class _FlakyObjectStorage:
    """Doble fiel mínimo de `ObjectStorage`: delega en un
    `LocalFilesystemObjectStorage` real pero simula que una clave concreta ya
    desapareció entre el listado y la lectura -- la misma condición de
    carrera que `ExtractWorker._process_job` ya maneja en fase 2 (ver
    `ARCHITECTURE.md`, fase 2, "extract-worker reports pages_missing > 0"),
    aplicada aquí al escaneo de `crawl-pages/`."""

    def __init__(self, delegate: LocalFilesystemObjectStorage, missing_key: str) -> None:
        self._delegate = delegate
        self._missing_key = missing_key

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> ObjectMetadata:
        return await self._delegate.put_object(bucket, key, data, content_type=content_type)

    async def get_object(self, bucket: str, key: str) -> bytes:
        if key == self._missing_key:
            raise ObjectNotFoundError(f"{key} ya no existe")
        return await self._delegate.get_object(bucket, key)

    async def delete_object(self, bucket: str, key: str) -> None:
        await self._delegate.delete_object(bucket, key)

    async def object_exists(self, bucket: str, key: str) -> bool:
        return await self._delegate.object_exists(bucket, key)

    def list_objects(self, bucket: str, prefix: str = "") -> AsyncIterator[ObjectMetadata]:
        return self._delegate.list_objects(bucket, prefix=prefix)


async def test_page_missing_between_listing_and_read_is_counted_not_fatal(tmp_path: Path) -> None:
    delegate = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await delegate.put_object(_BUCKET, f"{_PREFIX}/0.json", _page("https://example.com/a", []))
    await delegate.put_object(_BUCKET, f"{_PREFIX}/1.json", _page("https://example.com/b", []))
    storage = _FlakyObjectStorage(delegate, missing_key=f"{_PREFIX}/1.json")

    destination = tmp_path / "link_graph.jsonl"
    stats = await materialize_link_graph(
        storage, _BUCKET, _PREFIX, destination, max_concurrent_reads=4
    )

    assert stats.pages_materialized == 1
    assert stats.pages_missing == 1
    assert stats.pages_skipped_malformed == 0


async def test_empty_prefix_produces_an_empty_link_graph(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    destination = tmp_path / "link_graph.jsonl"

    stats = await materialize_link_graph(storage, _BUCKET, _PREFIX, destination)

    assert stats.pages_materialized == 0
    assert stats.pages_missing == 0
    assert stats.pages_skipped_malformed == 0
    assert destination.read_text(encoding="utf-8") == ""
