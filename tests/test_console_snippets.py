"""Tests de `PartitionedSnippetResolver` contra `LocalFilesystemObjectStorage`
directamente (sin mocks): resolución a través de límites de parte y de
partición, LRU acotada de partes calientes (verificada contando lecturas
reales sobre una subclase del backend local, no un mock), y las tres
degradaciones a `None` (doc_id fuera de rango, parte desaparecida, parte
corrupta)."""

from __future__ import annotations

import json
from pathlib import Path

from beacon_scale_infra.console.snippets import PartitionedSnippetResolver
from beacon_scale_infra.index.corpus_catalog import CorpusCatalog, CorpusPartEntry
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"


class _CountingLocalStorage(LocalFilesystemObjectStorage):
    """Backend local real con un contador de lecturas: la LRU se verifica por
    su efecto observable (cuántas veces se relee cada objeto), nunca
    inspeccionando el estado interno del resolver."""

    def __init__(self, root_dir: Path) -> None:
        super().__init__(root_dir)
        self.get_calls: dict[str, int] = {}

    async def get_object(self, bucket: str, key: str) -> bytes:
        self.get_calls[key] = self.get_calls.get(key, 0) + 1
        return await super().get_object(bucket, key)


def _doc_line(url: str, text: str) -> str:
    return json.dumps({"url": url, "title": f"título {url}", "main_text": text})


async def _setup(
    tmp_path: Path,
) -> tuple[_CountingLocalStorage, CorpusCatalog]:
    storage = _CountingLocalStorage(tmp_path / "storage")
    parts = {
        "extracted-documents/partition=a/documents-000000.jsonl": [
            _doc_line("https://e.com/0", "texto cero"),
            _doc_line("https://e.com/1", "texto uno"),
        ],
        "extracted-documents/partition=a/documents-000001.jsonl": [
            _doc_line("https://e.com/2", "texto dos"),
        ],
        "extracted-documents/partition=b/documents-000000.jsonl": [
            _doc_line("https://e.com/3", "texto tres"),
        ],
    }
    for key, lines in parts.items():
        await storage.put_object(_BUCKET, key, ("\n".join(lines) + "\n").encode("utf-8"))
    catalog = CorpusCatalog(
        index_version="v1",
        total_documents=4,
        last_crawled_at=None,
        parts=(
            CorpusPartEntry(
                partition_key="a",
                object_key="extracted-documents/partition=a/documents-000000.jsonl",
                start_doc_id=0,
                document_count=2,
            ),
            CorpusPartEntry(
                partition_key="a",
                object_key="extracted-documents/partition=a/documents-000001.jsonl",
                start_doc_id=2,
                document_count=1,
            ),
            CorpusPartEntry(
                partition_key="b",
                object_key="extracted-documents/partition=b/documents-000000.jsonl",
                start_doc_id=3,
                document_count=1,
            ),
        ),
    )
    return storage, catalog


async def test_resolves_across_part_and_partition_boundaries(tmp_path: Path) -> None:
    storage, catalog = await _setup(tmp_path)
    resolver = PartitionedSnippetResolver(storage, _BUCKET, catalog)

    for doc_id, expected_url, expected_text in (
        (0, "https://e.com/0", "texto cero"),
        (1, "https://e.com/1", "texto uno"),
        (2, "https://e.com/2", "texto dos"),
        (3, "https://e.com/3", "texto tres"),
    ):
        document = await resolver.resolve(doc_id)
        assert document is not None
        assert document.url == expected_url
        assert document.main_text == expected_text
        assert document.title == f"título {expected_url}"


async def test_hot_part_is_downloaded_once(tmp_path: Path) -> None:
    storage, catalog = await _setup(tmp_path)
    resolver = PartitionedSnippetResolver(storage, _BUCKET, catalog, max_cached_parts=4)

    assert await resolver.resolve(0) is not None
    assert await resolver.resolve(1) is not None
    assert await resolver.resolve(0) is not None

    key = "extracted-documents/partition=a/documents-000000.jsonl"
    assert storage.get_calls[key] == 1


async def test_lru_evicts_the_coldest_part(tmp_path: Path) -> None:
    storage, catalog = await _setup(tmp_path)
    resolver = PartitionedSnippetResolver(storage, _BUCKET, catalog, max_cached_parts=1)

    assert await resolver.resolve(0) is not None  # descarga parte a/0
    assert await resolver.resolve(3) is not None  # descarga parte b/0, expulsa a/0
    assert await resolver.resolve(0) is not None  # a/0 se vuelve a descargar

    key = "extracted-documents/partition=a/documents-000000.jsonl"
    assert storage.get_calls[key] == 2


async def test_out_of_range_doc_id_resolves_to_none(tmp_path: Path) -> None:
    storage, catalog = await _setup(tmp_path)
    resolver = PartitionedSnippetResolver(storage, _BUCKET, catalog)
    assert await resolver.resolve(99) is None


async def test_missing_part_degrades_to_none(tmp_path: Path) -> None:
    storage, catalog = await _setup(tmp_path)
    await storage.delete_object(_BUCKET, "extracted-documents/partition=b/documents-000000.jsonl")
    resolver = PartitionedSnippetResolver(storage, _BUCKET, catalog)
    assert await resolver.resolve(3) is None
    # El resto del corpus sigue resolviendo: nunca un fallo por una parte
    # ausente contamina a las demás.
    assert await resolver.resolve(0) is not None


async def test_corrupt_part_degrades_to_none(tmp_path: Path) -> None:
    storage, catalog = await _setup(tmp_path)
    await storage.put_object(
        _BUCKET,
        "extracted-documents/partition=a/documents-000001.jsonl",
        b"esto no es json\n",
    )
    resolver = PartitionedSnippetResolver(storage, _BUCKET, catalog)
    assert await resolver.resolve(2) is None


async def test_part_shorter_than_catalog_degrades_to_none(tmp_path: Path) -> None:
    storage, catalog = await _setup(tmp_path)
    # La parte a/0 queda con un único documento aunque el catálogo declare 2:
    # el doc_id 1 ya no existe en esa parte (builds mezcladas en el bucket).
    await storage.put_object(
        _BUCKET,
        "extracted-documents/partition=a/documents-000000.jsonl",
        (_doc_line("https://e.com/0", "texto cero") + "\n").encode("utf-8"),
    )
    resolver = PartitionedSnippetResolver(storage, _BUCKET, catalog)
    assert await resolver.resolve(1) is None
    assert await resolver.resolve(0) is not None
