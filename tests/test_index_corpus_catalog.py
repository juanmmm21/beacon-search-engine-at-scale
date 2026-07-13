"""Tests del catálogo de corpus (`index/corpus_catalog.py`): materialización
con desglose por fichero de parte (contra `LocalFilesystemObjectStorage`
directamente, sin mocks), resolución `doc_id -> parte` por búsqueda binaria,
round-trip JSON, y los artefactos nuevos que `IndexingPipeline` publica
(catálogo + marcadores de versión), incluida la detección de un manifiesto de
fase 2 desalineado con las particiones reales."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from beacon_scale_infra.errors import IndexingError
from beacon_scale_infra.extract.manifest import PartitionManifestEntry, write_partition_manifest
from beacon_scale_infra.index.corpus_catalog import (
    CorpusCatalog,
    CorpusPartEntry,
    materialize_partition_with_parts,
)
from beacon_scale_infra.index.index_version import parse_index_version_marker
from beacon_scale_infra.index.models import IndexingPipelineConfig
from beacon_scale_infra.index.pipeline import IndexingPipeline
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_PREFIX = "extracted-documents"


def _doc(url: str, text: str, fetched_at: str) -> str:
    return json.dumps(
        {"url": url, "title": url, "main_text": text, "fetched_at": fetched_at},
        ensure_ascii=False,
    )


async def _write_part(
    storage: LocalFilesystemObjectStorage, partition: str, part_seq: int, body: str
) -> str:
    key = f"{_PREFIX}/partition={partition}/documents-{part_seq:06d}.jsonl"
    await storage.put_object(_BUCKET, key, body.encode("utf-8"), content_type="application/jsonl")
    return key


async def test_materialization_reports_per_part_ranges_and_max_fetched_at(
    tmp_path: Path,
) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    key_0 = await _write_part(
        storage,
        "worker-a",
        0,
        _doc("https://e.com/0", "uno", "2026-07-01T10:00:00+00:00")
        + "\n"
        + _doc("https://e.com/1", "dos", "2026-07-03T10:00:00+00:00")
        + "\n",
    )
    # Segunda parte sin salto de línea final y con una línea en blanco
    # intermedia: ni lo uno ni lo otro puede desalinear el recuento (las
    # líneas en blanco no consumen doc_id, mismo criterio que IndexBuilder).
    key_1 = await _write_part(
        storage,
        "worker-a",
        1,
        _doc("https://e.com/2", "tres", "2026-07-02T10:00:00+00:00")
        + "\n\n"
        + _doc("https://e.com/3", "cuatro", "2026-06-30T10:00:00+00:00"),
    )

    destination = tmp_path / "materialized.jsonl"
    materialization = await materialize_partition_with_parts(
        storage, _BUCKET, _PREFIX, "worker-a", destination, start_doc_id=10
    )

    assert materialization.document_count == 4
    assert materialization.last_fetched_at == "2026-07-03T10:00:00+00:00"
    assert materialization.parts == (
        CorpusPartEntry(
            partition_key="worker-a", object_key=key_0, start_doc_id=10, document_count=2
        ),
        CorpusPartEntry(
            partition_key="worker-a", object_key=key_1, start_doc_id=12, document_count=2
        ),
    )

    # El fichero materializado sigue siendo un JSONL válido línea a línea
    # aunque una parte no terminase en salto de línea.
    lines = [line for line in destination.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [json.loads(line)["url"] for line in lines] == [
        "https://e.com/0",
        "https://e.com/1",
        "https://e.com/2",
        "https://e.com/3",
    ]


async def test_unreadable_part_line_raises_indexing_error_with_the_part_key(
    tmp_path: Path,
) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    key = await _write_part(storage, "worker-a", 0, "esto no es json\n")

    with pytest.raises(IndexingError) as exc_info:
        await materialize_partition_with_parts(
            storage, _BUCKET, _PREFIX, "worker-a", tmp_path / "out.jsonl", start_doc_id=0
        )
    assert key in str(exc_info.value)


def test_part_for_resolves_by_binary_search_over_ranges() -> None:
    catalog = CorpusCatalog(
        index_version="v",
        total_documents=7,
        last_crawled_at=None,
        parts=(
            CorpusPartEntry(partition_key="a", object_key="p0", start_doc_id=0, document_count=3),
            CorpusPartEntry(partition_key="a", object_key="p1", start_doc_id=3, document_count=1),
            CorpusPartEntry(partition_key="b", object_key="p2", start_doc_id=4, document_count=3),
        ),
    )
    assert catalog.part_for(0) is not None and catalog.part_for(0).object_key == "p0"
    assert catalog.part_for(2) is not None and catalog.part_for(2).object_key == "p0"
    assert catalog.part_for(3) is not None and catalog.part_for(3).object_key == "p1"
    assert catalog.part_for(6) is not None and catalog.part_for(6).object_key == "p2"
    assert catalog.part_for(7) is None
    assert catalog.part_for(-1) is None


def test_catalog_roundtrips_through_json() -> None:
    catalog = CorpusCatalog(
        index_version="abc",
        total_documents=2,
        last_crawled_at="2026-07-01T00:00:00+00:00",
        parts=(
            CorpusPartEntry(partition_key="a", object_key="p0", start_doc_id=0, document_count=2),
        ),
    )
    assert CorpusCatalog.from_json_dict(catalog.to_json_dict()) == catalog


def test_unsorted_parts_are_rejected() -> None:
    with pytest.raises(IndexingError):
        CorpusCatalog(
            index_version="v",
            total_documents=2,
            last_crawled_at=None,
            parts=(
                CorpusPartEntry(
                    partition_key="a", object_key="p1", start_doc_id=1, document_count=1
                ),
                CorpusPartEntry(
                    partition_key="a", object_key="p0", start_doc_id=0, document_count=1
                ),
            ),
        )


async def _write_manifest(
    storage: LocalFilesystemObjectStorage, partition: str, document_count: int, parts: int
) -> None:
    await write_partition_manifest(
        storage,
        _BUCKET,
        _PREFIX,
        PartitionManifestEntry(
            partition_key=partition,
            document_count=document_count,
            discarded_count=0,
            part_file_count=parts,
        ),
    )


async def test_pipeline_publishes_catalog_and_version_markers(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    await _write_part(
        storage,
        "worker-a",
        0,
        _doc("https://e.com/0", "search engine", "2026-07-01T10:00:00+00:00") + "\n",
    )
    await _write_part(
        storage,
        "worker-b",
        0,
        _doc("https://e.com/1", "engine room", "2026-07-05T10:00:00+00:00") + "\n",
    )
    await _write_manifest(storage, "worker-a", 1, 1)
    await _write_manifest(storage, "worker-b", 1, 1)

    config = IndexingPipelineConfig(bucket=_BUCKET, extract_prefix=_PREFIX, compress=True)
    stats = await IndexingPipeline(config, storage=storage).run()

    catalog = CorpusCatalog.from_json_dict(
        json.loads(await storage.get_object(_BUCKET, config.corpus_catalog_object_key))
    )
    assert catalog.total_documents == 2
    assert catalog.last_crawled_at == "2026-07-05T10:00:00+00:00"
    assert [part.start_doc_id for part in catalog.parts] == [0, 1]
    assert catalog.index_version == stats.index_version

    plain_marker = parse_index_version_marker(
        await storage.get_object(_BUCKET, "search-index/index_version.json")
    )
    compressed_marker = parse_index_version_marker(
        await storage.get_object(_BUCKET, "search-index-compressed/index_version.json")
    )
    # Un único índice lógico por build: el mismo marcador acompaña a las dos
    # variantes (comprimida y sin comprimir).
    assert plain_marker == compressed_marker == stats.index_version


async def test_index_version_is_deterministic_and_content_sensitive(tmp_path: Path) -> None:
    async def build_version(root: Path, text: str) -> str:
        storage = LocalFilesystemObjectStorage(root)
        line = _doc("https://e.com/0", text, "2026-07-01T10:00:00+00:00") + "\n"
        await _write_part(storage, "worker-a", 0, line)
        await _write_manifest(storage, "worker-a", 1, 1)
        config = IndexingPipelineConfig(bucket=_BUCKET, extract_prefix=_PREFIX, compress=False)
        stats = await IndexingPipeline(config, storage=storage).run()
        return stats.index_version

    version_a = await build_version(tmp_path / "a", "mismo corpus")
    version_b = await build_version(tmp_path / "b", "mismo corpus")
    version_c = await build_version(tmp_path / "c", "otro corpus distinto")

    # Mismo corpus -> misma versión (los resultados cacheados siguen siendo
    # válidos tras una reconstrucción idéntica); corpus distinto -> versión
    # distinta (namespace de caché nuevo).
    assert version_a == version_b
    assert version_a != version_c


async def test_manifest_out_of_sync_with_partitions_raises(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    await _write_part(
        storage,
        "worker-a",
        0,
        _doc("https://e.com/0", "solo un documento", "2026-07-01T10:00:00+00:00") + "\n",
    )
    # El manifiesto declara 3 documentos pero la partición solo tiene 1: el
    # escenario "un extract-worker seguía escribiendo al leer el manifiesto".
    await _write_manifest(storage, "worker-a", 3, 1)

    config = IndexingPipelineConfig(bucket=_BUCKET, extract_prefix=_PREFIX, compress=False)
    with pytest.raises(IndexingError):
        await IndexingPipeline(config, storage=storage).run()
