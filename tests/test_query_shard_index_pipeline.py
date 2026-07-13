"""Tests de `ShardIndexPipeline`: descarga el índice global que fase 3 dejó
en `ObjectStorage`, lo particiona con `distributed_index_sharding.partitioning
.partition_index` (sin modificar) y sube cada shard de vuelta -- ver
`ARCHITECTURE.md`, fase 5.

Genera el índice global de entrada con `IndexingPipeline` (fase 3) real, en
vez de fabricar a mano un directorio en el formato de `inverted-index-builder`
o de `index-compression-codec`: así el test ejercita el mismo objeto que
`shard-index` encontraría de verdad en un despliegue real, para ambos formatos
de origen (comprimido y sin comprimir).
"""

from __future__ import annotations

import json
from pathlib import Path

from distributed_index_sharding.partitioning import ClusterManifest

from beacon_scale_infra.errors import ShardIndexingError
from beacon_scale_infra.extract.manifest import PartitionManifestEntry, write_partition_manifest
from beacon_scale_infra.index.models import IndexingPipelineConfig
from beacon_scale_infra.index.pipeline import IndexingPipeline
from beacon_scale_infra.query.models import ShardIndexPipelineConfig
from beacon_scale_infra.query.shard_index_pipeline import ShardIndexPipeline
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_EXTRACT_PREFIX = "extracted-documents"

_DOCUMENTS: tuple[dict[str, str], ...] = (
    {"url": "https://example.com/0", "title": "Search Basics", "main_text": "search engine basics"},
    {"url": "https://example.com/1", "title": "Index Design", "main_text": "index design search"},
    {"url": "https://example.com/2", "title": "Ranking", "main_text": "ranking a search engine"},
    {"url": "https://example.com/3", "title": "Crawling", "main_text": "crawling the web at scale"},
    {"url": "https://example.com/4", "title": "Storage", "main_text": "object storage for index"},
)


async def _write_extracted_corpus(storage: LocalFilesystemObjectStorage) -> None:
    body = ("\n".join(json.dumps(doc, ensure_ascii=False) for doc in _DOCUMENTS) + "\n").encode(
        "utf-8"
    )
    key = f"{_EXTRACT_PREFIX}/partition=worker-a/documents-000000.jsonl"
    await storage.put_object(_BUCKET, key, body, content_type="application/jsonl")
    await write_partition_manifest(
        storage,
        _BUCKET,
        _EXTRACT_PREFIX,
        PartitionManifestEntry(
            partition_key="worker-a",
            document_count=len(_DOCUMENTS),
            discarded_count=0,
            part_file_count=1,
        ),
    )


async def _build_global_index(storage: LocalFilesystemObjectStorage, *, compress: bool) -> None:
    await _write_extracted_corpus(storage)
    config = IndexingPipelineConfig(
        bucket=_BUCKET, extract_prefix=_EXTRACT_PREFIX, compress=compress
    )
    await IndexingPipeline(config, storage=storage).run()


async def test_shards_a_compressed_global_index(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _build_global_index(storage, compress=True)

    config = ShardIndexPipelineConfig(
        bucket=_BUCKET,
        source_index_prefix="search-index-compressed",
        shard_index_prefix="shard-index",
        num_shards=3,
    )
    stats = await ShardIndexPipeline(config, storage=storage).run()

    assert stats.num_shards == 3
    assert stats.source_files_downloaded > 0
    assert stats.shard_files_uploaded > 0

    manifest_raw = await storage.get_object(_BUCKET, "shard-index/cluster_manifest.json")
    manifest = ClusterManifest.from_json_dict(json.loads(manifest_raw))
    assert manifest.num_shards == 3
    assert manifest.shard_dir_names == ("shard-0", "shard-1", "shard-2")

    # Cada shard es un directorio de inverted-index-builder self-contained e
    # independiente -- sin comprimir, aunque el índice de origen sí lo
    # estuviera (ver distributed_index_sharding.partitioning, sección "Por
    # qué cada shard se escribe en formato sin comprimir").
    for shard_dir_name in manifest.shard_dir_names:
        for filename in ("manifest.json", "documents.jsonl", "postings.jsonl", "stats.json"):
            assert await storage.object_exists(_BUCKET, f"shard-index/{shard_dir_name}/{filename}")

    # doc_id % num_shards, exactamente el criterio de partitioning.py -- cada
    # documento vive en exactamente un shard, ninguno se pierde ni se duplica.
    seen_doc_ids: set[int] = set()
    for shard_id, shard_dir_name in enumerate(manifest.shard_dir_names):
        documents_raw = await storage.get_object(
            _BUCKET, f"shard-index/{shard_dir_name}/documents.jsonl"
        )
        for line in documents_raw.decode("utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            assert record["doc_id"] % 3 == shard_id
            seen_doc_ids.add(record["doc_id"])
    assert seen_doc_ids == set(range(len(_DOCUMENTS)))


async def test_shards_an_uncompressed_global_index(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _build_global_index(storage, compress=False)

    config = ShardIndexPipelineConfig(
        bucket=_BUCKET, source_index_prefix="search-index", num_shards=2
    )
    stats = await ShardIndexPipeline(config, storage=storage).run()

    assert stats.num_shards == 2
    manifest_raw = await storage.get_object(_BUCKET, "shard-index/cluster_manifest.json")
    manifest = ClusterManifest.from_json_dict(json.loads(manifest_raw))
    assert manifest.num_shards == 2


async def test_missing_source_index_raises_shard_indexing_error(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")

    config = ShardIndexPipelineConfig(bucket=_BUCKET, source_index_prefix="search-index-compressed")

    try:
        await ShardIndexPipeline(config, storage=storage).run()
    except ShardIndexingError:
        pass
    else:
        raise AssertionError("se esperaba ShardIndexingError sobre un prefijo vacío")
