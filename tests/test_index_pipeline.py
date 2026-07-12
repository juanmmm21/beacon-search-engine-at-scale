"""Tests de integración de `IndexingPipeline`: la propiedad central de esta
fase (ver `ARCHITECTURE.md`, fase 3, sección 4) es que indexar el mismo
corpus repartido en varias particiones de fase 2 produce el mismo conjunto de
documentos, el mismo vocabulario y las mismas frecuencias por término que
indexarlo como un único `documents.jsonl` con `inverted-index-builder`
directamente -- aunque los `doc_id` numéricos difieran, porque ya no
significan "posición de línea de un único fichero" (ver esa misma sección).

También verifica, con un test explícito en vez de asumirlo de la
documentación compartida, que `index-compression-codec` comprime el índice
fusionado sin ninguna adaptación (sección 3), y que el fichero de corpus
global preserva la resolución posicional `doc_id -> texto` que
`beacon-search-console` necesita (sección 5).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from inverted_index_builder.pipeline import IndexBuilder

from beacon_scale_infra.extract.manifest import PartitionManifestEntry, write_partition_manifest
from beacon_scale_infra.index.models import IndexingPipelineConfig
from beacon_scale_infra.index.pipeline import IndexingPipeline
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_EXTRACT_PREFIX = "extracted-documents"

# Siete documentos con vocabulario deliberadamente solapado ("search",
# "engine", "index") repartidos de forma desigual entre tres particiones y,
# dentro de cada partición, entre dos ficheros de parte -- para ejercitar
# tanto la concatenación entre part-files como la fusión entre particiones.
_DOCUMENTS: tuple[dict[str, str], ...] = (
    {"url": "https://example.com/0", "title": "Search Basics", "main_text": "search engine basics"},
    {
        "url": "https://example.com/1",
        "title": "Index Design",
        "main_text": "index design for search",
    },
    {"url": "https://example.com/2", "title": "Ranking", "main_text": "ranking a search engine"},
    {"url": "https://example.com/3", "title": "Crawling", "main_text": "crawling the web at scale"},
    {
        "url": "https://example.com/4",
        "title": "Storage",
        "main_text": "object storage for an index",
    },
    {
        "url": "https://example.com/5",
        "title": "Compression",
        "main_text": "postings compression engine",
    },
    {
        "url": "https://example.com/6",
        "title": "Query Parsing",
        "main_text": "query parsing and search",
    },
)

# partition_key -> lista de (índice de fichero de parte, [índices en _DOCUMENTS])
_PARTITION_LAYOUT: dict[str, list[list[int]]] = {
    "worker-a": [[0, 1], [2]],
    "worker-b": [[3], [4]],
    "worker-c": [[5, 6]],
}


def _document_line(document: dict[str, str]) -> str:
    return json.dumps(document, ensure_ascii=False)


async def _write_partitioned_corpus(storage: LocalFilesystemObjectStorage) -> None:
    for partition_key, part_files in _PARTITION_LAYOUT.items():
        document_count = 0
        for part_seq, doc_indexes in enumerate(part_files):
            body = ("\n".join(_document_line(_DOCUMENTS[i]) for i in doc_indexes) + "\n").encode(
                "utf-8"
            )
            key = f"{_EXTRACT_PREFIX}/partition={partition_key}/documents-{part_seq:06d}.jsonl"
            await storage.put_object(_BUCKET, key, body, content_type="application/jsonl")
            document_count += len(doc_indexes)
        await write_partition_manifest(
            storage,
            _BUCKET,
            _EXTRACT_PREFIX,
            PartitionManifestEntry(
                partition_key=partition_key,
                document_count=document_count,
                discarded_count=0,
                part_file_count=len(part_files),
            ),
        )


def _build_reference_index_from_a_single_file(tmp_path: Path) -> Any:
    # Orden deliberadamente distinto al de las particiones (orden inverso de
    # _DOCUMENTS), para no depender por accidente de que ambas construcciones
    # coincidan en el orden de concatenación.
    reference_path = tmp_path / "reference-documents.jsonl"
    reference_path.write_text(
        "\n".join(_document_line(doc) for doc in reversed(_DOCUMENTS)) + "\n", encoding="utf-8"
    )
    return IndexBuilder().build(reference_path)


def _term_frequency_by_url(index: Any, url_by_doc_id: dict[int, str]) -> dict[str, dict[str, int]]:
    return {
        term: {
            url_by_doc_id[posting.doc_id]: posting.term_frequency
            for posting in postings_list.postings
        }
        for term, postings_list in index.postings_lists.items()
    }


async def _read_jsonl(storage: LocalFilesystemObjectStorage, key: str) -> list[dict[str, Any]]:
    raw = await storage.get_object(_BUCKET, key)
    return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]


async def test_partitioned_index_matches_single_file_build_modulo_doc_ids(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _write_partitioned_corpus(storage)

    config = IndexingPipelineConfig(
        bucket=_BUCKET,
        extract_prefix=_EXTRACT_PREFIX,
        index_output_prefix="search-index",
        corpus_object_key="search-index/corpus/documents.jsonl",
        compress=True,
        compressed_output_prefix="search-index-compressed",
    )
    stats = await IndexingPipeline(config, storage=storage).run()

    assert stats.partitions_indexed == 3
    assert stats.total_documents == len(_DOCUMENTS)

    uploaded_documents = await _read_jsonl(storage, "search-index/documents.jsonl")
    uploaded_postings = await _read_jsonl(storage, "search-index/postings.jsonl")
    url_by_doc_id = {int(record["doc_id"]): str(record["url"]) for record in uploaded_documents}

    # El espacio de doc_id debe quedar denso y sin huecos: 0..N-1 exactamente
    # una vez, condición de la que depende `JsonlDocumentIdResolver`
    # (pagerank-link-analysis) -- ver ARCHITECTURE.md, fase 3, sección 5.
    assert sorted(url_by_doc_id) == list(range(len(_DOCUMENTS)))
    assert set(url_by_doc_id.values()) == {doc["url"] for doc in _DOCUMENTS}

    pipeline_term_frequencies = {
        record["term"]: {
            url_by_doc_id[int(posting["doc_id"])]: int(posting["term_frequency"])
            for posting in record["postings"]
        }
        for record in uploaded_postings
    }

    reference_index = _build_reference_index_from_a_single_file(tmp_path)
    reference_url_by_doc_id = {
        record.doc_id: record.url for record in reference_index.documents.values()
    }
    reference_term_frequencies = _term_frequency_by_url(reference_index, reference_url_by_doc_id)

    assert pipeline_term_frequencies == reference_term_frequencies


async def test_corpus_file_line_position_equals_global_doc_id(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _write_partitioned_corpus(storage)

    config = IndexingPipelineConfig(
        bucket=_BUCKET,
        extract_prefix=_EXTRACT_PREFIX,
        compress=False,
    )
    await IndexingPipeline(config, storage=storage).run()

    uploaded_documents = await _read_jsonl(storage, "search-index/documents.jsonl")
    corpus_lines = (
        (await storage.get_object(_BUCKET, config.corpus_object_key)).decode("utf-8").splitlines()
    )

    assert len(corpus_lines) == len(_DOCUMENTS)
    for record in uploaded_documents:
        doc_id = int(record["doc_id"])
        corpus_record = json.loads(corpus_lines[doc_id])
        # El fichero de corpus (con main_text, el mismo que
        # beacon-search-console necesita para snippets) debe estar alineado
        # posicionalmente con doc_id -- exactamente lo que `SnippetIndex`
        # asume al indexar `self._documents[doc_id]` (ver
        # ARCHITECTURE.md, fase 3, sección 5).
        assert corpus_record["url"] == record["url"]
        assert "main_text" in corpus_record


async def test_compression_pipeline_accepts_the_merged_index_without_adaptation(
    tmp_path: Path,
) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _write_partitioned_corpus(storage)

    config = IndexingPipelineConfig(bucket=_BUCKET, extract_prefix=_EXTRACT_PREFIX, compress=True)
    stats = await IndexingPipeline(config, storage=storage).run()

    assert stats.compression_ratio is not None
    assert stats.compression_ratio > 0

    compression_stats = json.loads(
        await storage.get_object(_BUCKET, "search-index-compressed/compression_stats.json")
    )
    assert compression_stats["total_documents"] == len(_DOCUMENTS)

    # documents.jsonl/stats.json se copian sin modificar a través del códec
    # (contrato documentado en index-compression-codec/serialization.py).
    uncompressed_documents = await storage.get_object(_BUCKET, "search-index/documents.jsonl")
    compressed_documents = await storage.get_object(
        _BUCKET, "search-index-compressed/documents.jsonl"
    )
    assert uncompressed_documents == compressed_documents

    # postings.bin (binario) debe existir junto al directorio de términos.
    assert await storage.object_exists(_BUCKET, "search-index-compressed/postings.bin")
    assert await storage.object_exists(_BUCKET, "search-index-compressed/terms.jsonl")


async def test_pipeline_without_compression_skips_the_compressed_prefix(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _write_partitioned_corpus(storage)

    config = IndexingPipelineConfig(bucket=_BUCKET, extract_prefix=_EXTRACT_PREFIX, compress=False)
    stats = await IndexingPipeline(config, storage=storage).run()

    assert stats.compression_ratio is None
    assert not await storage.object_exists(_BUCKET, "search-index-compressed/manifest.json")


async def test_pipeline_over_an_empty_corpus_produces_an_empty_index(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")

    config = IndexingPipelineConfig(bucket=_BUCKET, extract_prefix=_EXTRACT_PREFIX, compress=True)
    stats = await IndexingPipeline(config, storage=storage).run()

    assert stats.partitions_indexed == 0
    assert stats.total_documents == 0
    uploaded_stats = json.loads(await storage.get_object(_BUCKET, "search-index/stats.json"))
    assert uploaded_stats["total_documents"] == 0
