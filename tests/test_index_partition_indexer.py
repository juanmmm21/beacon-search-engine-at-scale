"""Tests del paso *map*: `materialize_partition_documents` concatena los
ficheros de parte de una partición en el orden correcto (I/O, contra
`LocalFilesystemObjectStorage` sin dobles, ver `CLAUDE.md`), y
`build_index_from_materialized_partition`/`remap_index_to_global_doc_ids` son
funciones puras sobre un fichero local ya materializado (ver
`ARCHITECTURE.md`, fase 3, secciones 1 y 2)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from beacon_scale_infra.errors import IndexingError
from beacon_scale_infra.index.partition_indexer import (
    build_index_from_materialized_partition,
    materialize_partition_documents,
    remap_index_to_global_doc_ids,
)
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_PREFIX = "extracted-documents"


def _document_line(url: str, title: str, main_text: str) -> str:
    return json.dumps({"url": url, "title": title, "main_text": main_text}, ensure_ascii=False)


async def test_materialize_concatenates_part_files_in_ascending_part_seq_order(
    tmp_path: Path,
) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    # Se escriben fuera de orden para verificar que la concatenación ordena
    # por clave (zero-padding de part_seq), no por orden de escritura.
    await storage.put_object(
        _BUCKET,
        f"{_PREFIX}/partition=worker-a/documents-000001.jsonl",
        (_document_line("https://example.com/2", "Two", "goodbye world") + "\n").encode("utf-8"),
    )
    await storage.put_object(
        _BUCKET,
        f"{_PREFIX}/partition=worker-a/documents-000000.jsonl",
        (
            _document_line("https://example.com/0", "Zero", "hello world")
            + "\n"
            + _document_line("https://example.com/1", "One", "hello again")
            + "\n"
        ).encode("utf-8"),
    )
    # Un discarded-*.jsonl en la misma partición nunca debe colarse en la
    # materialización de documents.jsonl.
    await storage.put_object(
        _BUCKET,
        f"{_PREFIX}/partition=worker-a/discarded-000000.jsonl",
        b'{"url": "https://example.com/bad", "reason": "non_html_content"}\n',
    )

    destination = tmp_path / "materialized.jsonl"
    part_count = await materialize_partition_documents(
        storage, _BUCKET, _PREFIX, "worker-a", destination
    )

    assert part_count == 2
    lines = destination.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["url"] for line in lines] == [
        "https://example.com/0",
        "https://example.com/1",
        "https://example.com/2",
    ]


async def test_materialize_on_partition_with_no_part_files_produces_empty_file(
    tmp_path: Path,
) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    destination = tmp_path / "materialized.jsonl"

    part_count = await materialize_partition_documents(
        storage, _BUCKET, _PREFIX, "worker-empty", destination
    )

    assert part_count == 0
    assert destination.read_text(encoding="utf-8") == ""


def test_build_index_from_materialized_partition_assigns_local_then_remaps(tmp_path: Path) -> None:
    materialized = tmp_path / "materialized.jsonl"
    materialized.write_text(
        _document_line("https://example.com/0", "Zero", "hello world")
        + "\n"
        + _document_line("https://example.com/1", "One", "hello again")
        + "\n",
        encoding="utf-8",
    )

    remapped = build_index_from_materialized_partition(materialized, doc_id_offset=10)

    assert set(remapped.documents) == {10, 11}
    assert remapped.documents[10].url == "https://example.com/0"
    assert remapped.documents[11].url == "https://example.com/1"
    hello_postings = remapped.postings_lists["hello"]
    assert [posting.doc_id for posting in hello_postings.postings] == [10, 11]


def test_remap_with_zero_offset_returns_the_same_index_unchanged(tmp_path: Path) -> None:
    materialized = tmp_path / "materialized.jsonl"
    materialized.write_text(
        _document_line("https://example.com/0", "Zero", "hello world") + "\n", encoding="utf-8"
    )

    local_index = build_index_from_materialized_partition(materialized, doc_id_offset=0)

    assert set(local_index.documents) == {0}


def test_remap_rejects_negative_offset() -> None:
    from inverted_index_builder.models import IndexStats, InvertedIndex

    empty_index = InvertedIndex(documents={}, postings_lists={}, stats=IndexStats(0, 0, 0, 0.0))

    with pytest.raises(IndexingError):
        remap_index_to_global_doc_ids(empty_index, -1)
