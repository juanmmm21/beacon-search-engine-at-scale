"""Tests deterministas de `object_key_for_document_part`/`object_key_for_discarded_part`."""

from __future__ import annotations

from beacon_scale_infra.extract.partitioning import (
    object_key_for_discarded_part,
    object_key_for_document_part,
)


def test_document_part_key_has_partition_and_zero_padded_sequence() -> None:
    key = object_key_for_document_part("worker-a", 3, prefix="extracted-documents")
    assert key == "extracted-documents/partition=worker-a/documents-000003.jsonl"


def test_discarded_part_key_has_partition_and_zero_padded_sequence() -> None:
    key = object_key_for_discarded_part("worker-a", 3, prefix="extracted-documents")
    assert key == "extracted-documents/partition=worker-a/discarded-000003.jsonl"


def test_different_workers_never_share_a_partition_prefix() -> None:
    key_a = object_key_for_document_part("worker-a", 0)
    key_b = object_key_for_document_part("worker-b", 0)
    assert key_a != key_b
    assert "partition=worker-a" in key_a
    assert "partition=worker-b" in key_b


def test_successive_flushes_never_reuse_the_same_part_key() -> None:
    keys = {object_key_for_document_part("worker-a", seq) for seq in range(5)}
    assert len(keys) == 5
