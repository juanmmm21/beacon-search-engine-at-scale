"""Tests del paso *reduce*: `merge_partition_indexes` fusiona índices
parciales ya remapeados por concatenación (no merge-sort), y detecta
violaciones de la invariante de rangos disjuntos y ascendentes en vez de
producir un índice global corrupto en silencio (ver `ARCHITECTURE.md`, fase
3, sección 2)."""

from __future__ import annotations

import pytest
from inverted_index_builder.models import (
    DocumentRecord,
    IndexStats,
    InvertedIndex,
    Posting,
    PostingsList,
)

from beacon_scale_infra.errors import IndexingError
from beacon_scale_infra.index.merge import merge_partition_indexes

_EMPTY_STATS = IndexStats(
    total_documents=0, vocabulary_size=0, total_postings=0, average_document_length=0.0
)


def _partial_index(
    documents: dict[int, DocumentRecord], postings_lists: dict[str, PostingsList]
) -> InvertedIndex:
    return InvertedIndex(documents=documents, postings_lists=postings_lists, stats=_EMPTY_STATS)


def test_merge_concatenates_postings_in_partition_order_without_resorting() -> None:
    partition_a = _partial_index(
        documents={0: DocumentRecord(0, "https://example.com/0", "Zero", 2)},
        postings_lists={
            "search": PostingsList(
                term="search", document_frequency=1, postings=(Posting(0, 1, (0,)),)
            )
        },
    )
    partition_b = _partial_index(
        documents={1: DocumentRecord(1, "https://example.com/1", "One", 2)},
        postings_lists={
            "search": PostingsList(
                term="search", document_frequency=1, postings=(Posting(1, 2, (0, 3)),)
            )
        },
    )

    merged = merge_partition_indexes([partition_a, partition_b])

    assert set(merged.documents) == {0, 1}
    merged_postings = merged.postings_lists["search"]
    assert [posting.doc_id for posting in merged_postings.postings] == [0, 1]
    assert merged_postings.document_frequency == 2
    assert merged.stats.total_documents == 2
    assert merged.stats.vocabulary_size == 1
    assert merged.stats.total_postings == 2
    assert merged.stats.average_document_length == 2.0


def test_merge_of_a_single_partition_is_that_partition_unchanged() -> None:
    only_partition = _partial_index(
        documents={0: DocumentRecord(0, "https://example.com/0", "Zero", 3)},
        postings_lists={
            "hello": PostingsList(
                term="hello", document_frequency=1, postings=(Posting(0, 1, (0,)),)
            )
        },
    )

    merged = merge_partition_indexes([only_partition])

    assert merged.documents == only_partition.documents
    assert (
        merged.postings_lists["hello"].postings == only_partition.postings_lists["hello"].postings
    )


def test_merge_of_no_partitions_produces_an_empty_index() -> None:
    merged = merge_partition_indexes([])

    assert merged.documents == {}
    assert merged.postings_lists == {}
    assert merged.stats.total_documents == 0
    assert merged.stats.average_document_length == 0.0


def test_merge_rejects_duplicate_doc_id_across_partitions() -> None:
    partition_a = _partial_index(
        documents={5: DocumentRecord(5, "https://example.com/a", "A", 1)}, postings_lists={}
    )
    partition_b = _partial_index(
        documents={5: DocumentRecord(5, "https://example.com/b", "B", 1)}, postings_lists={}
    )

    with pytest.raises(IndexingError):
        merge_partition_indexes([partition_a, partition_b])


def test_merge_rejects_partitions_out_of_ascending_doc_id_order() -> None:
    partition_high = _partial_index(
        documents={10: DocumentRecord(10, "https://example.com/high", "High", 1)}, postings_lists={}
    )
    partition_low = _partial_index(
        documents={0: DocumentRecord(0, "https://example.com/low", "Low", 1)}, postings_lists={}
    )

    with pytest.raises(IndexingError):
        merge_partition_indexes([partition_high, partition_low])
