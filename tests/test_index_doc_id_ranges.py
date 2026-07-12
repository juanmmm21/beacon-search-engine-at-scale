"""Tests de `compute_doc_id_ranges`/`DocIdRangeAssignment.partition_for`: la
propiedad central de esta fase (ver `ARCHITECTURE.md`, fase 3, sección 1) es
que los rangos son contiguos, disjuntos, ordenados por `partition_key` y
resolubles con una búsqueda barata -- estos tests verifican exactamente eso,
no el resto del pipeline."""

from __future__ import annotations

import pytest

from beacon_scale_infra.errors import IndexingError
from beacon_scale_infra.extract.manifest import ExtractManifest, PartitionManifestEntry
from beacon_scale_infra.index.doc_id_ranges import compute_doc_id_ranges


def _manifest(*entries: tuple[str, int]) -> ExtractManifest:
    return ExtractManifest(
        partitions=tuple(
            PartitionManifestEntry(
                partition_key=partition_key,
                document_count=document_count,
                discarded_count=0,
                part_file_count=1,
            )
            for partition_key, document_count in entries
        )
    )


def test_ranges_are_contiguous_and_ordered_by_partition_key_ascending() -> None:
    # Deliberadamente fuera de orden en el manifiesto: worker-c antes que worker-a.
    manifest = _manifest(("worker-c", 5), ("worker-a", 10), ("worker-b", 7))

    assignment = compute_doc_id_ranges(manifest)

    assert [r.partition_key for r in assignment.ranges] == ["worker-a", "worker-b", "worker-c"]
    assert [(r.start_doc_id, r.end_doc_id) for r in assignment.ranges] == [
        (0, 10),
        (10, 17),
        (17, 22),
    ]
    assert assignment.total_documents == 22


def test_partitions_with_zero_documents_are_skipped_without_breaking_offsets() -> None:
    manifest = _manifest(("worker-a", 3), ("worker-b", 0), ("worker-c", 4))

    assignment = compute_doc_id_ranges(manifest)

    assert [r.partition_key for r in assignment.ranges] == ["worker-a", "worker-c"]
    assert assignment.range_for_partition("worker-a").start_doc_id == 0
    assert assignment.range_for_partition("worker-c").start_doc_id == 3
    assert assignment.total_documents == 7


def test_empty_manifest_produces_no_ranges() -> None:
    assignment = compute_doc_id_ranges(_manifest())

    assert assignment.ranges == ()
    assert assignment.total_documents == 0


def test_partition_for_resolves_doc_id_to_owning_partition_at_range_boundaries() -> None:
    manifest = _manifest(("worker-a", 3), ("worker-b", 2))
    assignment = compute_doc_id_ranges(manifest)

    assert assignment.partition_for(0) == "worker-a"
    assert assignment.partition_for(2) == "worker-a"
    assert assignment.partition_for(3) == "worker-b"  # primer doc_id del siguiente rango
    assert assignment.partition_for(4) == "worker-b"


def test_partition_for_raises_for_doc_id_outside_any_range() -> None:
    manifest = _manifest(("worker-a", 3))
    assignment = compute_doc_id_ranges(manifest)

    with pytest.raises(IndexingError):
        assignment.partition_for(3)
    with pytest.raises(IndexingError):
        assignment.partition_for(-1)


def test_range_for_partition_raises_for_unknown_partition() -> None:
    assignment = compute_doc_id_ranges(_manifest(("worker-a", 3)))

    with pytest.raises(IndexingError):
        assignment.range_for_partition("worker-does-not-exist")
