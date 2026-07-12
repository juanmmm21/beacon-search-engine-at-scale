"""Tests de `write_partition_manifest`/`read_manifest` contra `LocalFilesystemObjectStorage`,
sin dobles: la implementación local de desarrollo se testea directamente (ver `CLAUDE.md`,
sección de testing). El caso central (`test_manifest_aggregates_fragments_from_several_partitions`)
es la propiedad que motiva el diseño: varios `ExtractWorker` escriben cada uno su propio
fragmento sin pisar el de otro, y `read_manifest` los agrega en un único manifiesto."""

from __future__ import annotations

from pathlib import Path

from beacon_scale_infra.extract.manifest import (
    PartitionManifestEntry,
    read_manifest,
    write_partition_manifest,
)
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_PREFIX = "extracted-documents"


async def test_manifest_round_trips_a_single_partition(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    entry = PartitionManifestEntry(
        partition_key="worker-a", document_count=12, discarded_count=3, part_file_count=2
    )

    await write_partition_manifest(storage, _BUCKET, _PREFIX, entry)
    manifest = await read_manifest(storage, _BUCKET, _PREFIX)

    assert manifest.partitions == (entry,)
    assert manifest.total_documents == 12
    assert manifest.total_discarded == 3


async def test_manifest_aggregates_fragments_from_several_partitions(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    entry_a = PartitionManifestEntry(
        partition_key="worker-a", document_count=10, discarded_count=1, part_file_count=1
    )
    entry_b = PartitionManifestEntry(
        partition_key="worker-b", document_count=7, discarded_count=0, part_file_count=1
    )

    await write_partition_manifest(storage, _BUCKET, _PREFIX, entry_a)
    await write_partition_manifest(storage, _BUCKET, _PREFIX, entry_b)
    manifest = await read_manifest(storage, _BUCKET, _PREFIX)

    assert {entry.partition_key for entry in manifest.partitions} == {"worker-a", "worker-b"}
    assert manifest.total_documents == 17
    assert manifest.total_discarded == 1


async def test_rewriting_a_partition_fragment_overwrites_not_duplicates(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)
    first = PartitionManifestEntry(
        partition_key="worker-a", document_count=5, discarded_count=0, part_file_count=1
    )
    updated = PartitionManifestEntry(
        partition_key="worker-a", document_count=9, discarded_count=1, part_file_count=2
    )

    await write_partition_manifest(storage, _BUCKET, _PREFIX, first)
    await write_partition_manifest(storage, _BUCKET, _PREFIX, updated)
    manifest = await read_manifest(storage, _BUCKET, _PREFIX)

    assert manifest.partitions == (updated,)


async def test_read_manifest_on_empty_prefix_returns_no_partitions(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path)

    manifest = await read_manifest(storage, _BUCKET, _PREFIX)

    assert manifest.partitions == ()
    assert manifest.total_documents == 0
