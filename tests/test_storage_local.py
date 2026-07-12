"""Tests de comportamiento de `LocalFilesystemObjectStorage`: sin mocks, sin
red -- corre directamente contra un directorio temporal."""

from __future__ import annotations

from pathlib import Path

import pytest

from beacon_scale_infra.errors import ObjectNotFoundError, ObjectStorageError
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage


@pytest.fixture
def storage(tmp_path: Path) -> LocalFilesystemObjectStorage:
    return LocalFilesystemObjectStorage(tmp_path)


async def test_put_then_get_round_trips_bytes_and_content_type(
    storage: LocalFilesystemObjectStorage,
) -> None:
    metadata = await storage.put_object("bucket", "docs/a.txt", b"hello", content_type="text/plain")
    assert metadata.key == "docs/a.txt"
    assert metadata.size_bytes == 5
    assert metadata.content_type == "text/plain"

    data = await storage.get_object("bucket", "docs/a.txt")
    assert data == b"hello"


async def test_get_missing_key_raises_object_not_found(
    storage: LocalFilesystemObjectStorage,
) -> None:
    with pytest.raises(ObjectNotFoundError):
        await storage.get_object("bucket", "does/not/exist.txt")


async def test_object_exists_reflects_put_and_delete(
    storage: LocalFilesystemObjectStorage,
) -> None:
    assert not await storage.object_exists("bucket", "k.txt")
    await storage.put_object("bucket", "k.txt", b"x")
    assert await storage.object_exists("bucket", "k.txt")
    await storage.delete_object("bucket", "k.txt")
    assert not await storage.object_exists("bucket", "k.txt")


async def test_delete_missing_key_is_idempotent(storage: LocalFilesystemObjectStorage) -> None:
    await storage.delete_object("bucket", "never-existed.txt")  # no debe lanzar


async def test_list_objects_filters_by_prefix_and_excludes_metadata_sidecars(
    storage: LocalFilesystemObjectStorage,
) -> None:
    await storage.put_object("bucket", "docs/a.txt", b"a")
    await storage.put_object("bucket", "docs/b.txt", b"bb")
    await storage.put_object("bucket", "other/c.txt", b"ccc")

    keys = sorted([entry.key async for entry in storage.list_objects("bucket", prefix="docs/")])
    assert keys == ["docs/a.txt", "docs/b.txt"]

    sizes = {entry.key: entry.size_bytes async for entry in storage.list_objects("bucket")}
    assert sizes == {"docs/a.txt": 1, "docs/b.txt": 2, "other/c.txt": 3}


async def test_list_objects_on_missing_bucket_yields_nothing(
    storage: LocalFilesystemObjectStorage,
) -> None:
    keys = [entry async for entry in storage.list_objects("no-such-bucket")]
    assert keys == []


@pytest.mark.parametrize("bad_key", ["", "/abs/path.txt", "../escape.txt", "a/../../escape.txt"])
async def test_rejects_invalid_keys(storage: LocalFilesystemObjectStorage, bad_key: str) -> None:
    with pytest.raises(ObjectStorageError):
        await storage.put_object("bucket", bad_key, b"x")
