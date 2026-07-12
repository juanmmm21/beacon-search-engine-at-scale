"""Tests de contrato de `S3ObjectStorage` contra `moto` (mock fiel de la API
de S3): verifican que este cliente llama a la API S3-compatible
correctamente y traduce sus respuestas/errores al contrato de `ObjectStorage`
-- no requieren un MinIO real en marcha para correr en CI."""

from __future__ import annotations

from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws

from beacon_scale_infra.errors import ObjectNotFoundError
from beacon_scale_infra.storage.s3 import S3ObjectStorage

_BUCKET = "beacon-scale-test-bucket"


@pytest.fixture
def moto_bucket() -> Iterator[None]:
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        client.create_bucket(Bucket=_BUCKET)
        yield


@pytest.fixture
def storage(moto_bucket: None) -> S3ObjectStorage:
    return S3ObjectStorage(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
    )


async def test_put_then_get_round_trips_bytes(storage: S3ObjectStorage) -> None:
    await storage.put_object(_BUCKET, "docs/a.txt", b"hello", content_type="text/plain")
    assert await storage.get_object(_BUCKET, "docs/a.txt") == b"hello"


async def test_get_missing_key_raises_object_not_found(storage: S3ObjectStorage) -> None:
    with pytest.raises(ObjectNotFoundError):
        await storage.get_object(_BUCKET, "does/not/exist.txt")


async def test_object_exists_reflects_put_and_delete(storage: S3ObjectStorage) -> None:
    assert not await storage.object_exists(_BUCKET, "k.txt")
    await storage.put_object(_BUCKET, "k.txt", b"x")
    assert await storage.object_exists(_BUCKET, "k.txt")
    await storage.delete_object(_BUCKET, "k.txt")
    assert not await storage.object_exists(_BUCKET, "k.txt")


async def test_list_objects_filters_by_prefix(storage: S3ObjectStorage) -> None:
    await storage.put_object(_BUCKET, "docs/a.txt", b"a")
    await storage.put_object(_BUCKET, "docs/b.txt", b"bb")
    await storage.put_object(_BUCKET, "other/c.txt", b"ccc")

    keys = sorted([entry.key async for entry in storage.list_objects(_BUCKET, prefix="docs/")])
    assert keys == ["docs/a.txt", "docs/b.txt"]
