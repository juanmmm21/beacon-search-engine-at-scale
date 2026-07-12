"""Implementación real de `ObjectStorage` contra un endpoint S3-compatible:
MinIO en desarrollo (ver `docker-compose.yml`), S3 real en producción -- el
mismo cliente `boto3` sirve para ambos, solo cambia `endpoint_url` (ver
`ARCHITECTURE.md`, sección "Almacenamiento de objetos").

Se usa el cliente síncrono de `boto3` (no existe una versión async con
cobertura y mantenimiento equivalentes mantenida por AWS) envuelto en
`asyncio.to_thread`, en vez de añadir `aioboto3` como dependencia adicional
solo para exponer una interfaz `async` -- decisión documentada en
`ARCHITECTURE.md`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

import botocore.exceptions
from boto3 import client as boto3_client

from beacon_scale_infra.errors import ObjectNotFoundError, ObjectStorageError
from beacon_scale_infra.models import ObjectMetadata

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.paginator import ListObjectsV2Paginator

_NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class S3ObjectStorage:
    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region_name: str = "us-east-1",
    ) -> None:
        self._client: S3Client = boto3_client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
        )

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> ObjectMetadata:
        return await asyncio.to_thread(self._put_object_sync, bucket, key, data, content_type)

    def _put_object_sync(
        self, bucket: str, key: str, data: bytes, content_type: str
    ) -> ObjectMetadata:
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        except botocore.exceptions.ClientError as exc:
            raise ObjectStorageError(f"fallo al escribir {bucket}/{key}: {exc}") from exc
        return ObjectMetadata(key=key, size_bytes=len(data), content_type=content_type)

    async def get_object(self, bucket: str, key: str) -> bytes:
        return await asyncio.to_thread(self._get_object_sync, bucket, key)

    def _get_object_sync(self, bucket: str, key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as exc:
            raise self._translate_client_error(exc, bucket, key, "leer") from exc
        return response["Body"].read()

    async def delete_object(self, bucket: str, key: str) -> None:
        await asyncio.to_thread(self._delete_object_sync, bucket, key)

    def _delete_object_sync(self, bucket: str, key: str) -> None:
        try:
            self._client.delete_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as exc:
            raise ObjectStorageError(f"fallo al borrar {bucket}/{key}: {exc}") from exc

    async def object_exists(self, bucket: str, key: str) -> bool:
        return await asyncio.to_thread(self._object_exists_sync, bucket, key)

    def _object_exists_sync(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as exc:
            if self._error_code(exc) in _NOT_FOUND_CODES:
                return False
            raise ObjectStorageError(f"fallo al comprobar {bucket}/{key}: {exc}") from exc
        return True

    async def list_objects(self, bucket: str, prefix: str = "") -> AsyncIterator[ObjectMetadata]:
        paginator: ListObjectsV2Paginator = self._client.get_paginator("list_objects_v2")
        page_iterator = iter(paginator.paginate(Bucket=bucket, Prefix=prefix))
        while True:
            page = await asyncio.to_thread(self._next_page, page_iterator, bucket)
            if page is None:
                return
            for entry in page.get("Contents", []):
                yield ObjectMetadata(
                    key=entry["Key"],
                    size_bytes=entry["Size"],
                    content_type="application/octet-stream",
                    last_modified_epoch_seconds=entry["LastModified"].timestamp(),
                )

    def _next_page(self, page_iterator: Any, bucket: str) -> Any:
        try:
            return next(page_iterator)
        except StopIteration:
            return None
        except botocore.exceptions.ClientError as exc:
            raise ObjectStorageError(f"fallo al listar {bucket}: {exc}") from exc

    @staticmethod
    def _error_code(exc: botocore.exceptions.ClientError) -> str | None:
        error = exc.response.get("Error")
        return error.get("Code") if error else None

    def _translate_client_error(
        self, exc: botocore.exceptions.ClientError, bucket: str, key: str, verb: str
    ) -> ObjectStorageError:
        if self._error_code(exc) in _NOT_FOUND_CODES:
            return ObjectNotFoundError(f"objeto no encontrado: {bucket}/{key}")
        return ObjectStorageError(f"fallo al {verb} {bucket}/{key}: {exc}")

    async def aclose(self) -> None:
        await asyncio.to_thread(self._client.close)
