"""Implementación de desarrollo de `ObjectStorage`: guarda cada objeto como
un fichero regular bajo `root_dir/<bucket>/<key>`, con un sidecar
`<key>.metadata.json` para `content_type`/timestamp -- el filesystem no
tiene un concepto nativo de metadatos por objeto como S3, así que se modela
explícitamente en vez de inventar una convención implícita (p. ej. una
extensión de fichero) que perdería información al hacer round-trip.

Pensada para buckets de tamaño de desarrollo: `list_objects` enumera todas
las claves de golpe (`Path.rglob`) en vez de paginar como hace el backend S3
real -- aceptable aquí porque nunca corre contra un bucket de millones de
objetos, solo contra fixtures de test y datos locales de un desarrollador.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from pathlib import Path

from beacon_scale_infra.errors import ObjectNotFoundError, ObjectStorageError
from beacon_scale_infra.models import ObjectMetadata

_METADATA_SUFFIX = ".metadata.json"


class LocalFilesystemObjectStorage:
    def __init__(self, root_dir: Path | str) -> None:
        self._root = Path(root_dir)

    @staticmethod
    def _validate_key(key: str) -> None:
        if not key or key.startswith("/") or ".." in key.split("/"):
            raise ObjectStorageError(f"clave de objeto inválida: {key!r}")

    def _object_path(self, bucket: str, key: str) -> Path:
        self._validate_key(key)
        return self._root / bucket / key

    def _metadata_path(self, bucket: str, key: str) -> Path:
        return self._root / bucket / f"{key}{_METADATA_SUFFIX}"

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
        object_path = self._object_path(bucket, key)
        last_modified = time.time()
        try:
            object_path.parent.mkdir(parents=True, exist_ok=True)
            object_path.write_bytes(data)
            self._metadata_path(bucket, key).write_text(
                json.dumps(
                    {"content_type": content_type, "last_modified_epoch_seconds": last_modified}
                ),
                encoding="utf-8",
            )
        except OSError as exc:
            raise ObjectStorageError(f"fallo al escribir {bucket}/{key}: {exc}") from exc
        return ObjectMetadata(
            key=key,
            size_bytes=len(data),
            content_type=content_type,
            last_modified_epoch_seconds=last_modified,
        )

    async def get_object(self, bucket: str, key: str) -> bytes:
        return await asyncio.to_thread(self._get_object_sync, bucket, key)

    def _get_object_sync(self, bucket: str, key: str) -> bytes:
        object_path = self._object_path(bucket, key)
        try:
            return object_path.read_bytes()
        except FileNotFoundError as exc:
            raise ObjectNotFoundError(f"objeto no encontrado: {bucket}/{key}") from exc
        except OSError as exc:
            raise ObjectStorageError(f"fallo al leer {bucket}/{key}: {exc}") from exc

    async def delete_object(self, bucket: str, key: str) -> None:
        await asyncio.to_thread(self._delete_object_sync, bucket, key)

    def _delete_object_sync(self, bucket: str, key: str) -> None:
        try:
            self._object_path(bucket, key).unlink(missing_ok=True)
            self._metadata_path(bucket, key).unlink(missing_ok=True)
        except OSError as exc:
            raise ObjectStorageError(f"fallo al borrar {bucket}/{key}: {exc}") from exc

    async def object_exists(self, bucket: str, key: str) -> bool:
        return await asyncio.to_thread(self._object_exists_sync, bucket, key)

    def _object_exists_sync(self, bucket: str, key: str) -> bool:
        return self._object_path(bucket, key).is_file()

    async def list_objects(self, bucket: str, prefix: str = "") -> AsyncIterator[ObjectMetadata]:
        for key in await asyncio.to_thread(self._list_keys_sync, bucket, prefix):
            yield await asyncio.to_thread(self._read_metadata_sync, bucket, key)

    def _list_keys_sync(self, bucket: str, prefix: str) -> list[str]:
        bucket_dir = self._root / bucket
        if not bucket_dir.is_dir():
            return []
        try:
            keys = [
                str(path.relative_to(bucket_dir))
                for path in sorted(bucket_dir.rglob("*"))
                if path.is_file() and not path.name.endswith(_METADATA_SUFFIX)
            ]
        except OSError as exc:
            raise ObjectStorageError(f"fallo al listar {bucket}: {exc}") from exc
        return [key for key in keys if key.startswith(prefix)]

    def _read_metadata_sync(self, bucket: str, key: str) -> ObjectMetadata:
        object_path = self._object_path(bucket, key)
        metadata_path = self._metadata_path(bucket, key)
        size_bytes = object_path.stat().st_size
        try:
            raw = json.loads(metadata_path.read_text(encoding="utf-8"))
            content_type = str(raw["content_type"])
            last_modified = float(raw["last_modified_epoch_seconds"])
        except (OSError, KeyError, ValueError, json.JSONDecodeError):
            content_type = "application/octet-stream"
            last_modified = object_path.stat().st_mtime
        return ObjectMetadata(
            key=key,
            size_bytes=size_bytes,
            content_type=content_type,
            last_modified_epoch_seconds=last_modified,
        )
