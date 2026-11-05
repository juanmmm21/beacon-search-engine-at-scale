"""Publicación y restauración de un índice segmentado en `ObjectStorage`.

El índice segmentado (`inverted_index_builder.segments`) ya está diseñado
para esto: los segmentos y los ficheros de tombstones son inmutables y el
único fichero que cambia es `segments.json`. Publicar una instantánea es por
tanto:

1. subir los ficheros que el manifiesto nuevo referencia y aún no están en
   el almacenamiento (en la práctica: el segmento nuevo, el resultado de una
   fusión y los tombstones de esta generación);
2. subir `segments.json` — el "commit" visible: un lector que lo descarga
   encuentra ya en el almacenamiento todo lo que referencia;
3. subir `snapshot.json` (versión + hash del manifiesto al que corresponde)
   e `index_version.json` con esa versión, el mismo marcador que usan
   `build-index` y `shard-index` (`index/index_version.py`), para que la
   caché de la consola invalide por versión igual que hoy;
4. borrar lo que ya no referencia ni este manifiesto ni el anterior. El
   anterior se conserva una generación de gracia: un lector que acaba de
   descargar el manifiesto viejo todavía puede descargar sus segmentos.

Subir un objeto es atómico en S3/MinIO y en `LocalFilesystemObjectStorage`,
así que no hace falta ningún cerrojo distribuido entre escritor y lectores.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from inverted_index_builder.segments import (
    SEGMENTS_MANIFEST_FILENAME,
    SegmentedIndexError,
    SegmentsManifest,
)

from beacon_scale_infra.errors import IncrementalIndexingError, ObjectNotFoundError
from beacon_scale_infra.index.index_version import (
    INDEX_VERSION_MARKER_BASENAME,
    index_version_marker_body,
)
from beacon_scale_infra.protocols import ObjectStorage

_CONTENT_TYPES: Final[dict[str, str]] = {
    ".json": "application/json",
    ".jsonl": "application/jsonl",
}


def manifest_files(root: Path, manifest: SegmentsManifest) -> list[str]:
    """Rutas relativas (con `/`) de todos los ficheros que `manifest`
    referencia en `root`, ordenadas — sin incluir el propio manifiesto."""
    files: set[str] = set()
    for segment in manifest.segments:
        segment_dir = root / segment.name
        if not segment_dir.is_dir():
            raise IncrementalIndexingError(f"falta el segmento {segment_dir}")
        for path in segment_dir.rglob("*"):
            if path.is_file():
                files.add(path.relative_to(root).as_posix())
        if segment.deletes_file is not None:
            files.add(segment.deletes_file)
    return sorted(files)


SNAPSHOT_POINTER: Final[str] = "snapshot.json"


def snapshot_version(manifest_bytes: bytes, file_hashes: Mapping[str, str]) -> str:
    """Versión de contenido de una instantánea: el manifiesto más el hash de
    cada fichero que referencia. El manifiesto solo no basta: los nombres de
    segmento (`seg-000001`) y los recuentos coinciden entre dos índices
    construidos por separado con contenidos distintos."""
    digest = hashlib.sha256(manifest_bytes)
    for relative in sorted(file_hashes):
        digest.update(b"\x00" + relative.encode("utf-8") + b"\x00")
        digest.update(file_hashes[relative].encode("ascii"))
    return digest.hexdigest()


def manifest_digest(manifest_bytes: bytes) -> str:
    return hashlib.sha256(manifest_bytes).hexdigest()


def parse_snapshot_pointer(raw: bytes) -> tuple[str, str]:
    """`(index_version, sha256 del manifiesto)` de `snapshot.json`."""
    try:
        data = json.loads(raw)
        return str(data["index_version"]), str(data["manifest_sha256"])
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise IncrementalIndexingError(f"snapshot.json ilegible: {error}") from error


class SnapshotPublisher:
    """Sube instantáneas sucesivas de un índice segmentado local a
    `bucket`/`prefix`. Recuerda qué ficheros ya subió para no volver a
    subirlos; al crearse lista una vez lo que ya hay en el almacenamiento,
    de modo que un indexador reiniciado no repite subidas."""

    def __init__(self, storage: ObjectStorage, bucket: str, prefix: str) -> None:
        self._storage = storage
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._uploaded: set[str] | None = None
        self._previous_files: frozenset[str] = frozenset()
        # Los ficheros son inmutables: su hash se calcula una vez por proceso.
        self._hashes: dict[str, str] = {}

    def _key(self, relative: str) -> str:
        return f"{self._prefix}/{relative}"

    async def _remote_files(self) -> set[str]:
        if self._uploaded is None:
            uploaded: set[str] = set()
            async for item in self._storage.list_objects(self._bucket, f"{self._prefix}/"):
                uploaded.add(item.key[len(self._prefix) + 1 :])
            self._uploaded = uploaded
        return self._uploaded

    async def publish(self, root: Path) -> str:
        """Publica el estado actual de `root` y devuelve su versión."""
        manifest_path = root / SEGMENTS_MANIFEST_FILENAME
        try:
            manifest_bytes = manifest_path.read_bytes()
            manifest = SegmentsManifest.from_json_dict(json.loads(manifest_bytes))
        except (OSError, json.JSONDecodeError, SegmentedIndexError) as error:
            raise IncrementalIndexingError(f"manifiesto local ilegible: {error}") from error
        files = manifest_files(root, manifest)
        remote = await self._remote_files()

        for relative in files:
            if relative in remote:
                continue
            await self._storage.put_object(
                self._bucket,
                self._key(relative),
                (root / relative).read_bytes(),
                content_type=_CONTENT_TYPES.get(Path(relative).suffix, "application/octet-stream"),
            )
            remote.add(relative)

        for relative in files:
            if relative not in self._hashes:
                self._hashes[relative] = hashlib.sha256((root / relative).read_bytes()).hexdigest()
        version = snapshot_version(manifest_bytes, {f: self._hashes[f] for f in files})
        await self._storage.put_object(
            self._bucket,
            self._key(SEGMENTS_MANIFEST_FILENAME),
            manifest_bytes,
            content_type="application/json",
        )
        await self._storage.put_object(
            self._bucket,
            self._key(SNAPSHOT_POINTER),
            json.dumps(
                {"index_version": version, "manifest_sha256": manifest_digest(manifest_bytes)}
            ).encode("utf-8"),
            content_type="application/json",
        )
        await self._storage.put_object(
            self._bucket,
            self._key(INDEX_VERSION_MARKER_BASENAME),
            index_version_marker_body(version),
            content_type="application/json",
        )

        keep = set(files) | self._previous_files
        keep.update({SEGMENTS_MANIFEST_FILENAME, INDEX_VERSION_MARKER_BASENAME, SNAPSHOT_POINTER})
        for relative in sorted(remote - keep):
            await self._storage.delete_object(self._bucket, self._key(relative))
            remote.discard(relative)
        self._previous_files = frozenset(files)
        for relative in list(self._hashes):
            if relative not in keep:
                del self._hashes[relative]
        return version


async def restore_snapshot(
    storage: ObjectStorage,
    bucket: str,
    prefix: str,
    root: Path,
    *,
    expected_manifest_sha256: str | None = None,
) -> bool:
    """Descarga en `root` (que no debe existir o estar vacío) la última
    instantánea publicada. Devuelve `False` si no hay ninguna publicada.

    Descarga primero el manifiesto y después solo lo que referencia, así que
    un borrado de gracia posterior (paso 4 de la publicación) no le afecta
    mientras no pasen dos commits entre ambos pasos; si pasan, el objeto
    falta y se informa explícitamente en vez de dejar un índice a medias."""
    prefix = prefix.strip("/")
    try:
        manifest_bytes = await storage.get_object(bucket, f"{prefix}/{SEGMENTS_MANIFEST_FILENAME}")
    except ObjectNotFoundError:
        return False
    if (
        expected_manifest_sha256 is not None
        and manifest_digest(manifest_bytes) != expected_manifest_sha256
    ):
        raise IncrementalIndexingError(
            "el manifiesto publicado cambió entre leer snapshot.json y descargarlo: "
            "hay una publicación en curso, reintentar"
        )
    try:
        manifest = SegmentsManifest.from_json_dict(json.loads(manifest_bytes))
    except (json.JSONDecodeError, SegmentedIndexError) as error:
        raise IncrementalIndexingError(f"manifiesto publicado ilegible: {error}") from error
    if root.exists() and any(root.iterdir()):
        raise IncrementalIndexingError(f"{root} no está vacío: no se restaura encima")

    wanted: list[str] = []
    for segment in manifest.segments:
        async for item in storage.list_objects(bucket, f"{prefix}/{segment.name}/"):
            wanted.append(item.key[len(prefix) + 1 :])
        if segment.deletes_file is not None:
            wanted.append(segment.deletes_file)
    for relative in sorted(wanted):
        try:
            data = await storage.get_object(bucket, f"{prefix}/{relative}")
        except ObjectNotFoundError as error:
            raise IncrementalIndexingError(
                f"la instantánea referencia {relative} pero ya no está publicado: "
                "otra generación la reemplazó durante la descarga, reintentar"
            ) from error
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    root.mkdir(parents=True, exist_ok=True)
    (root / SEGMENTS_MANIFEST_FILENAME).write_bytes(manifest_bytes)
    return True
