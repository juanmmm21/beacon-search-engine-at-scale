"""Orquestador del particionado de shards para query serving (fase 5): baja el
índice global que fase 3 dejó en `ObjectStorage` (comprimido por
`index-compression-codec`, o sin comprimir si se corrió `build-index
--no-compress`), lo particiona en `num_shards` directorios con
`distributed_index_sharding.partitioning.partition_index` **sin modificar**, y
sube cada directorio de shard de vuelta a `ObjectStorage` para que las
réplicas de `ShardReplicaService` puedan descargar la suya al arrancar (ver
`ARCHITECTURE.md`, fase 5).

Job por lotes único, mismo criterio que `IndexingPipeline`/
`DistributedPageRankPipeline` en fases 3 y 4: no consume ningún `MessageQueue`,
se ejecuta una vez, después de que `build-index` haya terminado.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Final

from distributed_index_sharding.partitioning import partition_index

from beacon_scale_infra.errors import ObjectNotFoundError, ShardIndexingError
from beacon_scale_infra.index.index_version import (
    INDEX_VERSION_MARKER_BASENAME,
    index_version_marker_body,
    parse_index_version_marker,
)
from beacon_scale_infra.protocols import ObjectStorage
from beacon_scale_infra.query.models import ShardIndexingStats, ShardIndexPipelineConfig

_CONTENT_TYPES_BY_SUFFIX: Final[dict[str, str]] = {
    ".json": "application/json",
    ".jsonl": "application/jsonl",
}


class ShardIndexPipeline:
    """Ejecuta una pasada completa de particionado de shards sobre el índice
    global que fase 3 dejó en `config.bucket`/`config.source_index_prefix`."""

    def __init__(self, config: ShardIndexPipelineConfig, *, storage: ObjectStorage) -> None:
        self._config = config
        self._storage = storage

    async def run(self) -> ShardIndexingStats:
        with tempfile.TemporaryDirectory(prefix="beacon-scale-shard-index-") as raw_work_dir:
            work_dir = Path(raw_work_dir)
            source_dir = work_dir / "source-index"
            source_dir.mkdir()

            index_version = await self._read_source_index_version()

            downloaded = await self._download_directory(
                self._config.source_index_prefix, source_dir
            )
            if downloaded == 0:
                raise ShardIndexingError(
                    f"no hay ningún objeto bajo {self._config.source_index_prefix!r} en el "
                    f"bucket {self._config.bucket!r}: ¿ha terminado 'build-index' de fase 3?"
                )

            output_root = work_dir / "shards"
            try:
                manifest = partition_index(source_dir, output_root, self._config.num_shards)
            except (FileNotFoundError, ValueError) as exc:
                raise ShardIndexingError(
                    f"fallo al particionar el índice descargado en {source_dir}: {exc}"
                ) from exc

            uploaded = await self._upload_directory(output_root, self._config.shard_index_prefix)
            # El marcador de versión de fase 3 se propaga al prefijo de
            # shards: es lo que cada réplica anuncia en su metadata de
            # registro y lo que namespacea la caché de resultados de la
            # consola (ver ARCHITECTURE.md, fases 3 y 6).
            await self._storage.put_object(
                self._config.bucket,
                f"{self._config.shard_index_prefix}/{INDEX_VERSION_MARKER_BASENAME}",
                index_version_marker_body(index_version),
                content_type="application/json",
            )

        return ShardIndexingStats(
            num_shards=manifest.num_shards,
            source_files_downloaded=downloaded,
            shard_files_uploaded=uploaded,
        )

    async def _read_source_index_version(self) -> str:
        """Versión de contenido que `build-index` dejó junto al índice de
        origen. Obligatoria: sin ella, las réplicas de shard no podrían
        anunciar qué build sirven y la consola no tendría contra qué validar
        ni namespacear su caché -- mejor fallar aquí, con el remedio claro
        (re-ejecutar `build-index`, que ya la escribe siempre), que servir
        shards de procedencia desconocida."""
        marker_key = f"{self._config.source_index_prefix}/{INDEX_VERSION_MARKER_BASENAME}"
        try:
            raw = await self._storage.get_object(self._config.bucket, marker_key)
        except ObjectNotFoundError as exc:
            raise ShardIndexingError(
                f"no existe {marker_key!r} en el bucket {self._config.bucket!r}: el índice de "
                "origen se construyó con una versión de 'build-index' anterior al marcador de "
                "versión -- re-ejecuta 'build-index' antes de 'shard-index'"
            ) from exc
        try:
            return parse_index_version_marker(raw)
        except ValueError as exc:
            raise ShardIndexingError(f"marcador {marker_key!r} ilegible: {exc}") from exc

    async def _download_directory(self, object_prefix: str, local_dir: Path) -> int:
        """Descarga los objetos directos bajo `object_prefix/` a `local_dir`.

        El índice de origen (comprimido o no) es siempre un directorio plano
        de ficheros -- ver `index-compression-codec`/`inverted-index-builder`,
        secciones *Data formats* -- así que cualquier clave con un nivel de
        prefijo adicional no pertenece a él y se ignora. El marcador de
        versión que `build-index` publica como hermano del índice tampoco
        forma parte del formato que `partition_index` espera: se lee aparte
        (`_read_source_index_version`), nunca se materializa en el directorio
        de origen.
        """
        count = 0
        async for entry in self._storage.list_objects(
            self._config.bucket, prefix=f"{object_prefix}/"
        ):
            relative_name = entry.key[len(object_prefix) + 1 :]
            if not relative_name or "/" in relative_name:
                continue
            if relative_name == INDEX_VERSION_MARKER_BASENAME:
                continue
            data = await self._storage.get_object(self._config.bucket, entry.key)
            (local_dir / relative_name).write_bytes(data)
            count += 1
        return count

    async def _upload_directory(self, local_dir: Path, object_prefix: str) -> int:
        count = 0
        for path in sorted(local_dir.rglob("*")):
            if not path.is_file():
                continue
            relative_key = path.relative_to(local_dir).as_posix()
            await self._storage.put_object(
                self._config.bucket,
                f"{object_prefix}/{relative_key}",
                path.read_bytes(),
                content_type=_CONTENT_TYPES_BY_SUFFIX.get(path.suffix, "application/octet-stream"),
            )
            count += 1
        return count
