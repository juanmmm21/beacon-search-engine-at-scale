"""Descarga de los artefactos inmutables que cada réplica de la API
reconstruye idénticamente al arrancar (ver `ARCHITECTURE.md`, fase 6, sobre
la división estado compartido / estado por réplica): el índice global sin
comprimir de fase 3 (vocabulario para spellcheck/autocomplete, features del
reranker, stats), los scores de PageRank de fase 4, el modelo LTR subido por
`train-reranker`, el catálogo de corpus y los dos marcadores de versión.

Todos son artefactos de una build concreta del índice, atados entre sí por
`index_version` (ver `index/index_version.py`): descargar un catálogo de una
build y un índice de otra produciría snippets de documentos equivocados, así
que la coherencia se verifica aquí, al arrancar, con error explícito -- nunca
se sirve sobre artefactos mezclados.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from beacon_scale_infra.console.config import ConsoleAppConfig
from beacon_scale_infra.errors import ConsoleServingError, ObjectNotFoundError
from beacon_scale_infra.index.corpus_catalog import CorpusCatalog
from beacon_scale_infra.index.index_version import (
    INDEX_VERSION_MARKER_BASENAME,
    parse_index_version_marker,
)
from beacon_scale_infra.protocols import ObjectStorage

logger = logging.getLogger(__name__)

_CLUSTER_MANIFEST_BASENAME = "cluster_manifest.json"


@dataclass(frozen=True, slots=True)
class ConsoleArtifacts:
    """Rutas locales (ya descargadas) y metadatos de la build del índice
    sobre la que esta réplica va a servir."""

    index_dir: Path
    pagerank_dir: Path
    ltr_model_dir: Path
    index_version: str
    catalog: CorpusCatalog
    num_shards: int


async def download_console_artifacts(
    storage: ObjectStorage, config: ConsoleAppConfig, *, destination_root: Path
) -> ConsoleArtifacts:
    bucket = config.bucket
    index_dir = destination_root / "index"
    pagerank_dir = destination_root / "pagerank"
    ltr_model_dir = destination_root / "ltr-model"
    for directory in (index_dir, pagerank_dir, ltr_model_dir):
        directory.mkdir(parents=True, exist_ok=True)

    index_version = await _read_required_version_marker(
        storage,
        bucket,
        f"{config.index_prefix}/{INDEX_VERSION_MARKER_BASENAME}",
        remedy="ejecuta 'build-index' (fase 3), que ya publica el marcador de versión",
    )

    await _download_flat_prefix(
        storage,
        bucket,
        config.index_prefix,
        index_dir,
        what="el índice global de fase 3 ('build-index')",
    )
    await _download_flat_prefix(
        storage,
        bucket,
        config.pagerank_prefix,
        pagerank_dir,
        what="los scores de PageRank de fase 4 ('compute-pagerank')",
    )
    await _download_flat_prefix(
        storage,
        bucket,
        config.ltr_model_prefix,
        ltr_model_dir,
        what="el modelo de reranking ('train-reranker')",
    )

    catalog = await _read_corpus_catalog(storage, bucket, config.corpus_catalog_object_key)
    if catalog.index_version != index_version:
        raise ConsoleServingError(
            f"el catálogo de corpus ({config.corpus_catalog_object_key!r}) pertenece a la "
            f"versión {catalog.index_version!r} del índice pero el marcador de "
            f"{config.index_prefix!r} declara {index_version!r}: el bucket mezcla artefactos "
            "de builds distintas -- re-ejecuta 'build-index' completo"
        )

    num_shards = await _read_num_shards(storage, bucket, config.shard_index_prefix)
    await _warn_if_shard_marker_diverges(storage, bucket, config.shard_index_prefix, index_version)

    return ConsoleArtifacts(
        index_dir=index_dir,
        pagerank_dir=pagerank_dir,
        ltr_model_dir=ltr_model_dir,
        index_version=index_version,
        catalog=catalog,
        num_shards=num_shards,
    )


async def _download_flat_prefix(
    storage: ObjectStorage, bucket: str, prefix: str, local_dir: Path, *, what: str
) -> int:
    """Descarga los objetos directos bajo `prefix/` (los formatos de índice,
    PageRank y modelo son directorios planos de ficheros hermanos, mismo
    razonamiento que `ShardIndexPipeline._download_directory`), saltando el
    marcador de versión, que se lee aparte. Un prefijo vacío es error
    explícito: significa que el job por lotes correspondiente nunca corrió."""
    count = 0
    async for entry in storage.list_objects(bucket, prefix=f"{prefix}/"):
        relative_name = entry.key[len(prefix) + 1 :]
        if not relative_name or "/" in relative_name:
            continue
        if relative_name == INDEX_VERSION_MARKER_BASENAME:
            continue
        data = await storage.get_object(bucket, entry.key)
        (local_dir / relative_name).write_bytes(data)
        count += 1
    if count == 0:
        raise ConsoleServingError(
            f"no hay ningún objeto bajo {prefix!r} en el bucket {bucket!r}: "
            f"falta {what} antes de arrancar la consola"
        )
    return count


async def _read_required_version_marker(
    storage: ObjectStorage, bucket: str, marker_key: str, *, remedy: str
) -> str:
    try:
        raw = await storage.get_object(bucket, marker_key)
    except ObjectNotFoundError as exc:
        raise ConsoleServingError(
            f"no existe {marker_key!r} en el bucket {bucket!r}: {remedy}"
        ) from exc
    try:
        return parse_index_version_marker(raw)
    except ValueError as exc:
        raise ConsoleServingError(f"marcador {marker_key!r} ilegible: {exc}") from exc


async def _read_corpus_catalog(
    storage: ObjectStorage, bucket: str, catalog_key: str
) -> CorpusCatalog:
    try:
        raw = await storage.get_object(bucket, catalog_key)
    except ObjectNotFoundError as exc:
        raise ConsoleServingError(
            f"no existe {catalog_key!r} en el bucket {bucket!r}: ejecuta 'build-index' (fase 3), "
            "que ya publica el catálogo de corpus"
        ) from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConsoleServingError(f"catálogo de corpus {catalog_key!r} ilegible: {exc}") from exc
    try:
        return CorpusCatalog.from_json_dict(data)
    except (KeyError, TypeError, ValueError) as exc:
        raise ConsoleServingError(
            f"catálogo de corpus {catalog_key!r} con formato inesperado: {exc}"
        ) from exc


async def _read_num_shards(storage: ObjectStorage, bucket: str, shard_index_prefix: str) -> int:
    manifest_key = f"{shard_index_prefix}/{_CLUSTER_MANIFEST_BASENAME}"
    try:
        raw = await storage.get_object(bucket, manifest_key)
    except ObjectNotFoundError as exc:
        raise ConsoleServingError(
            f"no existe {manifest_key!r} en el bucket {bucket!r}: ejecuta 'shard-index' (fase 5) "
            "antes de arrancar la consola"
        ) from exc
    try:
        manifest = json.loads(raw)
        num_shards = int(manifest["num_shards"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ConsoleServingError(
            f"manifiesto de clúster {manifest_key!r} ilegible: {exc}"
        ) from exc
    if num_shards <= 0:
        raise ConsoleServingError(
            f"manifiesto de clúster {manifest_key!r} declara num_shards={num_shards}"
        )
    return num_shards


async def _warn_if_shard_marker_diverges(
    storage: ObjectStorage, bucket: str, shard_index_prefix: str, index_version: str
) -> None:
    """Aviso temprano al operador si `shard-index` aún no se re-ejecutó tras
    la última build del índice. Solo un aviso: la verificación vinculante es
    por consulta, contra la versión que cada réplica de shard anuncia en su
    metadata (ver `console/cluster_search.py`), porque lo que hay en el
    bucket ahora no prueba qué build tienen cargada los shards vivos."""
    marker_key = f"{shard_index_prefix}/{INDEX_VERSION_MARKER_BASENAME}"
    try:
        raw = await storage.get_object(bucket, marker_key)
        shard_version = parse_index_version_marker(raw)
    except (ObjectNotFoundError, ValueError):
        logger.warning(
            "sin marcador de versión legible en %r: no se puede verificar en el arranque si "
            "'shard-index' corresponde a la build cargada (la verificación por consulta sigue "
            "activa)",
            marker_key,
        )
        return
    if shard_version != index_version:
        logger.warning(
            "el prefijo %r se particionó desde la versión %s pero esta réplica cargó la %s: "
            "re-ejecuta 'shard-index' y reinicia las réplicas de shard",
            marker_key,
            shard_version,
            index_version,
        )
