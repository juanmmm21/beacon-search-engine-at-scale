"""Orquestador de la indexación distribuida (fase 3): lee el manifiesto de
fase 2, calcula los rangos globales de `doc_id` (`doc_id_ranges.py`),
construye y remapea el índice local de cada partición (`partition_indexer.py`,
paso *map*), los fusiona en un único índice global (`merge.py`, paso
*reduce*), lo serializa con `inverted_index_builder.serialization.write_index`
sin modificar, y opcionalmente lo comprime con `index-compression-codec`
también sin modificar (ver `ARCHITECTURE.md`, fase 3).

Job por lotes, no worker de larga duración: no consume ningún `MessageQueue`
-- se ejecuta una vez, después de que la fase 2 haya terminado (ver
`ARCHITECTURE.md`, fase 3, sección 0, sobre por qué esta fase no puede
solaparse con una fase 2 todavía en marcha).
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

from index_compression_codec.pipeline import CompressionPipeline
from inverted_index_builder.models import InvertedIndex
from inverted_index_builder.serialization import write_index

from beacon_scale_infra.errors import IndexingError
from beacon_scale_infra.extract.manifest import read_manifest
from beacon_scale_infra.index.corpus_catalog import (
    CorpusCatalog,
    CorpusPartEntry,
    materialize_partition_with_parts,
)
from beacon_scale_infra.index.doc_id_ranges import compute_doc_id_ranges
from beacon_scale_infra.index.index_version import (
    INDEX_VERSION_MARKER_BASENAME,
    compute_index_version,
    index_version_marker_body,
)
from beacon_scale_infra.index.merge import merge_partition_indexes
from beacon_scale_infra.index.models import IndexingPipelineConfig, IndexingRunStats
from beacon_scale_infra.index.partition_indexer import build_index_from_materialized_partition
from beacon_scale_infra.protocols import ObjectStorage

_CONTENT_TYPES_BY_SUFFIX = {
    ".json": "application/json",
    ".jsonl": "application/jsonl",
}


class IndexingPipeline:
    """Ejecuta una pasada completa de indexación distribuida sobre el corpus
    particionado que la fase 2 dejó en `config.bucket`/`config.extract_prefix`.
    """

    def __init__(self, config: IndexingPipelineConfig, *, storage: ObjectStorage) -> None:
        self._config = config
        self._storage = storage

    async def run(self) -> IndexingRunStats:
        manifest = await read_manifest(
            self._storage, self._config.bucket, self._config.extract_prefix
        )
        doc_id_ranges = compute_doc_id_ranges(manifest)

        with tempfile.TemporaryDirectory(prefix="beacon-scale-index-") as raw_work_dir:
            work_dir = Path(raw_work_dir)
            corpus_path = work_dir / "corpus-documents.jsonl"
            corpus_path.touch()

            partial_indexes: list[InvertedIndex] = []
            catalog_parts: list[CorpusPartEntry] = []
            last_crawled_at: str | None = None
            for doc_range in doc_id_ranges.ranges:
                partition_path = work_dir / f"partition-{doc_range.partition_key}.jsonl"
                materialization = await materialize_partition_with_parts(
                    self._storage,
                    self._config.bucket,
                    self._config.extract_prefix,
                    doc_range.partition_key,
                    partition_path,
                    start_doc_id=doc_range.start_doc_id,
                )
                # El manifiesto de fase 2 tiene que describir exactamente lo
                # que hay en las particiones: un desajuste significa que un
                # ExtractWorker seguía escribiendo después de leerlo, y todo
                # rango de doc_id calculado a partir de él sería inválido
                # (ver ARCHITECTURE.md, fase 3, sección 0) -- error explícito,
                # nunca un índice global silenciosamente desalineado.
                if materialization.document_count != doc_range.document_count:
                    raise IndexingError(
                        f"la partición {doc_range.partition_key!r} contiene "
                        f"{materialization.document_count} documentos pero su manifiesto "
                        f"declara {doc_range.document_count}: ¿seguía escribiendo un "
                        "extract-worker al lanzar build-index?"
                    )
                catalog_parts.extend(
                    part for part in materialization.parts if part.document_count > 0
                )
                if materialization.last_fetched_at is not None and (
                    last_crawled_at is None or materialization.last_fetched_at > last_crawled_at
                ):
                    last_crawled_at = materialization.last_fetched_at
                # Se concatena el mismo fichero materializado, en el mismo
                # orden ascendente de partition_key usado para asignar
                # rangos, en el corpus global -- así la posición de línea en
                # `corpus_path` coincide exactamente con el doc_id global
                # (ver ARCHITECTURE.md, fase 3, sección 5).
                with (
                    partition_path.open("rb") as partition_file,
                    corpus_path.open("ab") as corpus_file,
                ):
                    shutil.copyfileobj(partition_file, corpus_file)

                partial_indexes.append(
                    build_index_from_materialized_partition(partition_path, doc_range.start_doc_id)
                )

            merged_index = merge_partition_indexes(partial_indexes)

            index_dir = work_dir / "index"
            write_index(merged_index, index_dir)

            # Versión de contenido del índice publicado: los cuatro ficheros
            # fusionados más el corpus alineado por doc_id (ver
            # index_version.py) -- lo que namespacea la caché de resultados
            # de la consola y lo que anuncian las réplicas de shard.
            index_version = compute_index_version(
                [
                    *sorted(path for path in index_dir.iterdir() if path.is_file()),
                    corpus_path,
                ]
            )
            catalog = CorpusCatalog(
                index_version=index_version,
                total_documents=merged_index.stats.total_documents,
                last_crawled_at=last_crawled_at,
                parts=tuple(catalog_parts),
            )

            await self._upload_directory(index_dir, self._config.index_output_prefix)
            await self._upload_file(
                corpus_path, self._config.corpus_object_key, content_type="application/jsonl"
            )
            await self._storage.put_object(
                self._config.bucket,
                self._config.corpus_catalog_object_key,
                json.dumps(catalog.to_json_dict(), ensure_ascii=False).encode("utf-8"),
                content_type="application/json",
            )
            await self._storage.put_object(
                self._config.bucket,
                f"{self._config.index_output_prefix}/{INDEX_VERSION_MARKER_BASENAME}",
                index_version_marker_body(index_version),
                content_type="application/json",
            )

            compression_ratio: float | None = None
            if self._config.compress:
                compressed_dir = work_dir / "index-compressed"
                compression_stats = CompressionPipeline().compress(index_dir, compressed_dir)
                await self._upload_directory(compressed_dir, self._config.compressed_output_prefix)
                # El índice comprimido es la misma build lógica que el sin
                # comprimir (mismo corpus, mismos doc_id): comparte marcador,
                # para que 'shard-index' lo propague sea cual sea el prefijo
                # de origen elegido.
                await self._storage.put_object(
                    self._config.bucket,
                    f"{self._config.compressed_output_prefix}/{INDEX_VERSION_MARKER_BASENAME}",
                    index_version_marker_body(index_version),
                    content_type="application/json",
                )
                compression_ratio = compression_stats.compression_ratio

        return IndexingRunStats(
            partitions_indexed=len(doc_id_ranges.ranges),
            total_documents=merged_index.stats.total_documents,
            vocabulary_size=merged_index.stats.vocabulary_size,
            total_postings=merged_index.stats.total_postings,
            compression_ratio=compression_ratio,
            index_version=index_version,
        )

    async def _upload_directory(self, local_dir: Path, object_prefix: str) -> None:
        for path in sorted(local_dir.iterdir()):
            if not path.is_file():
                continue
            await self._upload_file(
                path,
                f"{object_prefix}/{path.name}",
                content_type=_CONTENT_TYPES_BY_SUFFIX.get(path.suffix, "application/octet-stream"),
            )

    async def _upload_file(self, local_path: Path, object_key: str, *, content_type: str) -> None:
        await self._storage.put_object(
            self._config.bucket, object_key, local_path.read_bytes(), content_type=content_type
        )
