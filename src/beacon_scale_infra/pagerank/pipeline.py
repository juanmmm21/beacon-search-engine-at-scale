"""Orquestador del pipeline de PageRank distribuido (fase 4): descarga
`search-index/documents.jsonl` de fase 3, materializa `link_graph.jsonl`
desde las páginas crudas de fase 1 (`link_graph_materializer.py`), y llama a
`pagerank_link_analysis.pipeline.PageRankPipeline` sin modificar para
resolver URLs, construir el grafo de adyacencia y ejecutar la iteración de
potencia -- ver `ARCHITECTURE.md`, fase 4, para el razonamiento completo de
por qué una sola máquina basta para el cómputo en sí.

Job por lotes, no worker de larga duración -- mismo criterio que
`IndexingPipeline` en fase 3 (`index/pipeline.py`): se ejecuta una única vez,
después de que fase 3 haya dejado `search-index/documents.jsonl` listo.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Final

from pagerank_link_analysis.document_resolver import JsonlDocumentIdResolver
from pagerank_link_analysis.pipeline import PageRankPipeline as CorpusPageRankPipeline
from pagerank_link_analysis.scores_io import write_pagerank_output

from beacon_scale_infra.errors import ObjectNotFoundError, ObjectStorageError, PageRankPhaseError
from beacon_scale_infra.pagerank.link_graph_materializer import materialize_link_graph
from beacon_scale_infra.pagerank.models import PageRankPipelineConfig, PageRankRunStats
from beacon_scale_infra.protocols import ObjectStorage

_CONTENT_TYPES_BY_SUFFIX: Final[dict[str, str]] = {
    ".json": "application/json",
    ".jsonl": "application/jsonl",
}


class DistributedPageRankPipeline:
    """Ejecuta una pasada completa del cálculo de PageRank sobre el corpus
    que fase 3 dejó en `config.bucket`/`config.documents_object_key`, usando
    las páginas crudas que fase 1 dejó en
    `config.bucket`/`config.crawl_pages_prefix` como grafo de enlaces.
    """

    def __init__(self, config: PageRankPipelineConfig, *, storage: ObjectStorage) -> None:
        self._config = config
        self._storage = storage

    async def run(self) -> PageRankRunStats:
        with tempfile.TemporaryDirectory(prefix="beacon-scale-pagerank-") as raw_work_dir:
            work_dir = Path(raw_work_dir)
            documents_path = work_dir / "documents.jsonl"
            link_graph_path = work_dir / "link_graph.jsonl"

            await self._download_documents(documents_path)
            materialization_stats = await materialize_link_graph(
                self._storage,
                self._config.bucket,
                self._config.crawl_pages_prefix,
                link_graph_path,
                max_concurrent_reads=self._config.max_concurrent_reads,
            )

            resolver = JsonlDocumentIdResolver(documents_path)
            corpus_pipeline = CorpusPageRankPipeline(resolver)
            result = corpus_pipeline.compute(link_graph_path, self._config.pagerank_params)

            output_dir = work_dir / "output"
            write_pagerank_output(output_dir, result.scores, result.convergence, result.graph_stats)
            await self._upload_directory(output_dir, self._config.output_prefix)

            return PageRankRunStats(
                pages_materialized=materialization_stats.pages_materialized,
                pages_missing=materialization_stats.pages_missing,
                pages_skipped_malformed=materialization_stats.pages_skipped_malformed,
                total_documents=result.graph_stats.total_documents,
                resolved_edges=result.graph_stats.resolved_edges,
                dangling_documents=result.graph_stats.dangling_documents,
                unresolved_source_entries=result.graph_stats.unresolved_source_entries,
                unresolved_target_links=result.graph_stats.unresolved_target_links,
                iterations_run=result.convergence.iterations_run,
                converged=result.convergence.converged,
                final_delta=result.convergence.final_delta,
                elapsed_seconds=result.convergence.elapsed_seconds,
            )

    async def _download_documents(self, destination: Path) -> None:
        try:
            raw = await self._storage.get_object(
                self._config.bucket, self._config.documents_object_key
            )
        except ObjectNotFoundError as exc:
            raise PageRankPhaseError(
                f"no existe {self._config.documents_object_key!r} en el bucket "
                f"{self._config.bucket!r}: ¿ha terminado ya build-index (fase 3)?"
            ) from exc
        except ObjectStorageError as exc:
            raise PageRankPhaseError(
                f"fallo al descargar {self._config.documents_object_key!r}: {exc}"
            ) from exc
        destination.write_bytes(raw)

    async def _upload_directory(self, local_dir: Path, object_prefix: str) -> None:
        for path in sorted(local_dir.iterdir()):
            if not path.is_file():
                continue
            await self._storage.put_object(
                self._config.bucket,
                f"{object_prefix}/{path.name}",
                path.read_bytes(),
                content_type=_CONTENT_TYPES_BY_SUFFIX.get(path.suffix, "application/octet-stream"),
            )
