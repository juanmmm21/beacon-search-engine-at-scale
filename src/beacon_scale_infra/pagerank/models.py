"""Tipos de datos del pipeline de PageRank distribuido (fase 4): configuración
de una ejecución y resumen final -- mismo criterio que `index/models.py` en
fase 3 (sin `worker_id` ni consumidor de cola: este pipeline es un job por
lotes único, no una réplica entre varias, ver `ARCHITECTURE.md`, fase 4,
sección 0)."""

from __future__ import annotations

from dataclasses import dataclass, field

from pagerank_link_analysis.models import PageRankParams


@dataclass(frozen=True, slots=True)
class PageRankPipelineConfig:
    """Configuración de una ejecución de `DistributedPageRankPipeline.run()`."""

    bucket: str = "beacon-scale-dev"
    crawl_pages_prefix: str = "crawl-pages"
    documents_object_key: str = "search-index/documents.jsonl"
    output_prefix: str = "pagerank-scores"
    max_concurrent_reads: int = 64
    pagerank_params: PageRankParams = field(default_factory=PageRankParams)

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket no puede estar vacío")
        if not self.crawl_pages_prefix:
            raise ValueError("crawl_pages_prefix no puede estar vacío")
        if not self.documents_object_key:
            raise ValueError("documents_object_key no puede estar vacío")
        if not self.output_prefix:
            raise ValueError("output_prefix no puede estar vacío")
        if self.max_concurrent_reads <= 0:
            raise ValueError("max_concurrent_reads debe ser positivo")


@dataclass(frozen=True, slots=True)
class PageRankRunStats:
    """Resumen final de una ejecución de `DistributedPageRankPipeline.run()`,
    combinando las estadísticas de materialización del grafo (propias de esta
    fase) con las de convergencia y construcción del grafo que
    `pagerank-link-analysis` ya calcula sin modificar."""

    pages_materialized: int
    pages_missing: int
    pages_skipped_malformed: int
    total_documents: int
    resolved_edges: int
    dangling_documents: int
    unresolved_source_entries: int
    unresolved_target_links: int
    iterations_run: int
    converged: bool
    final_delta: float
    elapsed_seconds: float
