"""Tipos de datos de la indexación distribuida (fase 3): configuración del
pipeline y resumen final de una ejecución."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IndexingPipelineConfig:
    """Configuración de una ejecución de `IndexingPipeline.run()`.

    A diferencia de `CrawlWorkerConfig`/`ExtractWorkerConfig`, no hay
    `worker_id` ni consumidor de cola: este pipeline no es una réplica entre
    varias, es el único job de fase 3 (ver `ARCHITECTURE.md`, fase 3, sección
    0, sobre por qué esta fase es un job por lotes, no un worker de larga
    duración).
    """

    bucket: str = "beacon-scale-dev"
    extract_prefix: str = "extracted-documents"
    index_output_prefix: str = "search-index"
    corpus_object_key: str = "search-index/corpus/documents.jsonl"
    corpus_catalog_object_key: str = "search-index/corpus_catalog.json"
    compress: bool = True
    compressed_output_prefix: str = "search-index-compressed"

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket no puede estar vacío")
        if not self.extract_prefix:
            raise ValueError("extract_prefix no puede estar vacío")
        if not self.index_output_prefix:
            raise ValueError("index_output_prefix no puede estar vacío")
        if not self.corpus_object_key:
            raise ValueError("corpus_object_key no puede estar vacío")
        if not self.corpus_catalog_object_key:
            raise ValueError("corpus_catalog_object_key no puede estar vacío")
        if not self.compressed_output_prefix:
            raise ValueError("compressed_output_prefix no puede estar vacío")


@dataclass(frozen=True, slots=True)
class IndexingRunStats:
    """Resumen final de una ejecución de `IndexingPipeline.run()`."""

    partitions_indexed: int
    total_documents: int
    vocabulary_size: int
    total_postings: int
    compression_ratio: float | None
    index_version: str
