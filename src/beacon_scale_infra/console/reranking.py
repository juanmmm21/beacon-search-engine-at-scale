"""Reranking LTR sobre el resultado ya fusionado del fan-out, con el estado
pesado cargado una única vez por réplica.

`learning_to_rank_reranker.pipeline.rerank` construye un `InvertedIndexReader`
y relee los scores de PageRank *en cada llamada* -- correcto para su propio
alcance (una invocación puntual sobre un directorio local), pero por consulta
significaría recargar el índice completo en cada búsqueda. Este módulo
compone las mismas piezas públicas de ese paquete (`InvertedIndexReader`,
`read_pagerank_scores`, `extract_features`, `LightGBMReranker.predict`) con
los dos lectores construidos una vez al arrancar la réplica, y reproduce el
criterio de desempate estándar del ecosistema (score descendente, `doc_id`
ascendente) -- nunca reimplementa extracción de features ni el modelo.

A diferencia del puente por-shard de `beacon-search-console`
(`reranking.py` de ese repo, que agrupa candidatos por el shard dueño de cada
`doc_id` y rerankea contra el índice local de ese shard), aquí se rerankea
contra el índice *global* de fase 3: los `doc_id` que devuelven los shards ya
son globales, y ese índice contiene la longitud y las posiciones de término
de cualquier candidato, venga del shard que venga. El agrupado por shard de
la consola original existe solo porque `rerank()` abre un único directorio
por llamada y su `data/` no retiene el índice global tras particionar -- una
restricción de su despliegue de proceso único, no del problema (ver
`ARCHITECTURE.md`, fase 6).
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import numpy as np
from distributed_index_sharding.models import ShardHit
from learning_to_rank_reranker.feature_extraction import extract_features
from learning_to_rank_reranker.index_reader import InvertedIndexReader
from learning_to_rank_reranker.model import LightGBMReranker
from learning_to_rank_reranker.models import Candidate, RankedDocument
from learning_to_rank_reranker.pagerank_reader import read_pagerank_scores

from beacon_scale_infra.errors import ConsoleServingError


class PreloadedRerankContext:
    """Lectores y modelo cargados una vez por réplica de la API. Seguros de
    reconstruir idénticamente en cada réplica: se construyen desde artefactos
    inmutables de una build concreta del índice (ver `console/artifacts.py`),
    así que N réplicas producen exactamente el mismo reranking."""

    def __init__(
        self,
        *,
        index_reader: InvertedIndexReader,
        pagerank_scores: dict[int, float],
        model: LightGBMReranker,
    ) -> None:
        self._index_reader = index_reader
        self._pagerank_scores = pagerank_scores
        self._model = model

    @classmethod
    def load(
        cls, *, index_dir: Path, pagerank_dir: Path, model_dir: Path
    ) -> PreloadedRerankContext:
        try:
            index_reader = InvertedIndexReader(index_dir)
        except (OSError, KeyError, ValueError) as exc:
            raise ConsoleServingError(
                f"fallo al cargar el índice global para reranking desde {index_dir}: {exc}"
            ) from exc
        try:
            pagerank_scores = read_pagerank_scores(pagerank_dir)
        except (OSError, KeyError, ValueError) as exc:
            raise ConsoleServingError(
                f"fallo al cargar los scores de PageRank desde {pagerank_dir}: {exc}"
            ) from exc
        try:
            model = LightGBMReranker.load(model_dir)
        except (OSError, KeyError, ValueError, RuntimeError) as exc:
            raise ConsoleServingError(
                f"fallo al cargar el modelo LTR desde {model_dir}: ¿corrió 'train-reranker'? "
                f"({exc})"
            ) from exc
        return cls(index_reader=index_reader, pagerank_scores=pagerank_scores, model=model)

    def rerank(
        self, hits: Sequence[ShardHit], query_terms: Sequence[str], *, top_k: int
    ) -> list[RankedDocument]:
        """Reordena `hits` (ya fusionados entre shards por score BM25) con el
        modelo LTR. Sin candidatos o sin términos de query (una búsqueda
        resuelta solo por filtros), no hay features que extraer: lista vacía,
        nunca el modelo invocado con entradas degeneradas -- mismo contrato
        que `rerank_sharded_hits` en `beacon-search-console`."""
        if not hits or not query_terms:
            return []
        candidates = [Candidate(doc_id=hit.doc_id, bm25_score=hit.score) for hit in hits]
        features = extract_features(
            candidates, query_terms, self._index_reader, self._pagerank_scores
        )
        feature_matrix = np.array([feature.to_array() for feature in features], dtype=np.float64)
        scores = self._model.predict(feature_matrix)
        ranked = sorted(
            zip((candidate.doc_id for candidate in candidates), scores.tolist(), strict=True),
            key=lambda item: (-item[1], item[0]),
        )
        return [
            RankedDocument(doc_id=doc_id, final_score=score) for doc_id, score in ranked[:top_k]
        ]
