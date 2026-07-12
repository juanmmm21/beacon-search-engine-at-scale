"""Paso *reduce* del pipeline de indexación distribuida: fusiona N índices
parciales ya remapeados al espacio global de `doc_id` (uno por partición,
ver `partition_indexer.py`) en un único `InvertedIndex` global.

No existe hoy ningún repo del ecosistema que haga esta fusión de N índices en
uno -- es lógica nueva de esta fase, pero deliberadamente delgada: la parte
difícil (producir un orden total correcto) ya la resolvió la asignación de
rangos de `doc_id_ranges.py` y el remapeo de `partition_indexer.py`, no este
módulo (ver `ARCHITECTURE.md`, fase 3, sección 2).

**Requisito del llamador, no verificado por reordenamiento:** `partial_indexes`
debe llegar en el mismo orden ascendente de `partition_key` en que
`doc_id_ranges.compute_doc_id_ranges` asignó los rangos -- este módulo
concatena las posting lists de cada término en ese orden en vez de fusionarlas
con un merge-sort general, precisamente porque ese orden ya garantiza que el
resultado queda ascendente por `doc_id` (una partición entera siempre precede
a la siguiente en el espacio de `doc_id`). Pasar los índices en cualquier
otro orden produciría posting lists no ascendentes en silencio si esta
función no comprobara la propiedad -- por eso sí se verifica explícitamente
(`_ensure_strictly_increasing_ranges`), en vez de asumir que el llamador
nunca se equivoca.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from inverted_index_builder.models import DocumentRecord, IndexStats, InvertedIndex, PostingsList

from beacon_scale_infra.errors import IndexingError


def _ensure_strictly_increasing_ranges(partial_indexes: Sequence[InvertedIndex]) -> None:
    last_max_doc_id = -1
    for index in partial_indexes:
        if not index.documents:
            continue
        current_min_doc_id = min(index.documents)
        if current_min_doc_id <= last_max_doc_id:
            raise IndexingError(
                "los índices parciales no llegaron en orden ascendente y disjunto de doc_id: "
                f"doc_id {current_min_doc_id} no es mayor que el máximo previo {last_max_doc_id}"
            )
        last_max_doc_id = max(index.documents)


def merge_partition_indexes(partial_indexes: Sequence[InvertedIndex]) -> InvertedIndex:
    """Fusiona índices parciales disjuntos, ya ordenados por rango de
    `doc_id` ascendente, en un único `InvertedIndex` global.

    - `documents`: unión directa de los diccionarios por partición. Los
      rangos son disjuntos por construcción (`doc_id_ranges.py`), así que
      ninguna clave debería repetirse -- si ocurre, es una violación de esa
      invariante, no un caso normal a tolerar, y se levanta `IndexingError`.
    - `postings_lists`, por término: concatenación (no merge-sort) de las
      posting lists de cada partición en el orden recibido -- ya ascendente
      por construcción, ver el docstring del módulo.
    """
    _ensure_strictly_increasing_ranges(partial_indexes)

    documents: dict[int, DocumentRecord] = {}
    postings_by_term: dict[str, list[PostingsList]] = defaultdict(list)

    for index in partial_indexes:
        for doc_id, record in index.documents.items():
            if doc_id in documents:
                raise IndexingError(
                    f"doc_id duplicado entre particiones tras el remapeo global: {doc_id}"
                )
            documents[doc_id] = record
        for term, postings_list in index.postings_lists.items():
            postings_by_term[term].append(postings_list)

    postings_lists: dict[str, PostingsList] = {}
    for term, lists_for_term in postings_by_term.items():
        merged_postings = tuple(
            posting for postings_list in lists_for_term for posting in postings_list.postings
        )
        postings_lists[term] = PostingsList(
            term=term,
            document_frequency=len(merged_postings),
            postings=merged_postings,
        )

    return InvertedIndex(
        documents=documents,
        postings_lists=postings_lists,
        stats=_compute_stats(documents, postings_lists),
    )


def _compute_stats(
    documents: dict[int, DocumentRecord], postings_lists: dict[str, PostingsList]
) -> IndexStats:
    """Misma aritmética que `IndexBuilder._compute_stats` hace internamente
    (suma/longitud, no lógica de indexación) -- no reutilizable directamente
    porque ese método es privado a `IndexBuilder` y opera sobre una única
    pasada de construcción, pero recalcularla aquí no reimplementa nada que
    `inverted-index-builder` deba poseer (ver `ARCHITECTURE.md`, fase 3,
    sección 2)."""
    total_documents = len(documents)
    total_postings = sum(len(postings_list.postings) for postings_list in postings_lists.values())
    average_length = (
        sum(record.length for record in documents.values()) / total_documents
        if total_documents > 0
        else 0.0
    )
    return IndexStats(
        total_documents=total_documents,
        vocabulary_size=len(postings_lists),
        total_postings=total_postings,
        average_document_length=average_length,
    )
