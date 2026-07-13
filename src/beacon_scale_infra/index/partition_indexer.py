"""Paso *map* del pipeline de indexación distribuida: construye el índice
invertido local de una única partición de fase 2 reutilizando
`inverted_index_builder.pipeline.IndexBuilder` sin modificar, y remapea sus
`doc_id` locales (0-indexados dentro de la partición) al espacio global de
`doc_id` (ver `ARCHITECTURE.md`, fase 3, secciones 1 y 2).

Cada partición es una unidad de trabajo completamente independiente de las
demás -- ninguna función de este módulo lee ni escribe el estado de otra
partición -- exactamente la misma propiedad que hace de la extracción de
fase 2 "el paso más fácil de paralelizar" (ver `extract/worker.py`): estas
funciones podrían correr en procesos separados sin cambiar nada de su forma,
aunque el pipeline actual (`pipeline.py`) las invoca secuencialmente en un
único proceso (ver `ARCHITECTURE.md`, fase 3, "Limitaciones conocidas").
"""

from __future__ import annotations

from pathlib import Path

from inverted_index_builder.models import DocumentRecord, InvertedIndex, Posting, PostingsList
from inverted_index_builder.pipeline import IndexBuilder

from beacon_scale_infra.errors import IndexingError
from beacon_scale_infra.index.corpus_catalog import materialize_partition_with_parts
from beacon_scale_infra.protocols import ObjectStorage


async def materialize_partition_documents(
    storage: ObjectStorage,
    bucket: str,
    prefix: str,
    partition_key: str,
    destination: Path,
) -> int:
    """Concatena, en orden ascendente de `part_seq`, todos los ficheros de
    parte `documents-*.jsonl` de una partición en un único fichero local --
    el mismo fichero de entrada que `IndexBuilder.build` espera, y el mismo
    orden que asignará como `local_doc_id` (orden de aparición en el
    fichero). Devuelve el número de ficheros de parte concatenados, para que
    el llamador pueda distinguir una partición vacía (`0`) de un fallo de
    lectura silencioso.

    Delegado en `corpus_catalog.materialize_partition_with_parts` (fase 6
    necesita además el desglose de rangos de `doc_id` por fichero de parte y
    el `fetched_at` máximo; este envoltorio conserva la firma original para
    quien solo necesita materializar) -- una única implementación del bucle
    de descarga, nunca dos copias que mantener sincronizadas.
    """
    materialization = await materialize_partition_with_parts(
        storage, bucket, prefix, partition_key, destination, start_doc_id=0
    )
    return len(materialization.parts)


def _remap_document(record: DocumentRecord, offset: int) -> DocumentRecord:
    return DocumentRecord(
        doc_id=record.doc_id + offset,
        url=record.url,
        title=record.title,
        length=record.length,
    )


def _remap_postings_list(postings_list: PostingsList, offset: int) -> PostingsList:
    remapped_postings = tuple(
        Posting(
            doc_id=posting.doc_id + offset,
            term_frequency=posting.term_frequency,
            positions=posting.positions,
        )
        for posting in postings_list.postings
    )
    return PostingsList(
        term=postings_list.term,
        document_frequency=postings_list.document_frequency,
        postings=remapped_postings,
    )


def remap_index_to_global_doc_ids(index: InvertedIndex, offset: int) -> InvertedIndex:
    """Traslada los `doc_id` locales de `index` (asignados por
    `IndexBuilder.build` sobre una única partición, 0-indexados) al espacio
    global de `doc_id`, sumando `offset` -- el `start_doc_id` del rango de
    esa partición (ver `doc_id_ranges.py`).

    Sumar una constante preserva el orden: una posting list que ya venía
    ascendente por `local_doc_id` (garantía de `IndexBuilder`, ver
    `inverted_index_builder/pipeline.py`) sigue ascendente por `doc_id`
    global tras el remapeo, sin necesitar ningún reordenamiento (ver
    `ARCHITECTURE.md`, fase 3, sección 2).
    """
    if offset == 0:
        return index
    if offset < 0:
        raise IndexingError(f"offset de doc_id no puede ser negativo: {offset}")

    documents = {
        record.doc_id + offset: _remap_document(record, offset)
        for record in index.documents.values()
    }
    postings_lists = {
        term: _remap_postings_list(postings_list, offset)
        for term, postings_list in index.postings_lists.items()
    }
    return InvertedIndex(documents=documents, postings_lists=postings_lists, stats=index.stats)


def build_index_from_materialized_partition(
    materialized_path: Path, doc_id_offset: int
) -> InvertedIndex:
    """Construye el índice local de una partición ya materializada
    (`materialize_partition_documents`) con `IndexBuilder.build`, sin
    modificar, y devuelve el resultado ya remapeado al espacio global de
    `doc_id`. Función pura sobre el fichero local -- no toca el
    almacenamiento de objetos -- para poder testearse sin dobles de red."""
    try:
        local_index = IndexBuilder().build(materialized_path)
    except (OSError, KeyError, ValueError) as exc:
        raise IndexingError(
            f"fallo al construir el índice local de {materialized_path}: {exc}"
        ) from exc
    return remap_index_to_global_doc_ids(local_index, doc_id_offset)
