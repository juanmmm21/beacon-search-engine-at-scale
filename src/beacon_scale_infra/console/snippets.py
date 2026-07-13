"""Resolución `doc_id global -> texto real` contra las particiones de fase 2.

`beacon-search-console` resuelve snippets cargando entero en memoria el
`documents.jsonl` único de `html-content-extractor` (`doc_id` = posición de
línea) -- válido para su corpus de ~180 páginas, inviable a la escala de este
repo: serían gigabytes de texto duplicados en cada réplica de la API. Aquí la
resolución usa el esquema global de `doc_id` de fase 3 llevado al nivel de
fichero de parte (`index/corpus_catalog.py`): búsqueda binaria de `doc_id` a
su fichero de parte de fase 2, descarga bajo demanda de ese único fichero
(~`flush_every_pages` documentos) y una LRU acotada de partes calientes en
memoria de proceso -- nunca el corpus completo, y nunca una caché de
crecimiento ilimitado (regla de `CLAUDE.md`, sección 2).

La construcción del snippet en sí (ventana + rangos de resaltado) es
exactamente `beacon_search_console.snippets.build_snippet`, importada del
paquete real de la consola sin reescribirla (ver `ARCHITECTURE.md`, fase 6):
este módulo solo cambia *de dónde sale el texto*, no cómo se presenta.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from beacon_scale_infra.errors import ObjectStorageError
from beacon_scale_infra.index.corpus_catalog import CorpusCatalog, CorpusPartEntry
from beacon_scale_infra.protocols import ObjectStorage

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ResolvedDocument:
    """`url`/`title`/`main_text` de un documento de fase 2, tal como los
    serializó `ExtractWorker` (mismos campos que `beacon-search-console` lee
    de su `documents.jsonl` original)."""

    url: str
    title: str
    main_text: str


class PartitionedSnippetResolver:
    """Resuelve `doc_id -> ResolvedDocument` bajo demanda.

    Un `doc_id` fuera de todo rango del catálogo, o cuyo fichero de parte ya
    no existe/no se puede leer, resuelve a `None` (registrado): el llamador
    descarta ese resultado y sigue sirviendo el resto -- el mismo criterio
    que la ruta de búsqueda de `beacon-search-console` aplica a un `doc_id`
    ausente de su tabla de snippets, nunca un 500 por un documento suelto.
    """

    def __init__(
        self,
        storage: ObjectStorage,
        bucket: str,
        catalog: CorpusCatalog,
        *,
        max_cached_parts: int = 32,
    ) -> None:
        if max_cached_parts <= 0:
            raise ValueError(f"max_cached_parts debe ser positivo, recibido {max_cached_parts}")
        self._storage = storage
        self._bucket = bucket
        self._catalog = catalog
        self._max_cached_parts = max_cached_parts
        # dict preserva orden de inserción: la primera clave es siempre la
        # parte usada menos recientemente (misma técnica LRU que
        # cache/memory.py).
        self._parts_cache: dict[str, tuple[ResolvedDocument, ...]] = {}

    @property
    def catalog(self) -> CorpusCatalog:
        return self._catalog

    async def resolve(self, doc_id: int) -> ResolvedDocument | None:
        part = self._catalog.part_for(doc_id)
        if part is None:
            logger.warning(
                "doc_id %s fuera de todo rango del catálogo de corpus (versión %s)",
                doc_id,
                self._catalog.index_version,
            )
            return None
        documents = await self._documents_of_part(part)
        if documents is None:
            return None
        local_position = doc_id - part.start_doc_id
        if local_position >= len(documents):
            # El fichero de parte real tiene menos documentos de los que el
            # catálogo declara: artefactos de builds mezcladas en el bucket.
            logger.warning(
                "el fichero de parte %r contiene %s documentos pero el catálogo esperaba al "
                "menos %s (doc_id %s): ¿se sobrescribió la partición tras 'build-index'?",
                part.object_key,
                len(documents),
                local_position + 1,
                doc_id,
            )
            return None
        return documents[local_position]

    async def _documents_of_part(
        self, part: CorpusPartEntry
    ) -> tuple[ResolvedDocument, ...] | None:
        cached = self._parts_cache.get(part.object_key)
        if cached is not None:
            del self._parts_cache[part.object_key]
            self._parts_cache[part.object_key] = cached
            return cached
        try:
            raw = await self._storage.get_object(self._bucket, part.object_key)
        except ObjectStorageError as exc:
            logger.warning(
                "fallo al descargar el fichero de parte %r para snippets: %s",
                part.object_key,
                exc,
            )
            return None
        documents = _parse_part_documents(raw, part.object_key)
        if documents is None:
            return None
        self._parts_cache[part.object_key] = documents
        while len(self._parts_cache) > self._max_cached_parts:
            oldest_key = next(iter(self._parts_cache))
            del self._parts_cache[oldest_key]
        return documents


def _parse_part_documents(raw: bytes, part_key: str) -> tuple[ResolvedDocument, ...] | None:
    """Parsea las líneas no vacías de un fichero de parte (el mismo criterio
    de posición que la asignación de `doc_id` de fase 3, ver
    `index/corpus_catalog.py`). Una línea ilegible invalida la parte entera
    (posiciones posteriores quedarían desalineadas): se registra y se
    devuelve `None`, degradando solo los resultados de esa parte."""
    documents: list[ResolvedDocument] = []
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError as exc:
            logger.warning("línea JSON ilegible en el fichero de parte %r: %s", part_key, exc)
            return None
        documents.append(
            ResolvedDocument(
                url=str(data.get("url", "")),
                title=str(data.get("title", "")),
                main_text=str(data.get("main_text", "")),
            )
        )
    return tuple(documents)
