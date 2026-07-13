"""Catálogo de resolución `doc_id global -> fichero de parte de fase 2`.

La fase 3 asigna a cada documento un `doc_id` global igual a su posición en
la concatenación ordenada de las particiones de fase 2 (`doc_id_ranges.py`).
Ese mismo orden, llevado un nivel más abajo (a los ficheros de parte
`documents-NNNNNN.jsonl` dentro de cada partición, concatenados en orden
ascendente de `part_seq`), da a cada *fichero de parte* un rango contiguo y
disjunto de `doc_id` globales `[start_doc_id, start_doc_id +
document_count)` -- exactamente la misma construcción que la asignación por
partición, con la misma consecuencia: resolver qué parte contiene un
`doc_id` es una búsqueda binaria sobre los límites de rango, nunca sobre
documentos.

Esto es lo que permite a la consola (fase 6) resolver `doc_id -> texto real`
contra las particiones de fase 2 *bajo demanda* (descargar un único fichero
de parte de ~`flush_every_pages` documentos por acierto), en vez de cargar el
corpus completo en la memoria de cada réplica de la API como hace
`beacon-search-console` con su `documents.jsonl` único de proceso único --
inviable a la escala objetivo de este repo (ver `ARCHITECTURE.md`, fase 6).

El recuento de líneas replica exactamente el contrato de `doc_id` de
`inverted_index_builder.pipeline.IndexBuilder.build`: solo cuentan las líneas
no vacías (una línea en blanco no consume `doc_id`).
"""

from __future__ import annotations

import json
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from beacon_scale_infra.errors import IndexingError, ObjectStorageError
from beacon_scale_infra.protocols import ObjectStorage

_CATALOG_FORMAT_VERSION: Final[int] = 1
_DOCUMENT_PART_PREFIX: Final[str] = "documents-"


@dataclass(frozen=True, slots=True)
class CorpusPartEntry:
    """Rango semiabierto de `doc_id` globales contenido en un único fichero
    de parte de fase 2 (`object_key`), en el bucket del corpus."""

    partition_key: str
    object_key: str
    start_doc_id: int
    document_count: int

    def __post_init__(self) -> None:
        if self.start_doc_id < 0:
            raise ValueError(f"start_doc_id no puede ser negativo: {self.start_doc_id}")
        if self.document_count < 0:
            raise ValueError(f"document_count no puede ser negativo: {self.document_count}")

    @property
    def end_doc_id(self) -> int:
        return self.start_doc_id + self.document_count

    def contains(self, doc_id: int) -> bool:
        return self.start_doc_id <= doc_id < self.end_doc_id

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "partition_key": self.partition_key,
            "object_key": self.object_key,
            "start_doc_id": self.start_doc_id,
            "document_count": self.document_count,
        }

    @staticmethod
    def from_json_dict(data: dict[str, Any]) -> CorpusPartEntry:
        return CorpusPartEntry(
            partition_key=str(data["partition_key"]),
            object_key=str(data["object_key"]),
            start_doc_id=int(data["start_doc_id"]),
            document_count=int(data["document_count"]),
        )


@dataclass(frozen=True, slots=True)
class CorpusCatalog:
    """Vista completa de la resolución `doc_id -> parte`, más los dos datos
    agregados del corpus que la consola necesita sin escanearlo entera
    (`total_documents`, `last_crawled_at`). `index_version` ata el catálogo a
    la build exacta del índice que lo produjo: un catálogo de otra versión
    resolvería `doc_id`s contra el texto equivocado."""

    index_version: str
    total_documents: int
    last_crawled_at: str | None
    parts: tuple[CorpusPartEntry, ...]

    def __post_init__(self) -> None:
        starts = [part.start_doc_id for part in self.parts]
        if starts != sorted(starts):
            raise IndexingError(
                "las partes del catálogo de corpus deben venir ordenadas por start_doc_id"
            )

    def part_for(self, doc_id: int) -> CorpusPartEntry | None:
        """`None` si `doc_id` no cae en ningún rango (p. ej. un `doc_id` de
        otra versión del índice) -- el llamador decide cómo degradar."""
        starts = [part.start_doc_id for part in self.parts]
        index = bisect_right(starts, doc_id) - 1
        if index < 0 or not self.parts[index].contains(doc_id):
            return None
        return self.parts[index]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "format_version": _CATALOG_FORMAT_VERSION,
            "index_version": self.index_version,
            "total_documents": self.total_documents,
            "last_crawled_at": self.last_crawled_at,
            "parts": [part.to_json_dict() for part in self.parts],
        }

    @staticmethod
    def from_json_dict(data: dict[str, Any]) -> CorpusCatalog:
        raw_last_crawled = data.get("last_crawled_at")
        return CorpusCatalog(
            index_version=str(data["index_version"]),
            total_documents=int(data["total_documents"]),
            last_crawled_at=None if raw_last_crawled is None else str(raw_last_crawled),
            parts=tuple(CorpusPartEntry.from_json_dict(part) for part in data.get("parts", [])),
        )


@dataclass(frozen=True, slots=True)
class PartitionMaterialization:
    """Resultado de materializar una partición de fase 2 a un fichero local:
    el desglose por fichero de parte (con su rango global ya asignado) y el
    `fetched_at` máximo observado -- `parts` incluye también las partes
    vacías (0 documentos), para que el llamador pueda distinguir "partición
    sin ficheros" de "ficheros sin documentos"."""

    parts: tuple[CorpusPartEntry, ...]
    last_fetched_at: str | None

    @property
    def document_count(self) -> int:
        return sum(part.document_count for part in self.parts)


async def list_document_part_keys(
    storage: ObjectStorage, bucket: str, prefix: str, partition_key: str
) -> list[str]:
    """Claves de los ficheros de parte `documents-*.jsonl` de una partición,
    en orden ascendente de `part_seq` -- el padding a ancho fijo de
    `extract/partitioning.py` hace que el orden lexicográfico coincida con el
    numérico, el mismo orden en que `ExtractWorker` los escribió y el que la
    asignación de `doc_id` de fase 3 necesita preservar."""
    partition_prefix = f"{prefix}/partition={partition_key}/{_DOCUMENT_PART_PREFIX}"
    keys = [
        object_metadata.key
        async for object_metadata in storage.list_objects(bucket, prefix=partition_prefix)
    ]
    return sorted(keys)


async def materialize_partition_with_parts(
    storage: ObjectStorage,
    bucket: str,
    prefix: str,
    partition_key: str,
    destination: Path,
    *,
    start_doc_id: int,
) -> PartitionMaterialization:
    """Concatena los ficheros de parte de una partición en `destination`
    (el mismo fichero de entrada que `IndexBuilder.build` espera) y, en la
    misma pasada, computa el rango global de `doc_id` de cada parte y el
    `fetched_at` máximo del corpus -- una línea JSON ilegible o un fallo de
    lectura levantan `IndexingError` con la clave de la parte culpable, nunca
    un fallo silencioso que desalinease todo `doc_id` posterior."""
    parts: list[CorpusPartEntry] = []
    last_fetched_at: str | None = None
    next_doc_id = start_doc_id
    try:
        part_keys = await list_document_part_keys(storage, bucket, prefix, partition_key)
        with destination.open("wb") as destination_file:
            for part_key in part_keys:
                raw = await storage.get_object(bucket, part_key)
                destination_file.write(raw)
                if raw and not raw.endswith(b"\n"):
                    destination_file.write(b"\n")
                line_count, part_last_fetched = _scan_part_lines(raw, part_key)
                parts.append(
                    CorpusPartEntry(
                        partition_key=partition_key,
                        object_key=part_key,
                        start_doc_id=next_doc_id,
                        document_count=line_count,
                    )
                )
                next_doc_id += line_count
                if part_last_fetched is not None and (
                    last_fetched_at is None or part_last_fetched > last_fetched_at
                ):
                    last_fetched_at = part_last_fetched
    except (OSError, ObjectStorageError) as exc:
        raise IndexingError(
            f"fallo al materializar la partición {partition_key!r} en {destination}: {exc}"
        ) from exc
    return PartitionMaterialization(parts=tuple(parts), last_fetched_at=last_fetched_at)


def _scan_part_lines(raw: bytes, part_key: str) -> tuple[int, str | None]:
    """Cuenta las líneas no vacías de un fichero de parte (el mismo criterio
    con el que `IndexBuilder.build` asigna `doc_id`) y devuelve el
    `fetched_at` máximo entre ellas (comparación lexicográfica válida: los
    workers de fase 2 siempre serializan ISO 8601 con offset explícito)."""
    line_count = 0
    last_fetched_at: str | None = None
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise IndexingError(
                f"línea JSON ilegible en el fichero de parte {part_key!r}: {exc}"
            ) from exc
        line_count += 1
        fetched_at = record.get("fetched_at") if isinstance(record, dict) else None
        if (
            isinstance(fetched_at, str)
            and fetched_at
            and (last_fetched_at is None or fetched_at > last_fetched_at)
        ):
            last_fetched_at = fetched_at
    return line_count, last_fetched_at
