"""Asignación de `doc_id` global: un rango contiguo y disjunto por partición,
calculado a partir del manifiesto de fase 2 -- nunca de un contador
centralizado (ver `ARCHITECTURE.md`, fase 3, sección 1, para el porqué
completo y las alternativas descartadas).

Las particiones se ordenan por `partition_key` (el `worker_id`, comparado
lexicográficamente) y cada una recibe el rango `[start, start +
document_count)`, donde `start` es la suma acumulada de `document_count` de
toda partición que ordene antes que ella. El resultado es, por construcción,
denso (sin huecos) y disjunto: no hace falta ningún `WATCH`/`MULTI` ni
coordinación entre particiones para calcularlo, solo leer el manifiesto una
vez.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass

from beacon_scale_infra.errors import IndexingError
from beacon_scale_infra.extract.manifest import ExtractManifest


@dataclass(frozen=True, slots=True)
class PartitionDocIdRange:
    """Rango semiabierto `[start_doc_id, end_doc_id)` de `doc_id` globales
    reservado a una partición de fase 2."""

    partition_key: str
    start_doc_id: int
    end_doc_id: int

    def __post_init__(self) -> None:
        if self.start_doc_id < 0:
            raise ValueError(f"start_doc_id no puede ser negativo: {self.start_doc_id}")
        if self.end_doc_id < self.start_doc_id:
            raise ValueError(
                f"end_doc_id ({self.end_doc_id}) no puede ser menor que "
                f"start_doc_id ({self.start_doc_id})"
            )

    @property
    def document_count(self) -> int:
        return self.end_doc_id - self.start_doc_id

    def contains(self, doc_id: int) -> bool:
        return self.start_doc_id <= doc_id < self.end_doc_id


@dataclass(frozen=True, slots=True)
class DocIdRangeAssignment:
    """Asignación completa de rangos, ordenada ascendentemente por
    `start_doc_id` (equivalente a orden ascendente de `partition_key`, ver
    `compute_doc_id_ranges`). Particiones sin documentos no aparecen aquí:
    un rango vacío no aporta nada a la búsqueda binaria de `partition_for` y
    complicaría su condición de contorno sin necesidad."""

    ranges: tuple[PartitionDocIdRange, ...]

    def __post_init__(self) -> None:
        starts = [doc_range.start_doc_id for doc_range in self.ranges]
        if starts != sorted(starts):
            raise IndexingError("los rangos de doc_id deben venir ordenados por start_doc_id")

    @property
    def total_documents(self) -> int:
        return self.ranges[-1].end_doc_id if self.ranges else 0

    def range_for_partition(self, partition_key: str) -> PartitionDocIdRange:
        for doc_range in self.ranges:
            if doc_range.partition_key == partition_key:
                return doc_range
        raise IndexingError(f"partición desconocida en la asignación de doc_id: {partition_key!r}")

    def partition_for(self, doc_id: int) -> str:
        """Resuelve, de forma barata (búsqueda binaria sobre los límites de
        rango, no sobre documentos), qué partición posee `doc_id`."""
        starts = [doc_range.start_doc_id for doc_range in self.ranges]
        index = bisect_right(starts, doc_id) - 1
        if index < 0 or not self.ranges[index].contains(doc_id):
            raise IndexingError(f"doc_id fuera de cualquier rango asignado: {doc_id}")
        return self.ranges[index].partition_key


def compute_doc_id_ranges(manifest: ExtractManifest) -> DocIdRangeAssignment:
    """Calcula la asignación de rangos a partir del manifiesto agregado de
    fase 2. Determinista: dado el mismo manifiesto, siempre produce la misma
    asignación, porque el orden de particiones (`partition_key` ascendente)
    no depende de en qué orden `read_manifest` haya listado sus fragmentos."""
    ordered_entries = sorted(manifest.partitions, key=lambda entry: entry.partition_key)

    ranges: list[PartitionDocIdRange] = []
    offset = 0
    for entry in ordered_entries:
        start = offset
        end = offset + entry.document_count
        if entry.document_count > 0:
            ranges.append(
                PartitionDocIdRange(
                    partition_key=entry.partition_key, start_doc_id=start, end_doc_id=end
                )
            )
        offset = end

    return DocIdRangeAssignment(ranges=tuple(ranges))
