"""Manifiesto de particiones de `documents.jsonl`, la interfaz que la fase de
indexación distribuida siguiente necesita para saber qué particiones existen
y cuántos documentos tiene cada una, sin tener que listar y contar cada
fichero de parte de cada partición al arrancar.

**Sin coordinación entre workers, igual que las particiones mismas.** Cada
`ExtractWorker` escribe (y sobrescribe en cada `flush`) un único fragmento de
manifiesto bajo su propia clave, namespaced por su `worker_id`
(`manifest/partition=<worker_id>.json`) -- nunca lee ni escribe el fragmento
de otro worker, así que no hace falta ningún `WATCH`/`MULTI` ni reintento de
escritura concurrente como sí necesita `CoordinatedRateLimiter` en fase 1
(ver `crawl/rate_limiter.py`). Sobrescribir el propio fragmento en cada
`flush` es barato: a diferencia de un fichero de parte, el fragmento es un
JSON minúsculo con solo contadores, no el contenido de los documentos.

`read_manifest` reconstruye el manifiesto completo listando ese prefijo
(`ObjectStorage.list_objects` ya pagina en streaming sin volcar el bucket
entero a memoria, ver `protocols.py`) y agregando cada fragmento -- el
manifiesto "único" que ve la fase de indexación es, en almacenamiento, tan
distribuido como las propias particiones que describe.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from beacon_scale_infra.protocols import ObjectStorage

_MANIFEST_PREFIX = "manifest"


@dataclass(frozen=True, slots=True)
class PartitionManifestEntry:
    """Estado acumulado de una partición, tal y como lo ve la última
    escritura de manifiesto de su `ExtractWorker` dueño."""

    partition_key: str
    document_count: int
    discarded_count: int
    part_file_count: int

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "partition_key": self.partition_key,
            "document_count": self.document_count,
            "discarded_count": self.discarded_count,
            "part_file_count": self.part_file_count,
        }

    @staticmethod
    def from_json_dict(data: dict[str, Any]) -> PartitionManifestEntry:
        return PartitionManifestEntry(
            partition_key=str(data["partition_key"]),
            document_count=int(data["document_count"]),
            discarded_count=int(data["discarded_count"]),
            part_file_count=int(data["part_file_count"]),
        )


@dataclass(frozen=True, slots=True)
class ExtractManifest:
    """Vista agregada de todas las particiones de un `object_key_prefix`
    dado, reconstruida a partir de sus fragmentos individuales."""

    partitions: tuple[PartitionManifestEntry, ...]

    @property
    def total_documents(self) -> int:
        return sum(entry.document_count for entry in self.partitions)

    @property
    def total_discarded(self) -> int:
        return sum(entry.discarded_count for entry in self.partitions)


def manifest_key_for_partition(prefix: str, partition_key: str) -> str:
    return f"{prefix}/{_MANIFEST_PREFIX}/partition={partition_key}.json"


async def write_partition_manifest(
    storage: ObjectStorage, bucket: str, prefix: str, entry: PartitionManifestEntry
) -> None:
    key = manifest_key_for_partition(prefix, entry.partition_key)
    body = json.dumps(entry.to_json_dict(), ensure_ascii=False).encode("utf-8")
    await storage.put_object(bucket, key, body, content_type="application/json")


async def read_manifest(storage: ObjectStorage, bucket: str, prefix: str) -> ExtractManifest:
    entries: list[PartitionManifestEntry] = []
    manifest_prefix = f"{prefix}/{_MANIFEST_PREFIX}/"
    async for object_metadata in storage.list_objects(bucket, prefix=manifest_prefix):
        raw = await storage.get_object(bucket, object_metadata.key)
        entries.append(PartitionManifestEntry.from_json_dict(json.loads(raw)))
    return ExtractManifest(partitions=tuple(entries))
