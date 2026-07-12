"""Particionado de `documents.jsonl` dentro del almacenamiento de objetos.

Cada `ExtractWorker` es dueño de su propia partición, identificada por su
`worker_id` -- a diferencia del particionado por fecha + shard de hash de
`crawl/partitioning.py` (necesario ahí porque *todos* los workers de crawl
escriben bajo el mismo `date=.../shard=NNN` cuando dos páginas distintas
caen en el mismo día y el mismo shard), aquí no hace falta ninguna función de
hash: como ningún otro worker escribe jamás bajo el prefijo de este
`worker_id`, la partición no necesita repartirse de forma determinista entre
réplicas para evitar colisiones, solo aislar a cada una en su propio
subespacio de claves -- la forma más simple de que N workers escriban en
paralelo sin ninguna coordinación entre ellos (ver `ARCHITECTURE.md`, fase 2).

Dentro de la partición de un worker, cada `flush` produce un fichero de parte
nuevo (`documents-NNNNNN.jsonl` / `discarded-NNNNNN.jsonl`), nunca
sobrescrito: `ObjectStorage.put_object` no soporta *append*, así que
reescribir un único fichero cada vez que se acumulan más documentos
retransmitiría todo el contenido ya escrito en cada `flush`, un coste
cuadrático sobre el tamaño de la partición a la escala de esta fase (unos
pocos millones de páginas). Muchos ficheros de parte inmutables por
partición es el mismo patrón que Spark/Hive usan para escribir salidas
particionadas.
"""

from __future__ import annotations


def object_key_for_document_part(
    partition_key: str, part_seq: int, *, prefix: str = "extracted-documents"
) -> str:
    """Clave de objeto para un lote de documentos extraídos, con el formato
    `<prefix>/partition=<partition_key>/documents-<NNNNNN>.jsonl`."""
    return f"{prefix}/partition={partition_key}/documents-{part_seq:06d}.jsonl"


def object_key_for_discarded_part(
    partition_key: str, part_seq: int, *, prefix: str = "extracted-documents"
) -> str:
    """Clave de objeto para un lote de páginas descartadas, con el formato
    `<prefix>/partition=<partition_key>/discarded-<NNNNNN>.jsonl`."""
    return f"{prefix}/partition={partition_key}/discarded-{part_seq:06d}.jsonl"
