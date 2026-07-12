"""Materializa `link_graph.jsonl` a partir de las páginas crudas que fase 1
escribió en `crawl-pages/` -- la pieza genuinamente nueva de esta fase (ver
`ARCHITECTURE.md`, fase 4, sección 3).

A diferencia del paso *map* de fase 3 (`index/partition_indexer.py`), que lee
un puñado de ficheros de parte grandes por partición, `crawl-pages/` contiene
un objeto por página crawleada -- para un corpus de unos pocos millones de
páginas, unos pocos millones de objetos individuales. Leerlos con un bucle
secuencial de `get_object` convertiría el cuello de botella real de esta fase
en latencia de red, no en cómputo (ver `ARCHITECTURE.md`, fase 4, sección 3,
para el cálculo). Por eso las lecturas se reparten entre un número acotado de
workers concurrentes que comparten un único iterador de claves -- seguro sin
ningún lock explícito porque `asyncio` es cooperativo de un solo hilo y
`next()` sobre el iterador nunca cede el control a mitad de ejecución -- en
vez de crear una `Task` por clave de golpe (que para varios millones de claves
agotaría memoria en objetos `Task` pendientes antes de que el semáforo
llegara a limitar nada).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from beacon_scale_infra.errors import ObjectNotFoundError, ObjectStorageError
from beacon_scale_infra.protocols import ObjectStorage


@dataclass(frozen=True, slots=True)
class LinkGraphMaterializationStats:
    """Resumen de una ejecución de `materialize_link_graph`."""

    pages_materialized: int
    pages_missing: int
    pages_skipped_malformed: int


def _parse_crawled_page_record(raw: bytes) -> tuple[str, list[str]] | None:
    """Extrae `(final_url, outlinks)` de un `CrawledPageRecord` serializado
    (`crawl/models.py::to_json_dict`), o `None` si el objeto no tiene la
    forma esperada -- nunca lanza, la llamada cuenta el fallo y sigue."""
    try:
        data = json.loads(raw)
        final_url = str(data["final_url"])
        outlinks = [str(outlink) for outlink in data["outlinks"]]
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None
    return final_url, outlinks


async def materialize_link_graph(
    storage: ObjectStorage,
    bucket: str,
    crawl_pages_prefix: str,
    destination: Path,
    *,
    max_concurrent_reads: int = 64,
) -> LinkGraphMaterializationStats:
    """Escanea `crawl_pages_prefix` y escribe en `destination` una línea
    `{"url": ..., "outlinks": [...]}` por página, la forma exacta que
    `pagerank_link_analysis.link_graph_reader.read_link_graph_entries`
    espera. `url` es `CrawledPageRecord.final_url` (post-redirección),
    exactamente la convención que esa librería ya documenta para
    `link_graph.jsonl` (ver `ARCHITECTURE.md`, fase 4, sección 3).

    Las lecturas se reparten entre como mucho `max_concurrent_reads`
    peticiones concurrentes a `storage.get_object`. Una clave listada pero ya
    desaparecida se cuenta en `pages_missing`; una página que no parsea como
    un `CrawledPageRecord` válido se cuenta en `pages_skipped_malformed` --
    ninguna de las dos aborta el resto del escaneo (mismo principio que
    `ExtractWorker._process_job` aplica a una página referenciada pero ya
    ausente, ver `extract/worker.py`).
    """
    keys = [
        object_metadata.key
        async for object_metadata in storage.list_objects(bucket, prefix=crawl_pages_prefix)
    ]

    destination.parent.mkdir(parents=True, exist_ok=True)
    key_iterator: Iterator[str] = iter(keys)
    pages_materialized = 0
    pages_missing = 0
    pages_skipped_malformed = 0

    with destination.open("w", encoding="utf-8") as link_graph_file:

        async def _worker() -> None:
            nonlocal pages_materialized, pages_missing, pages_skipped_malformed
            for key in key_iterator:
                try:
                    raw = await storage.get_object(bucket, key)
                except ObjectNotFoundError:
                    pages_missing += 1
                    continue
                except ObjectStorageError:
                    pages_skipped_malformed += 1
                    continue

                parsed = _parse_crawled_page_record(raw)
                if parsed is None:
                    pages_skipped_malformed += 1
                    continue

                final_url, outlinks = parsed
                # Ninguna llamada entre la preparación de la línea y su
                # escritura cede el control a otro worker (no hay `await` de
                # por medio), así que escribir directamente al mismo
                # descriptor de fichero desde varios workers concurrentes es
                # seguro sin un lock explícito.
                link_graph_file.write(json.dumps({"url": final_url, "outlinks": outlinks}) + "\n")
                pages_materialized += 1

        worker_count = min(max_concurrent_reads, len(keys)) or 1
        await asyncio.gather(*(_worker() for _ in range(worker_count)))

    return LinkGraphMaterializationStats(
        pages_materialized=pages_materialized,
        pages_missing=pages_missing,
        pages_skipped_malformed=pages_skipped_malformed,
    )
