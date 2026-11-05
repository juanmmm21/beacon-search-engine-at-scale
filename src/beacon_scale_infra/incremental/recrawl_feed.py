"""Puente entre el re-rastreo de `web-crawler-scheduler` (>= 0.2.0) y el
indexador incremental.

`web-crawler-scheduler recrawl --output-dir` deja dos ficheros: `pages.jsonl`
con *solo* las páginas cuyo contenido cambió (mismo formato que un crawl
normal) y `removed.jsonl` con las que respondieron 404/410. Esto los
convierte en `IndexUpdate`:

- página cambiada → se extrae con la misma función que usa `extract-worker`
  (`extract_single_page`) → alta. Si ya no produce contenido indexable, es
  una baja: la versión vieja no debe seguir apareciendo en resultados.
- página desaparecida → baja.

Las páginas que no cambiaron (304 o mismo hash) ni aparecen en esos ficheros:
no generan ningún trabajo, que es justo el ahorro del re-rastreo por tasa de
cambio frente a recrawlear y reindexar todo.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from html_content_extractor.models import ExtractedDocument, ExtractionConfig

from beacon_scale_infra.errors import IncrementalIndexingError
from beacon_scale_infra.extract.page_extractor import extract_single_page
from beacon_scale_infra.incremental.models import IndexUpdate

logger = logging.getLogger(__name__)

_CHANGED_PAGES_FILENAME = "pages.jsonl"
_REMOVED_PAGES_FILENAME = "removed.jsonl"


@dataclass(frozen=True, slots=True)
class RecrawlFeedSummary:
    changed_pages: int
    upserts: int
    no_longer_indexable: int
    removed_pages: int


def _iter_jsonl(path: Path) -> Iterator[dict[str, object]]:
    if not path.exists():
        return
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise IncrementalIndexingError(
                    f"{path}:{line_number} no es JSON válido: {error}"
                ) from error
            if not isinstance(record, dict):
                raise IncrementalIndexingError(f"{path}:{line_number} no es un objeto JSON")
            yield record


def updates_from_recrawl(
    output_dir: Path, config: ExtractionConfig | None = None
) -> tuple[list[IndexUpdate], RecrawlFeedSummary]:
    if not output_dir.is_dir():
        raise IncrementalIndexingError(f"{output_dir} no es un directorio de salida de recrawl")
    extraction = config if config is not None else ExtractionConfig()
    updates: list[IndexUpdate] = []
    changed = upserts = gone_empty = removed = 0
    for record in _iter_jsonl(output_dir / _CHANGED_PAGES_FILENAME):
        changed += 1
        result = extract_single_page(record, extraction)
        if isinstance(result, ExtractedDocument):
            updates.append(IndexUpdate.upsert(result.to_json_dict()))
            upserts += 1
        else:
            logger.info("%s ya no es indexable (%s): baja", result.url, result.reason)
            updates.append(IndexUpdate.delete(result.url))
            gone_empty += 1
    for record in _iter_jsonl(output_dir / _REMOVED_PAGES_FILENAME):
        updates.append(IndexUpdate.delete(str(record["url"])))
        removed += 1
    return updates, RecrawlFeedSummary(changed, upserts, gone_empty, removed)
