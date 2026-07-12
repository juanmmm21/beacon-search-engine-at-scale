"""Extracción de contenido principal de una única página crawleada.

Reutiliza como dependencia de paquete real la lógica de dominio ya construida
y pulida en `html-content-extractor` -- corrección de encoding
(`resolve_encoding`), parseo DOM tolerante (`parse_html`), heurístico de
densidad de texto (`extract_main_content`), extracción de metadatos
(`extract_metadata`) y normalización Unicode (`normalize_text`) --
exactamente el mismo patrón de integración que `crawl/worker.py` ya aplica
con `web-crawler-scheduler` (ver su propio docstring, y
`~/Desarrollo/beacon-search-engine/CLAUDE.md`, sección "Por qué los diez
repos originales no se tocan").

`html_content_extractor.pipeline.ExtractionPipeline` en sí **no** se
reutiliza directamente: está deliberadamente acoplada a procesar un
`pages.jsonl` completo en un único proceso, abriendo y escribiendo dos
ficheros de salida (`documents.jsonl`/`discarded.jsonl`) desde `__enter__` a
`__exit__` -- correcto para ese repo, exactamente igual que
`web_crawler_scheduler.pipeline.CrawlPipeline` era correcto para un solo
proceso antes de que `crawl/worker.py` sustituyera su orquestación por una
distribuida sin tocar su lógica de fetch/robots/enlaces. `extract_single_page`
reimplementa aquí el mismo orden de etapas que
`ExtractionPipeline._process_record` (filtrado por `Content-Type`, resolución
de encoding, parseo, extracción de contenido principal, metadatos,
normalización) llamando exactamente a las mismas funciones públicas, pero
devuelve el resultado en vez de escribirlo a un fichero abierto -- lo que
permite procesar una página a la vez, mensaje a mensaje, en un `ExtractWorker`
que se reparte el trabajo entre varias réplicas sin ningún estado compartido
más allá de la cola (ver `worker.py`).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from html_content_extractor.density import extract_main_content
from html_content_extractor.encoding import resolve_encoding
from html_content_extractor.htmlparse import parse_html
from html_content_extractor.metadata import extract_metadata
from html_content_extractor.models import (
    DiscardedPage,
    DiscardReason,
    ExtractedDocument,
    ExtractionConfig,
)
from html_content_extractor.normalize import normalize_text

logger = logging.getLogger(__name__)

# Mismos prefijos de `Content-Type` que `ExtractionPipeline` descarta antes de
# intentar parsear (ver `html_content_extractor/pipeline.py`).
_NON_HTML_CONTENT_TYPE_PREFIXES = (
    "image/",
    "audio/",
    "video/",
    "application/pdf",
    "application/zip",
    "application/octet-stream",
)


def _resolve_content_type_header(record: dict[str, Any]) -> str | None:
    headers = record.get("headers")
    if isinstance(headers, dict):
        for key, value in headers.items():
            if str(key).lower() == "content-type":
                return str(value)
    content_type = record.get("content_type")
    return str(content_type) if isinstance(content_type, str) else None


def _discard(url: str, reason: DiscardReason, detail: str) -> DiscardedPage:
    return DiscardedPage(url=url, reason=reason, detail=detail, discarded_at=datetime.now(UTC))


def extract_single_page(
    record: dict[str, Any], config: ExtractionConfig
) -> ExtractedDocument | DiscardedPage:
    """Convierte un `CrawledPageRecord` ya deserializado (ver
    `beacon_scale_infra.crawl.models.CrawledPageRecord.to_json_dict`) en un
    `ExtractedDocument`, o en un `DiscardedPage` si la página no produce
    contenido indexable -- nunca lanza por una página individual malformada,
    igual que `ExtractionPipeline._process_record` (ver
    `~/Desarrollo/beacon-search-engine/CLAUDE.md`, sección 2.D)."""
    url = str(record["url"])
    final_url = str(record["final_url"])
    html = str(record["html"])
    depth = int(record.get("depth", 0))
    fetched_at = datetime.fromisoformat(str(record["fetched_at"]))

    content_type_header = _resolve_content_type_header(record)
    if content_type_header is not None:
        mime_type = content_type_header.split(";")[0].strip().lower()
        if mime_type.startswith(_NON_HTML_CONTENT_TYPE_PREFIXES):
            return _discard(url, DiscardReason.NON_HTML_CONTENT, content_type_header)

    resolved_html, encoding_used = resolve_encoding(html, content_type_header)

    try:
        root = parse_html(resolved_html)
        main_content = extract_main_content(root, config)
    except RecursionError as exc:
        logger.warning("Anidamiento de HTML excesivo en %s", url)
        return _discard(url, DiscardReason.PARSE_ERROR, f"recursión excesiva: {exc}")

    if len(main_content.text) < config.min_main_content_chars:
        return _discard(
            url,
            DiscardReason.EMPTY_MAIN_CONTENT,
            "contenido principal insuficiente tras el heurístico de densidad",
        )

    metadata = extract_metadata(root)
    normalized_text = normalize_text(main_content.text)

    return ExtractedDocument(
        url=url,
        final_url=final_url,
        title=metadata.title,
        main_text=normalized_text,
        language=metadata.language,
        author=metadata.author,
        published_at=metadata.published_at,
        fetched_at=fetched_at,
        extracted_at=datetime.now(UTC),
        encoding_used=encoding_used,
        depth=depth,
        word_count=len(normalized_text.split()),
    )
