"""Rastreo del buscador vertical: cada fuente curada se rastrea con
`web-crawler-scheduler` restringido a sus prefijos y a su presupuesto de
páginas, se extrae con la misma función que `extract-worker` y entra en el
índice como altas de la indexación incremental (`incremental/`).

Las fuentes se rastrean a la vez (son dominios distintos: no compiten por el
mismo rate limiter); dentro de cada una manda la cortesía del crawler —
`robots.txt`, `Crawl-delay` y como mucho `max_concurrent_per_domain`
peticiones simultáneas, con al menos `min_delay_seconds` entre ellas.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urldefrag

import aiohttp
from html_content_extractor.models import ExtractedDocument, ExtractionConfig
from web_crawler_scheduler.models import CrawlConfig
from web_crawler_scheduler.pipeline import CrawlPipeline

from beacon_scale_infra.errors import BeaconScaleInfraError
from beacon_scale_infra.extract.page_extractor import extract_single_page
from beacon_scale_infra.incremental.models import IndexUpdate
from beacon_scale_infra.vertical.sources import DocSource, VerticalSources

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "BeaconDocsBot/0.2 (+https://github.com/juanmmm21/beacon-search-engine-at-scale)"
)


class VerticalCrawlError(BeaconScaleInfraError):
    """Fallo al rastrear o extraer una fuente del vertical."""


@dataclass(frozen=True, slots=True)
class VerticalCrawlConfig:
    work_dir: Path
    max_depth: int = 4
    min_delay_seconds: float = 1.0
    max_concurrent_per_domain: int = 2
    request_timeout_seconds: float = 15.0
    user_agent: str = DEFAULT_USER_AGENT
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)

    def __post_init__(self) -> None:
        if self.min_delay_seconds < 0.5:
            # Suelo deliberado: son sitios de terceros que sirven a personas,
            # y este bot no tiene ninguna prisa que justifique más ritmo.
            raise ValueError("min_delay_seconds no puede bajar de 0.5 s en sitios ajenos")
        if self.max_concurrent_per_domain > 4:
            raise ValueError("max_concurrent_per_domain no puede pasar de 4 en sitios ajenos")


@dataclass(frozen=True, slots=True)
class SourceCrawlReport:
    source_id: str
    budget: int
    pages_crawled: int
    urls_discarded: int
    documents: int
    not_indexable: int
    out_of_scope: int


def tag_document(document: ExtractedDocument, source: DocSource) -> dict[str, object]:
    """Documento extraído + su fuente. La URL pierde el fragmento: el crawler
    deduplica sin él, así que `pagina.html#seccion` es la página entera, y
    con el fragmento el resultado enlazaría a una sección cualquiera."""
    record: dict[str, object] = document.to_json_dict()
    record["url"] = urldefrag(document.url).url
    record["final_url"] = urldefrag(document.final_url).url
    record["source_id"] = source.source_id
    return record


def updates_from_pages(
    pages_path: Path, source: DocSource, extraction: ExtractionConfig
) -> tuple[list[IndexUpdate], int, int]:
    """`(altas, páginas no indexables, páginas fuera de ámbito)`. Una página
    que redirige fuera de los prefijos de su fuente no se indexa: su licencia
    no sería la declarada para esa fuente."""
    updates: list[IndexUpdate] = []
    not_indexable = out_of_scope = 0
    if not pages_path.exists():
        return updates, 0, 0
    with pages_path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            if not source.owns(str(record.get("final_url", record["url"]))):
                out_of_scope += 1
                continue
            result = extract_single_page(record, extraction)
            if isinstance(result, ExtractedDocument):
                updates.append(IndexUpdate.upsert(tag_document(result, source)))
            else:
                not_indexable += 1
    return updates, not_indexable, out_of_scope


async def crawl_source(
    source: DocSource,
    budget: int,
    config: VerticalCrawlConfig,
    session: aiohttp.ClientSession,
) -> tuple[list[IndexUpdate], SourceCrawlReport]:
    output = config.work_dir / source.source_id
    # CrawlPipeline añade a sus ficheros (para poder reanudar); cada
    # ejecución del vertical es un rastreo completo, así que se parte de cero.
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    crawl_config = CrawlConfig(
        seed_urls=source.seeds,
        max_pages=budget,
        max_depth=config.max_depth,
        max_concurrent_requests=config.max_concurrent_per_domain,
        max_concurrent_per_domain=config.max_concurrent_per_domain,
        default_min_delay_seconds=config.min_delay_seconds,
        request_timeout_seconds=config.request_timeout_seconds,
        user_agent=config.user_agent,
        obey_robots_txt=True,
        allowed_url_prefixes=source.allowed_prefixes,
    )
    try:
        async with CrawlPipeline(
            crawl_config,
            session,
            pages_output_path=output / "pages.jsonl",
            link_graph_output_path=output / "link_graph.jsonl",
            discarded_output_path=output / "discarded.jsonl",
            checkpoint_path=output / "checkpoint.json",
        ) as pipeline:
            stats = await pipeline.run()
    except (OSError, aiohttp.ClientError) as error:
        raise VerticalCrawlError(f"{source.source_id}: el rastreo falló: {error}") from error
    updates, not_indexable, out_of_scope = await asyncio.to_thread(
        updates_from_pages, output / "pages.jsonl", source, config.extraction
    )
    report = SourceCrawlReport(
        source_id=source.source_id,
        budget=budget,
        pages_crawled=stats.pages_crawled,
        urls_discarded=stats.urls_discarded,
        documents=len(updates),
        not_indexable=not_indexable,
        out_of_scope=out_of_scope,
    )
    logger.info("vertical %s: %s", source.source_id, report)
    return updates, report


async def crawl_vertical(
    sources: VerticalSources, config: VerticalCrawlConfig
) -> tuple[list[IndexUpdate], list[SourceCrawlReport]]:
    budgets = sources.budgets()
    active = [source for source in sources.enabled if budgets[source.source_id] > 0]
    timeout = aiohttp.ClientTimeout(total=config.request_timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = await asyncio.gather(
            *(crawl_source(s, budgets[s.source_id], config, session) for s in active)
        )
    updates = [update for source_updates, _ in results for update in source_updates]
    return updates, [report for _, report in results]
