"""`GET /api/v1/index/stats` -- mismo contrato que la ruta homónima de
`beacon-search-console`: stats del índice global de fase 3, nº de shards del
despliegue de fase 5 y última actualización del crawl (calculada una vez por
`build-index` y transportada en el catálogo de corpus, nunca escaneando el
corpus completo al arrancar cada réplica)."""

from __future__ import annotations

from beacon_search_console.models import IndexStatsResponse
from fastapi import APIRouter, Depends

from beacon_scale_infra.console.dependencies import ConsoleAppState, get_app_state

router = APIRouter(tags=["stats"])


@router.get("/index/stats", response_model=IndexStatsResponse)
async def index_stats(state: ConsoleAppState = Depends(get_app_state)) -> IndexStatsResponse:
    return IndexStatsResponse(
        total_documents=state.global_stats.total_documents,
        vocabulary_size=state.global_stats.vocabulary_size,
        total_postings=state.global_stats.total_postings,
        average_document_length=state.global_stats.average_document_length,
        num_shards=state.num_shards,
        last_crawled_at=state.last_crawled_at,
    )
