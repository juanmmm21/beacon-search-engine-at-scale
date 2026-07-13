"""`GET /api/v1/search` -- el mismo pipeline online que la ruta homónima de
`beacon-search-console` (parseo y corrección -> fan-out -> reranking ->
snippets), con las tres diferencias de fase 6: el fan-out va contra el
clúster real de fase 5 (descubrimiento por consulta con verificación de
versión de índice), el resultado completo se sirve/escribe en la caché
compartida cuando la consulta es cacheable, y los snippets se resuelven bajo
demanda contra las particiones de fase 2. Ningún fallo de shard llega como
500: se degrada explícitamente, igual que la consola original."""

from __future__ import annotations

from beacon_search_console.models import (
    QueryCorrectionInfo,
    SearchResponse,
    SearchResultItem,
    ShardStatusInfo,
)
from beacon_search_console.snippets import build_snippet
from fastapi import APIRouter, Depends, Query
from query_parser_autocomplete.pipeline import parse_and_correct_query

from beacon_scale_infra.console.cache import search_cache_key
from beacon_scale_infra.console.cluster_search import ClusterView
from beacon_scale_infra.console.dependencies import ConsoleAppState, get_app_state

router = APIRouter(tags=["search"])

_EMPTY_QUERY_MESSAGE = "Escribe al menos un término para buscar."
_NO_RESULTS_MESSAGE = "No se encontraron resultados para esta búsqueda."
_NO_SEARCHABLE_TERMS_MESSAGE = (
    "La búsqueda no contiene ningún término por el que se pueda buscar "
    "(solo exclusiones o filtros)."
)
_NO_LIVE_SHARDS_MESSAGE = (
    "Ningún nodo del índice está disponible en este momento y no se pudo completar la búsqueda."
)
_NO_LIVE_REPLICAS_ERROR = "sin réplicas vivas en el registro de servicio"


def _version_mismatch_error(state: ConsoleAppState) -> str:
    return (
        "la réplica elegida sirve otra versión del índice distinta de la que cargó esta API "
        f"({state.index_version[:12]}…): reinicia las réplicas desactualizadas"
    )


def _degraded_message(failed_shards: int, total_shards: int, has_results: bool) -> str:
    if has_results:
        return (
            f"Resultados parciales: {failed_shards} de {total_shards} nodos del índice "
            "no respondieron a tiempo."
        )
    return (
        f"El índice está degradado ({failed_shards} de {total_shards} nodos no responden) "
        "y no se pudo completar la búsqueda."
    )


def _compose_shard_statuses(
    state: ConsoleAppState,
    view: ClusterView,
    fan_out_statuses: dict[int, ShardStatusInfo],
) -> tuple[ShardStatusInfo, ...]:
    """Un estado por shard *esperado* (0..num_shards-1, del manifiesto de
    clúster), no solo por shard que respondió: un shard sin ninguna réplica
    viva, o excluido por servir otra versión del índice, aparece como error
    explícito -- la consola original enseñaba la lista fija de su clúster
    local; aquí la lista fija es el nº de particiones del despliegue."""
    statuses: list[ShardStatusInfo] = []
    mismatched = set(view.version_mismatched_shard_ids)
    for shard_id in range(state.num_shards):
        if shard_id in fan_out_statuses:
            statuses.append(fan_out_statuses[shard_id])
        elif shard_id in mismatched:
            statuses.append(
                ShardStatusInfo(
                    shard_id=shard_id,
                    status="error",
                    error_message=_version_mismatch_error(state),
                )
            )
        else:
            statuses.append(
                ShardStatusInfo(
                    shard_id=shard_id, status="error", error_message=_NO_LIVE_REPLICAS_ERROR
                )
            )
    return tuple(statuses)


@router.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query("", description="Texto de la búsqueda, en español o cualquier idioma indexado"),
    limit: int = Query(10, ge=1, le=50),
    state: ConsoleAppState = Depends(get_app_state),
) -> SearchResponse:
    raw_query = q.strip()
    if not raw_query:
        return SearchResponse(
            query=raw_query,
            results=(),
            corrections=(),
            shard_statuses=(),
            degraded=False,
            message=_EMPTY_QUERY_MESSAGE,
        )

    parsed = parse_and_correct_query(raw_query, state.spell_checker)
    corrections = tuple(
        QueryCorrectionInfo(original=c.original, corrected=c.corrected) for c in parsed.corrections
    )

    if parsed.is_empty():
        return SearchResponse(
            query=raw_query,
            results=(),
            corrections=corrections,
            shard_statuses=(),
            degraded=False,
            message=_NO_SEARCHABLE_TERMS_MESSAGE,
        )

    view = await state.cluster.snapshot()

    # La caché solo se consulta cuando la foto del clúster está completa y
    # verificada (todas las particiones vivas, todas anunciando la versión de
    # esta API): así una entrada cacheada siempre equivale a lo que el fan-out
    # habría respondido ahora mismo -- ver ARCHITECTURE.md, fase 6.
    cluster_complete = len(view.targets) == state.num_shards
    cache_key: str | None = None
    if view.cacheable and cluster_complete:
        cache_key = search_cache_key(state.index_version, raw_query, limit)
        cached = await state.cache.get(cache_key)
        if cached is not None:
            return cached

    if not view.targets:
        return SearchResponse(
            query=raw_query,
            results=(),
            corrections=corrections,
            shard_statuses=_compose_shard_statuses(state, view, {}),
            degraded=True,
            message=_NO_LIVE_SHARDS_MESSAGE,
        )

    fetch_limit = limit * state.config.candidate_overfetch_multiplier
    fan_out = await state.cluster.search_parsed_query(
        view, parsed.to_json_dict(), top_k=fetch_limit
    )

    query_terms = list(parsed.required_terms) + [
        term for phrase in parsed.phrases for term in phrase
    ]
    reranked = state.rerank_context.rerank(fan_out.merged, query_terms, top_k=limit)

    results: list[SearchResultItem] = []
    for ranked in reranked:
        document = await state.snippet_resolver.resolve(ranked.doc_id)
        if document is None:
            # Un doc_id que un shard conoce pero que no resuelve contra el
            # catálogo de corpus de esta build indica artefactos mezclados o
            # una parte ilegible (ya registrado por el resolver): se descarta
            # ese resultado y se sigue sirviendo el resto.
            continue
        snippet = build_snippet(document.main_text, query_terms)
        results.append(
            SearchResultItem(
                doc_id=ranked.doc_id,
                url=document.url,
                title=document.title,
                snippet=snippet,
                score=ranked.final_score,
            )
        )

    fan_out_statuses = {
        outcome.shard_id: ShardStatusInfo(
            shard_id=outcome.shard_id, status=outcome.status, error_message=outcome.error_message
        )
        for outcome in fan_out.outcomes
    }
    shard_statuses = _compose_shard_statuses(state, view, fan_out_statuses)
    failed_shards = sum(1 for status in shard_statuses if status.status != "ok")
    degraded = failed_shards > 0

    message: str | None = None
    if degraded:
        message = _degraded_message(failed_shards, state.num_shards, bool(results))
    elif not results:
        message = _NO_RESULTS_MESSAGE

    response = SearchResponse(
        query=raw_query,
        results=tuple(results),
        corrections=corrections,
        shard_statuses=shard_statuses,
        degraded=degraded,
        message=message,
    )
    if cache_key is not None:
        # set() ya descarta por sí mismo las respuestas degradadas.
        await state.cache.set(cache_key, response)
    return response
