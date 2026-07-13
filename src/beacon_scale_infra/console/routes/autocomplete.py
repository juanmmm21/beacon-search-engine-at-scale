"""`GET /api/v1/autocomplete` -- delega en `query-parser-autocomplete`
exactamente igual que la ruta homónima de `beacon-search-console` (un prefijo
vacío devuelve las queries más frecuentes, comportamiento documentado de ese
paquete). Sin caché compartida: el índice de autocompletado ya vive en la
memoria de cada réplica y responder desde él es más barato que un round-trip
a Redis."""

from __future__ import annotations

from beacon_search_console.models import AutocompleteResponse, AutocompleteSuggestion
from fastapi import APIRouter, Depends, Query
from query_parser_autocomplete.pipeline import suggest as suggest_completions

from beacon_scale_infra.console.dependencies import ConsoleAppState, get_app_state

router = APIRouter(tags=["autocomplete"])


@router.get("/autocomplete", response_model=AutocompleteResponse)
async def autocomplete(
    q: str = Query("", description="Prefijo ya escrito por el usuario"),
    limit: int = Query(8, ge=1, le=20),
    state: ConsoleAppState = Depends(get_app_state),
) -> AutocompleteResponse:
    prefix = q.strip()
    suggestions = suggest_completions(prefix, state.autocomplete_index, limit)
    return AutocompleteResponse(
        prefix=prefix,
        suggestions=tuple(
            AutocompleteSuggestion(text=suggestion.text, score=suggestion.score)
            for suggestion in suggestions
        ),
    )
