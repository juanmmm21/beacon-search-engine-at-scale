"""Serialización JSON explícita de `SearchResponse` (el contrato `/api/v1`
de `beacon-search-console`, reutilizado como dependencia de paquete) para la
caché compartida de resultados.

Conversores explícitos campo a campo, no `dataclasses.asdict` + construcción
dinámica: una entrada de caché escrita por una réplica con una versión del
contrato y leída por otra con un contrato distinto debe fallar de forma
detectable (KeyError/TypeError capturados por `console/cache.py`), nunca
reconstruirse a medias en silencio. El formato serializado es exactamente el
mismo JSON que la API responde -- no existe un segundo esquema que mantener.
"""

from __future__ import annotations

from typing import Any

from beacon_search_console.models import (
    HighlightRange,
    QueryCorrectionInfo,
    SearchResponse,
    SearchResultItem,
    ShardStatusInfo,
    Snippet,
)


def search_response_to_json_dict(response: SearchResponse) -> dict[str, Any]:
    return {
        "query": response.query,
        "results": [
            {
                "doc_id": item.doc_id,
                "url": item.url,
                "title": item.title,
                "snippet": {
                    "text": item.snippet.text,
                    "highlights": [
                        {"start": highlight.start, "end": highlight.end}
                        for highlight in item.snippet.highlights
                    ],
                },
                "score": item.score,
            }
            for item in response.results
        ],
        "corrections": [
            {"original": correction.original, "corrected": correction.corrected}
            for correction in response.corrections
        ],
        "shard_statuses": [
            {
                "shard_id": status.shard_id,
                "status": status.status,
                "error_message": status.error_message,
            }
            for status in response.shard_statuses
        ],
        "degraded": response.degraded,
        "message": response.message,
    }


def search_response_from_json_dict(data: dict[str, Any]) -> SearchResponse:
    return SearchResponse(
        query=str(data["query"]),
        results=tuple(
            SearchResultItem(
                doc_id=int(item["doc_id"]),
                url=str(item["url"]),
                title=str(item["title"]),
                snippet=Snippet(
                    text=str(item["snippet"]["text"]),
                    highlights=tuple(
                        HighlightRange(start=int(highlight["start"]), end=int(highlight["end"]))
                        for highlight in item["snippet"]["highlights"]
                    ),
                ),
                score=float(item["score"]),
            )
            for item in data["results"]
        ),
        corrections=tuple(
            QueryCorrectionInfo(
                original=str(correction["original"]), corrected=str(correction["corrected"])
            )
            for correction in data["corrections"]
        ),
        shard_statuses=tuple(
            ShardStatusInfo(
                shard_id=int(status["shard_id"]),
                status=str(status["status"]),
                error_message=(
                    None if status["error_message"] is None else str(status["error_message"])
                ),
            )
            for status in data["shard_statuses"]
        ),
        degraded=bool(data["degraded"]),
        message=None if data["message"] is None else str(data["message"]),
    )
