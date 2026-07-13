"""Round-trip de la serialización de `SearchResponse` para la caché
compartida (`console/response_serialization.py`): lo que una réplica escribe
debe reconstruirse idéntico en otra, y una entrada con forma inesperada debe
fallar con una excepción detectable (que `SearchResultCache` captura), nunca
reconstruirse a medias."""

from __future__ import annotations

import pytest
from beacon_search_console.models import (
    HighlightRange,
    QueryCorrectionInfo,
    SearchResponse,
    SearchResultItem,
    ShardStatusInfo,
    Snippet,
)

from beacon_scale_infra.console.response_serialization import (
    search_response_from_json_dict,
    search_response_to_json_dict,
)


def _full_response() -> SearchResponse:
    return SearchResponse(
        query="python tutorial",
        results=(
            SearchResultItem(
                doc_id=3,
                url="https://e.com/3",
                title="Python",
                snippet=Snippet(
                    text="…un tutorial de python…",
                    highlights=(HighlightRange(start=16, end=22),),
                ),
                score=1.25,
            ),
        ),
        corrections=(QueryCorrectionInfo(original="pyton", corrected="python"),),
        shard_statuses=(
            ShardStatusInfo(shard_id=0, status="ok", error_message=None),
            ShardStatusInfo(shard_id=1, status="error", error_message="connection refused"),
        ),
        degraded=False,
        message=None,
    )


def test_full_response_roundtrips() -> None:
    response = _full_response()
    assert search_response_from_json_dict(search_response_to_json_dict(response)) == response


def test_empty_response_roundtrips() -> None:
    response = SearchResponse(
        query="",
        results=(),
        corrections=(),
        shard_statuses=(),
        degraded=False,
        message="Escribe al menos un término para buscar.",
    )
    assert search_response_from_json_dict(search_response_to_json_dict(response)) == response


def test_unexpected_shape_raises_a_detectable_error() -> None:
    with pytest.raises(KeyError):
        search_response_from_json_dict({"query": "x"})
