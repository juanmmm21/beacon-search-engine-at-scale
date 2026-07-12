"""Tests de integración de `DistributedPageRankPipeline`.

Sobre un grafo en ciclo (a -> b -> c -> a), la distribución estacionaria de
PageRank es exactamente uniforme (`1/N`) para cualquier factor de
amortiguación -- la misma propiedad que `pagerank-link-analysis` ya usa en
sus propios tests (ver su README, "Deterministic, hand-verified
correctness") -- así que el resultado se puede verificar con `math.isclose`
en vez de solo comprobar que "algo" se calculó. También ejercita, con datos
reales aunque pequeños, la parte genuinamente nueva de esta fase: descargar
`search-index/documents.jsonl`, materializar `link_graph.jsonl` desde
`crawl-pages/`, y subir `pagerank-scores/` -- incluyendo una página
malformada y un enlace a una URL no indexada, sin abortar el pipeline (ver
`ARCHITECTURE.md`, fase 4)."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pytest
from pagerank_link_analysis.models import PageRankParams

from beacon_scale_infra.errors import PageRankPhaseError
from beacon_scale_infra.pagerank.models import PageRankPipelineConfig
from beacon_scale_infra.pagerank.pipeline import DistributedPageRankPipeline
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"

_DOCUMENTS: tuple[dict[str, Any], ...] = (
    {"doc_id": 0, "url": "https://example.com/a", "title": "A"},
    {"doc_id": 1, "url": "https://example.com/b", "title": "B"},
    {"doc_id": 2, "url": "https://example.com/c", "title": "C"},
)


def _crawled_page_record(url: str, outlinks: list[str]) -> dict[str, Any]:
    return {
        "url": url,
        "final_url": url,
        "status_code": 200,
        "headers": {},
        "html": "<html></html>",
        "content_type": "text/html",
        "depth": 0,
        "fetched_at": "2026-07-08T09:00:00+00:00",
        "outlinks": outlinks,
        "fetched_by_worker": "worker-0",
    }


async def _write_documents(storage: LocalFilesystemObjectStorage) -> None:
    body = ("\n".join(json.dumps(doc) for doc in _DOCUMENTS) + "\n").encode("utf-8")
    await storage.put_object(
        _BUCKET, "search-index/documents.jsonl", body, content_type="application/jsonl"
    )


async def _read_jsonl(storage: LocalFilesystemObjectStorage, key: str) -> list[dict[str, Any]]:
    raw = await storage.get_object(_BUCKET, key)
    return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]


async def test_cycle_graph_converges_to_the_uniform_distribution(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _write_documents(storage)

    pages = {
        "https://example.com/a": ["https://example.com/b"],
        "https://example.com/b": ["https://example.com/c"],
        "https://example.com/c": ["https://example.com/a"],
    }
    for index, (url, outlinks) in enumerate(pages.items()):
        await storage.put_object(
            _BUCKET,
            f"crawl-pages/page-{index}.json",
            json.dumps(_crawled_page_record(url, outlinks)).encode("utf-8"),
            content_type="application/json",
        )

    config = PageRankPipelineConfig(
        bucket=_BUCKET, pagerank_params=PageRankParams(damping_factor=0.85)
    )
    stats = await DistributedPageRankPipeline(config, storage=storage).run()

    assert stats.pages_materialized == 3
    assert stats.pages_missing == 0
    assert stats.pages_skipped_malformed == 0
    assert stats.total_documents == 3
    assert stats.resolved_edges == 3
    assert stats.dangling_documents == 0
    assert stats.converged

    scores = {
        int(record["doc_id"]): float(record["pagerank_score"])
        for record in await _read_jsonl(storage, "pagerank-scores/pagerank_scores.jsonl")
    }
    assert len(scores) == 3
    for score in scores.values():
        assert math.isclose(score, 1.0 / 3.0, rel_tol=1e-4)

    manifest = json.loads(await storage.get_object(_BUCKET, "pagerank-scores/manifest.json"))
    assert manifest["scores_file"] == "pagerank_scores.jsonl"
    convergence = json.loads(await storage.get_object(_BUCKET, "pagerank-scores/convergence.json"))
    assert convergence["total_documents"] == 3
    assert convergence["damping_factor"] == 0.85


async def test_malformed_page_and_unresolved_link_are_counted_not_fatal(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _write_documents(storage)

    # a enlaza a b (resuelve) y a una URL externa no indexada (no resuelve).
    await storage.put_object(
        _BUCKET,
        "crawl-pages/page-0.json",
        json.dumps(
            _crawled_page_record(
                "https://example.com/a",
                ["https://example.com/b", "https://external.example/nope"],
            )
        ).encode("utf-8"),
        content_type="application/json",
    )
    # Página sin campo "outlinks" -- malformada, debe contarse y no abortar.
    await storage.put_object(
        _BUCKET,
        "crawl-pages/page-1.json",
        json.dumps({"final_url": "https://example.com/b"}).encode("utf-8"),
        content_type="application/json",
    )

    config = PageRankPipelineConfig(bucket=_BUCKET, max_concurrent_reads=1)
    stats = await DistributedPageRankPipeline(config, storage=storage).run()

    assert stats.pages_materialized == 1
    assert stats.pages_skipped_malformed == 1
    assert stats.total_documents == 3
    assert stats.resolved_edges == 1
    assert stats.unresolved_target_links == 1
    # c no aparece en ningún link_graph.jsonl materializado: nodo colgante,
    # sigue recibiendo un score de PageRank vía teletransporte (ver
    # `pagerank_link_analysis.pagerank`, no un hueco en el resultado).
    assert stats.dangling_documents >= 1

    scores = await _read_jsonl(storage, "pagerank-scores/pagerank_scores.jsonl")
    assert {int(record["doc_id"]) for record in scores} == {0, 1, 2}


async def test_missing_search_index_documents_raises_a_typed_error(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    config = PageRankPipelineConfig(bucket=_BUCKET)

    with pytest.raises(PageRankPhaseError):
        await DistributedPageRankPipeline(config, storage=storage).run()
