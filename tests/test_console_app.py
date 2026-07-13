"""Tests end-to-end de la API de la consola (fase 6) sobre artefactos reales:
el corpus particionado de fase 2 se indexa con `IndexingPipeline` (fase 3),
se particiona con `ShardIndexPipeline` (fase 5), el modelo LTR se entrena y
publica con `RerankerTrainingPipeline`, y los shards se sirven con servidores
HTTP reales de `distributed-index-sharding`
(`aiohttp.test_utils.TestServer`) registrados en `InMemoryServiceRegistry` --
la misma pirámide de realismo que `test_query_pipeline.py`, extendida hasta
el contrato `/api/v1` completo servido por ASGI (httpx), incluyendo la caché
compartida de resultados y la verificación de versión de índice por consulta.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestServer
from distributed_index_sharding.shard_server import create_app as create_shard_app
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pagerank_link_analysis.models import ConvergenceInfo, GraphBuildStats, PageRankScore
from pagerank_link_analysis.scores_io import write_pagerank_output

from beacon_scale_infra.cache.memory import InMemoryCacheStore
from beacon_scale_infra.console.config import ConsoleAppConfig
from beacon_scale_infra.console.dependencies import ConsoleAppState
from beacon_scale_infra.console.reranker_job import (
    RerankerTrainingConfig,
    RerankerTrainingPipeline,
)
from beacon_scale_infra.console.routes import autocomplete, search, stats
from beacon_scale_infra.extract.manifest import PartitionManifestEntry, write_partition_manifest
from beacon_scale_infra.index.index_version import parse_index_version_marker
from beacon_scale_infra.index.models import IndexingPipelineConfig
from beacon_scale_infra.index.pipeline import IndexingPipeline
from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.query.models import ShardIndexPipelineConfig
from beacon_scale_infra.query.shard_index_pipeline import ShardIndexPipeline
from beacon_scale_infra.registry.local import InMemoryServiceRegistry
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_SERVICE_NAME = "beacon-scale-shard"
_NUM_SHARDS = 2

# Seis documentos con "python" repartido entre ambos shards (doc_id % 2) y
# fetched_at creciente para verificar last_crawled_at en /index/stats.
_DOCUMENTS = (
    {"url": "https://e.com/0", "title": "Python Tutorial", "main_text": "python tutorial basics"},
    {"url": "https://e.com/1", "title": "Recetas", "main_text": "recetas de cocina caseras"},
    {"url": "https://e.com/2", "title": "Python Data", "main_text": "python for data science"},
    {"url": "https://e.com/3", "title": "Jardinería", "main_text": "cocina y jardín en casa"},
    {"url": "https://e.com/4", "title": "Async Python", "main_text": "async python patterns"},
    {"url": "https://e.com/5", "title": "Testing", "main_text": "testing python code well"},
)
_LAST_FETCHED_AT = "2026-07-06T10:00:00+00:00"

# partition_key -> ficheros de parte -> índices en _DOCUMENTS
_PARTITION_LAYOUT: dict[str, list[list[int]]] = {
    "worker-a": [[0, 1], [2]],
    "worker-b": [[3, 4], [5]],
}


def _document_line(index: int) -> str:
    document = dict(_DOCUMENTS[index])
    document["fetched_at"] = (
        _LAST_FETCHED_AT if index == 5 else f"2026-07-0{index + 1}T10:00:00+00:00"
    )
    return json.dumps(document, ensure_ascii=False)


async def _build_all_artifacts(root: Path) -> None:
    storage = LocalFilesystemObjectStorage(root)
    for partition_key, part_files in _PARTITION_LAYOUT.items():
        document_count = 0
        for part_seq, doc_indexes in enumerate(part_files):
            body = ("\n".join(_document_line(i) for i in doc_indexes) + "\n").encode("utf-8")
            key = f"extracted-documents/partition={partition_key}/documents-{part_seq:06d}.jsonl"
            await storage.put_object(_BUCKET, key, body, content_type="application/jsonl")
            document_count += len(doc_indexes)
        await write_partition_manifest(
            storage,
            _BUCKET,
            "extracted-documents",
            PartitionManifestEntry(
                partition_key=partition_key,
                document_count=document_count,
                discarded_count=0,
                part_file_count=len(part_files),
            ),
        )

    await IndexingPipeline(
        IndexingPipelineConfig(bucket=_BUCKET, extract_prefix="extracted-documents"),
        storage=storage,
    ).run()
    await ShardIndexPipeline(
        ShardIndexPipelineConfig(bucket=_BUCKET, num_shards=_NUM_SHARDS), storage=storage
    ).run()
    await RerankerTrainingPipeline(
        RerankerTrainingConfig(bucket=_BUCKET, num_queries=10, candidates_per_query=5, seed=7),
        storage=storage,
    ).run()
    await _publish_pagerank_scores(storage, root)


async def _publish_pagerank_scores(storage: LocalFilesystemObjectStorage, root: Path) -> None:
    """Publica scores de PageRank en el formato real (el propio
    `write_pagerank_output` de `pagerank-link-analysis`, nunca un formato
    escrito a mano en el test) -- la fase 4 completa (materializar el grafo
    desde crawl-pages/) ya tiene su propia suite; aquí solo hace falta su
    artefacto de salida."""
    scores = tuple(
        PageRankScore(doc_id=doc_id, score=1.0 / len(_DOCUMENTS))
        for doc_id in range(len(_DOCUMENTS))
    )
    convergence = ConvergenceInfo(
        damping_factor=0.85,
        tolerance=1e-6,
        max_iterations=100,
        iterations_run=1,
        converged=True,
        final_delta=0.0,
        elapsed_seconds=0.0,
    )
    graph_stats = GraphBuildStats(
        total_documents=len(_DOCUMENTS),
        resolved_edges=0,
        dangling_documents=len(_DOCUMENTS),
        unresolved_source_entries=0,
        unresolved_target_links=0,
    )
    local_dir = root / "pagerank-local"
    write_pagerank_output(local_dir, scores, convergence, graph_stats)
    for path in sorted(local_dir.iterdir()):
        if path.is_file():
            await storage.put_object(_BUCKET, f"pagerank-scores/{path.name}", path.read_bytes())


@pytest.fixture(scope="module")
def artifacts_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Los jobs por lotes (indexar, particionar, entrenar) corren una única
    vez por módulo: son deterministas y ningún test los muta. asyncio.run en
    una fixture síncrona para no atar la fixture de módulo al event loop por
    función de pytest-asyncio."""
    root = tmp_path_factory.mktemp("console-object-storage")
    asyncio.run(_build_all_artifacts(root))
    return root


async def _download_shard_dir(
    storage: LocalFilesystemObjectStorage, shard_id: int, destination: Path
) -> Path:
    prefix = f"shard-index/shard-{shard_id}"
    destination.mkdir(parents=True, exist_ok=True)
    async for entry in storage.list_objects(_BUCKET, prefix=f"{prefix}/"):
        relative_name = entry.key[len(prefix) + 1 :]
        if not relative_name or "/" in relative_name:
            continue
        (destination / relative_name).write_bytes(await storage.get_object(_BUCKET, entry.key))
    return destination


@dataclass(slots=True)
class ConsoleHarness:
    storage: LocalFilesystemObjectStorage
    registry: InMemoryServiceRegistry
    cache_store: InMemoryCacheStore
    state: ConsoleAppState
    client: AsyncClient
    shard_servers: dict[int, TestServer]
    index_version: str

    async def register_replica(
        self, service_id: str, shard_id: int, server: TestServer, *, index_version: str | None
    ) -> None:
        url = server.make_url("")
        assert url.host is not None and url.port is not None
        metadata = {"shard_id": str(shard_id)}
        if index_version is not None:
            metadata["index_version"] = index_version
        await self.registry.register(
            ServiceInstance(
                service_id=service_id,
                service_name=_SERVICE_NAME,
                host=url.host,
                port=url.port,
                metadata=metadata,
            )
        )

    async def close_all_shards(self) -> None:
        for server in self.shard_servers.values():
            if not server.closed:
                await server.close()


@pytest_asyncio.fixture
async def harness(artifacts_root: Path, tmp_path: Path) -> AsyncIterator[ConsoleHarness]:
    storage = LocalFilesystemObjectStorage(artifacts_root)
    index_version = parse_index_version_marker(
        await storage.get_object(_BUCKET, "shard-index/index_version.json")
    )

    shard_servers: dict[int, TestServer] = {}
    for shard_id in range(_NUM_SHARDS):
        shard_dir = await _download_shard_dir(storage, shard_id, tmp_path / f"shard-{shard_id}")
        server = TestServer(create_shard_app(shard_id, shard_dir))
        await server.start_server()
        shard_servers[shard_id] = server

    registry = InMemoryServiceRegistry()
    cache_store = InMemoryCacheStore()
    config = ConsoleAppConfig(bucket=_BUCKET, artifacts_dir=tmp_path / "artifacts")
    state = await ConsoleAppState.build(
        config, storage=storage, registry=registry, cache_store=cache_store
    )

    app = FastAPI()
    app.include_router(search.router, prefix="/api/v1")
    app.include_router(autocomplete.router, prefix="/api/v1")
    app.include_router(stats.router, prefix="/api/v1")
    app.state.console_state = state

    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://console")
    harness = ConsoleHarness(
        storage=storage,
        registry=registry,
        cache_store=cache_store,
        state=state,
        client=client,
        shard_servers=shard_servers,
        index_version=index_version,
    )
    try:
        yield harness
    finally:
        await client.aclose()
        await state.close()
        await harness.close_all_shards()


async def _register_healthy_cluster(harness: ConsoleHarness) -> None:
    for shard_id, server in harness.shard_servers.items():
        await harness.register_replica(
            f"replica-shard-{shard_id}", shard_id, server, index_version=harness.index_version
        )


async def test_search_returns_reranked_results_with_real_snippets(
    harness: ConsoleHarness,
) -> None:
    await _register_healthy_cluster(harness)

    response = await harness.client.get("/api/v1/search", params={"q": "python", "limit": 10})
    assert response.status_code == 200
    body = response.json()

    assert body["degraded"] is False
    assert body["message"] is None
    assert {status["status"] for status in body["shard_statuses"]} == {"ok"}
    assert len(body["shard_statuses"]) == _NUM_SHARDS

    urls = {result["url"] for result in body["results"]}
    # Todos los documentos que contienen "python" viven repartidos entre los
    # dos shards: que aparezcan todos demuestra fan-out + merge + reranking +
    # snippets funcionando de extremo a extremo.
    assert urls == {"https://e.com/0", "https://e.com/2", "https://e.com/4", "https://e.com/5"}
    for result in body["results"]:
        assert "python" in result["snippet"]["text"].lower()
        assert result["snippet"]["highlights"], "todo resultado debe llevar resaltado real"
        assert result["title"]


async def test_cache_hit_survives_the_whole_cluster_dying(harness: ConsoleHarness) -> None:
    await _register_healthy_cluster(harness)

    first = await harness.client.get("/api/v1/search", params={"q": "python", "limit": 5})
    assert first.status_code == 200
    assert first.json()["degraded"] is False
    assert len(harness.cache_store) == 1

    # Ambos shards mueren sin desregistrarse (la ventana TTL): la misma
    # búsqueda se sirve desde la caché compartida, byte a byte igual...
    await harness.close_all_shards()
    cached = await harness.client.get("/api/v1/search", params={"q": "python", "limit": 5})
    assert cached.status_code == 200
    assert cached.json() == first.json()

    # ...y una búsqueda distinta (sin entrada en caché) degrada de verdad.
    fresh = await harness.client.get("/api/v1/search", params={"q": "cocina", "limit": 5})
    assert fresh.status_code == 200
    assert fresh.json()["degraded"] is True


async def test_degraded_responses_are_never_cached(harness: ConsoleHarness) -> None:
    await _register_healthy_cluster(harness)
    await harness.shard_servers[0].close()

    degraded = await harness.client.get("/api/v1/search", params={"q": "python", "limit": 5})
    assert degraded.json()["degraded"] is True
    assert len(harness.cache_store) == 0


async def test_replica_without_announced_version_disables_caching(
    harness: ConsoleHarness,
) -> None:
    # "aaa-" ordena antes que "replica-": la réplica sin versión anunciada es
    # la elegida para el shard 0.
    await _register_healthy_cluster(harness)
    await harness.register_replica(
        "aaa-legacy-shard-0", 0, harness.shard_servers[0], index_version=None
    )

    response = await harness.client.get("/api/v1/search", params={"q": "python", "limit": 5})
    assert response.status_code == 200
    body = response.json()
    # La búsqueda se sirve igual (mismo comportamiento que la consola
    # original, que no verificaba versión)...
    assert body["degraded"] is False
    assert body["results"]
    # ...pero sin versión verificada no se escribe nada en la caché.
    assert len(harness.cache_store) == 0


async def test_replica_serving_another_index_version_is_excluded_and_degrades(
    harness: ConsoleHarness,
) -> None:
    await _register_healthy_cluster(harness)
    await harness.register_replica(
        "aaa-stale-shard-0", 0, harness.shard_servers[0], index_version="otra-version"
    )

    response = await harness.client.get("/api/v1/search", params={"q": "python", "limit": 10})
    assert response.status_code == 200
    body = response.json()

    assert body["degraded"] is True
    statuses = {status["shard_id"]: status for status in body["shard_statuses"]}
    assert statuses[0]["status"] == "error"
    assert "versión" in statuses[0]["error_message"]
    assert statuses[1]["status"] == "ok"
    # Solo los documentos del shard 1 (doc_id impares) pueden aparecer.
    assert {result["doc_id"] % 2 for result in body["results"]} == {1}
    assert len(harness.cache_store) == 0


async def test_partition_with_no_live_replicas_is_reported_explicitly(
    harness: ConsoleHarness,
) -> None:
    await harness.register_replica(
        "replica-shard-0", 0, harness.shard_servers[0], index_version=harness.index_version
    )

    response = await harness.client.get("/api/v1/search", params={"q": "python", "limit": 10})
    body = response.json()

    assert body["degraded"] is True
    statuses = {status["shard_id"]: status for status in body["shard_statuses"]}
    assert statuses[1]["status"] == "error"
    assert "réplicas vivas" in statuses[1]["error_message"]
    assert body["results"], "los shards sanos siguen sirviendo resultados"


async def test_no_live_shards_at_all_degrades_without_raising(harness: ConsoleHarness) -> None:
    response = await harness.client.get("/api/v1/search", params={"q": "python"})
    body = response.json()
    assert response.status_code == 200
    assert body["degraded"] is True
    assert body["results"] == []
    assert len(body["shard_statuses"]) == _NUM_SHARDS


async def test_empty_query_returns_a_message_without_touching_the_cluster(
    harness: ConsoleHarness,
) -> None:
    response = await harness.client.get("/api/v1/search", params={"q": "   "})
    body = response.json()
    assert body["degraded"] is False
    assert body["results"] == []
    assert body["message"]


async def test_autocomplete_suggests_from_the_global_vocabulary(
    harness: ConsoleHarness,
) -> None:
    response = await harness.client.get("/api/v1/autocomplete", params={"q": "py"})
    assert response.status_code == 200
    suggestions = [suggestion["text"] for suggestion in response.json()["suggestions"]]
    assert any("python" in suggestion for suggestion in suggestions)


async def test_index_stats_reflect_the_deployed_build(harness: ConsoleHarness) -> None:
    response = await harness.client.get("/api/v1/index/stats")
    assert response.status_code == 200
    body = response.json()
    assert body["total_documents"] == len(_DOCUMENTS)
    assert body["num_shards"] == _NUM_SHARDS
    assert body["last_crawled_at"] == _LAST_FETCHED_AT
    assert body["vocabulary_size"] > 0
