"""Tests de `DistributedQueryServingPipeline` contra shards HTTP reales
(`distributed_index_sharding.shard_server.create_app`, servidos por
`aiohttp.test_utils.TestServer` -- sockets reales, no un doble en memoria),
registrados en `InMemoryServiceRegistry`: verifica que el fan-out+merge de
`distributed-index-sharding` (sin modificar) sigue funcionando cuando la
lista de `ShardTarget` se resuelve dinámicamente en cada búsqueda en vez de
fijarse al arrancar (ver `ARCHITECTURE.md`, fase 5).

No usa subprocesos aquí (eso lo cubre `test_query_shard_replica_service.py` y,
contra contenedores de verdad, `test_query_docker_shard_failover.py`) -- el
objetivo de este fichero es la lógica de descubrimiento + fan-out + merge en
sí, con sockets reales pero rápidos de arrancar/parar.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest_asyncio
from aiohttp.test_utils import TestServer
from distributed_index_sharding.partitioning import partition_index
from distributed_index_sharding.shard_server import create_app
from inverted_index_builder.pipeline import IndexBuilder
from inverted_index_builder.serialization import write_index

from beacon_scale_infra.errors import QueryServingError
from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.query.pipeline import DistributedQueryServingPipeline
from beacon_scale_infra.registry.local import InMemoryServiceRegistry

_SERVICE_NAME = "beacon-scale-shard"

# doc_id 0..3 (posición de línea, ver inverted-index-builder). Particionado
# doc_id % 2: shard 0 = {0, 2} ("python" x2), shard 1 = {1, 3} ("python" x1) --
# para que una búsqueda real toque ambos shards.
_DOCUMENTS = (
    {"url": "https://example.com/0", "title": "Python Tutorial", "main_text": "python tutorial"},
    {"url": "https://example.com/1", "title": "Recipes", "main_text": "cooking recipes"},
    {
        "url": "https://example.com/2",
        "title": "Python Data",
        "main_text": "python for data science",
    },
    {
        "url": "https://example.com/3",
        "title": "Machine Learning",
        "main_text": "python and machine learning",
    },
)


def _build_two_shard_dirs(tmp_path: Path) -> tuple[Path, Path]:
    documents_path = tmp_path / "documents.jsonl"
    documents_path.write_text(
        "\n".join(_document_line(doc) for doc in _DOCUMENTS) + "\n", encoding="utf-8"
    )
    index = IndexBuilder().build(documents_path)
    source_dir = tmp_path / "source-index"
    write_index(index, source_dir)

    output_root = tmp_path / "shards"
    manifest = partition_index(source_dir, output_root, num_shards=2)
    assert manifest.num_shards == 2
    return output_root / "shard-0", output_root / "shard-1"


def _document_line(document: dict[str, str]) -> str:
    return json.dumps(document, ensure_ascii=False)


@pytest_asyncio.fixture
async def shard_servers(tmp_path: Path) -> AsyncIterator[dict[int, TestServer]]:
    shard0_dir, shard1_dir = _build_two_shard_dirs(tmp_path)
    server0 = TestServer(create_app(0, shard0_dir))
    server1 = TestServer(create_app(1, shard1_dir))
    await server0.start_server()
    await server1.start_server()
    try:
        yield {0: server0, 1: server1}
    finally:
        for server in (server0, server1):
            if not server.closed:
                await server.close()


async def _register(
    registry: InMemoryServiceRegistry, service_id: str, shard_id: int, server: TestServer
) -> None:
    url = server.make_url("")
    assert url.host is not None
    assert url.port is not None
    await registry.register(
        ServiceInstance(
            service_id=service_id,
            service_name=_SERVICE_NAME,
            host=url.host,
            port=url.port,
            metadata={"shard_id": str(shard_id)},
        )
    )


async def test_healthy_search_merges_both_shards(
    shard_servers: dict[int, TestServer],
) -> None:
    registry = InMemoryServiceRegistry()
    await _register(registry, "shard-0-a", 0, shard_servers[0])
    await _register(registry, "shard-1-a", 1, shard_servers[1])

    async with DistributedQueryServingPipeline(registry, service_name=_SERVICE_NAME) as pipeline:
        result = await pipeline.search_text("python", top_k=10)

    assert result.failed_shard_ids == ()
    assert sorted(result.healthy_shard_ids) == [0, 1]
    assert {hit.doc_id for hit in result.merged} == {0, 2, 3}


async def test_search_degrades_when_a_shard_is_unreachable(
    shard_servers: dict[int, TestServer],
) -> None:
    registry = InMemoryServiceRegistry()
    await _register(registry, "shard-0-a", 0, shard_servers[0])
    await _register(registry, "shard-1-a", 1, shard_servers[1])

    # El registro sigue creyendo que shard 0 está vivo (nunca se desregistró
    # ni expiró su TTL) -- exactamente la ventana real entre "un shard cae" y
    # "el registro se entera": la tolerancia a fallo de
    # distributed-index-sharding, no la resolución de shards, es lo que debe
    # cubrir esta ventana.
    await shard_servers[0].close()

    async with DistributedQueryServingPipeline(registry, service_name=_SERVICE_NAME) as pipeline:
        result = await pipeline.search_text("python", top_k=10)

    assert result.failed_shard_ids == (0,)
    assert result.healthy_shard_ids == (1,)
    assert {hit.doc_id for hit in result.merged} == {3}


async def test_failover_to_a_second_live_replica_of_the_same_shard(
    tmp_path: Path, shard_servers: dict[int, TestServer]
) -> None:
    shard0_dir = tmp_path / "shards" / "shard-0"
    replica_server = TestServer(create_app(0, shard0_dir))
    await replica_server.start_server()
    try:
        registry = InMemoryServiceRegistry()
        await _register(registry, "shard-0-replica-a", 0, shard_servers[0])
        await _register(registry, "shard-0-replica-b", 0, replica_server)
        await _register(registry, "shard-1-a", 1, shard_servers[1])

        async with DistributedQueryServingPipeline(
            registry, service_name=_SERVICE_NAME
        ) as pipeline:
            before = await pipeline.search_text("python", top_k=10)
            assert before.failed_shard_ids == ()
            assert {hit.doc_id for hit in before.merged} == {0, 2, 3}

            # Réplica "a" (elegida por ser lexicográficamente menor) cae sin
            # avisar -- se desregistra explícitamente aquí para simular el
            # efecto de una expiración de TTL sin tener que esperarla de
            # verdad en el test.
            await shard_servers[0].close()
            await registry.deregister("shard-0-replica-a")

            after_failover = await pipeline.search_text("python", top_k=10)

        # La partición del shard 0 sigue respondiendo -- a través de la
        # réplica "b", nunca degradada, aunque la "a" original haya muerto.
        assert after_failover.failed_shard_ids == ()
        assert {hit.doc_id for hit in after_failover.merged} == {0, 2, 3}
    finally:
        if not replica_server.closed:
            await replica_server.close()


async def test_all_shard_ids_unregistered_raises_query_serving_error() -> None:
    registry = InMemoryServiceRegistry()

    async with DistributedQueryServingPipeline(registry, service_name=_SERVICE_NAME) as pipeline:
        try:
            await pipeline.search_text("python", top_k=10)
        except QueryServingError:
            pass
        else:
            raise AssertionError("se esperaba QueryServingError sin ninguna réplica registrada")
