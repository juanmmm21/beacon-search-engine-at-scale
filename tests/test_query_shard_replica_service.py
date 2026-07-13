"""Test de extremo a extremo de `ShardReplicaService` con un subproceso real
`distributed-index-sharding serve-shard` (no un doble en memoria) -- mismo
criterio que `distributed_index_sharding/tests/test_cluster_end_to_end.py`
aplica a `LocalShardCluster`, aquí sobre una única réplica que además se
anuncia en un `ServiceRegistry` real (`InMemoryServiceRegistry`) en vez de
depender de una lista fija de `ShardTarget`.

Cubre las dos rutas de apagado documentadas en `shard_replica_service.py`:
apagado *con aviso* (`shutdown()`, desregistro explícito) y caída *sin aviso*
(`kill_process()`, el subproceso muere sin ejecutar ningún código propio --
el registro solo deja de devolver esa réplica cuando su TTL expira). El
equivalente contra un contenedor Docker real (no un subproceso) vive en
`test_query_docker_shard_failover.py`.
"""

from __future__ import annotations

import asyncio
import json
import random
from pathlib import Path

from distributed_index_sharding.partitioning import partition_index
from inverted_index_builder.pipeline import IndexBuilder
from inverted_index_builder.serialization import write_index

from beacon_scale_infra.errors import QueryServingError
from beacon_scale_infra.extract.manifest import PartitionManifestEntry, write_partition_manifest
from beacon_scale_infra.index.models import IndexingPipelineConfig
from beacon_scale_infra.index.pipeline import IndexingPipeline
from beacon_scale_infra.query.models import ShardIndexPipelineConfig, ShardReplicaConfig
from beacon_scale_infra.query.pipeline import DistributedQueryServingPipeline
from beacon_scale_infra.query.shard_discovery import SHARD_ID_METADATA_KEY
from beacon_scale_infra.query.shard_index_pipeline import ShardIndexPipeline
from beacon_scale_infra.query.shard_replica_service import ShardReplicaService
from beacon_scale_infra.registry.local import InMemoryServiceRegistry
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"
_SERVICE_NAME = "beacon-scale-shard"
_DOCUMENTS = (
    {"url": "https://example.com/0", "title": "Python", "main_text": "python tutorial"},
    {"url": "https://example.com/1", "title": "Other", "main_text": "cooking recipes"},
)


async def _seed_shard_index(tmp_path: Path, storage: LocalFilesystemObjectStorage) -> None:
    documents_path = tmp_path / "documents.jsonl"
    documents_path.write_text(
        "\n".join(json.dumps(doc, ensure_ascii=False) for doc in _DOCUMENTS) + "\n",
        encoding="utf-8",
    )
    index = IndexBuilder().build(documents_path)
    source_dir = tmp_path / "source-index"
    write_index(index, source_dir)

    output_root = tmp_path / "shards"
    partition_index(source_dir, output_root, num_shards=1)
    for path in sorted((output_root / "shard-0").iterdir()):
        await storage.put_object(_BUCKET, f"shard-index/shard-0/{path.name}", path.read_bytes())


def _replica_config(*, replica_id: str, port: int, ttl_seconds: float) -> ShardReplicaConfig:
    return ShardReplicaConfig(
        shard_id=0,
        replica_id=replica_id,
        bucket=_BUCKET,
        shard_index_prefix="shard-index",
        service_name=_SERVICE_NAME,
        host="127.0.0.1",
        port=port,
        announce_host="127.0.0.1",
        ttl_seconds=ttl_seconds,
        heartbeat_interval_seconds=ttl_seconds / 4,
        health_check_timeout_seconds=15.0,
    )


async def test_replica_registers_itself_and_answers_a_real_query(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _seed_shard_index(tmp_path, storage)
    registry = InMemoryServiceRegistry()
    config = _replica_config(
        replica_id="replica-a", port=random.randint(20000, 60000), ttl_seconds=15.0
    )

    service = await ShardReplicaService.start(config, storage=storage, registry=registry)
    try:
        discovered = await registry.discover(_SERVICE_NAME)
        assert [i.service_id for i in discovered] == [config.service_id]
        assert discovered[0].metadata[SHARD_ID_METADATA_KEY] == "0"
        assert discovered[0].host == "127.0.0.1"
        assert discovered[0].port == config.port

        async with DistributedQueryServingPipeline(
            registry, service_name=_SERVICE_NAME
        ) as pipeline:
            result = await pipeline.search_text("python", top_k=10)
        assert result.failed_shard_ids == ()
        assert {hit.doc_id for hit in result.merged} == {0}
    finally:
        await service.shutdown()

    # Apagado con aviso: desregistro explícito, sin esperar a ningún TTL.
    assert await registry.discover(_SERVICE_NAME) == []


async def test_ungraceful_kill_is_only_noticed_after_ttl_expiry(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    await _seed_shard_index(tmp_path, storage)
    registry = InMemoryServiceRegistry()
    ttl_seconds = 0.4
    config = _replica_config(
        replica_id="replica-b", port=random.randint(20000, 60000), ttl_seconds=ttl_seconds
    )

    service = await ShardReplicaService.start(config, storage=storage, registry=registry)
    try:
        assert [i.service_id for i in await registry.discover(_SERVICE_NAME)] == [config.service_id]

        # docker kill / SIGKILL real: el proceso muere sin ejecutar
        # shutdown() ni deregister() -- kill_process() reproduce exactamente
        # eso (ver su propio docstring).
        await service.kill_process()

        # Justo tras la caída, el registro todavía no lo sabe: el heartbeat
        # anterior sigue dentro de su ventana de TTL.
        assert [i.service_id for i in await registry.discover(_SERVICE_NAME)] == [config.service_id]

        await asyncio.sleep(ttl_seconds * 2)

        # Pasado el TTL sin heartbeat, InMemoryServiceRegistry deja de
        # devolverla -- sin que ningún código de esta réplica se haya
        # ejecutado para desregistrarla.
        assert await registry.discover(_SERVICE_NAME) == []
    finally:
        # El subproceso ya está muerto (kill_process ya esperó su salida);
        # shutdown() solo limpia el directorio temporal y confirma que
        # desregistrar algo ya ausente es idempotente.
        await service.shutdown()


async def test_missing_shard_data_raises_query_serving_error(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    registry = InMemoryServiceRegistry()
    config = _replica_config(
        replica_id="replica-c", port=random.randint(20000, 60000), ttl_seconds=15.0
    )

    try:
        await ShardReplicaService.start(config, storage=storage, registry=registry)
    except QueryServingError:
        pass
    else:
        raise AssertionError("se esperaba QueryServingError sin datos de shard en el storage")


async def test_shard_index_pipeline_output_is_directly_consumable_by_a_replica(
    tmp_path: Path,
) -> None:
    """Extremo a extremo de fase 5 completa: `ShardIndexPipeline` particiona y
    sube, `ShardReplicaService` descarga exactamente lo que subió y sirve
    consultas reales sobre ello -- sin ningún paso manual entre ambos."""
    storage = LocalFilesystemObjectStorage(tmp_path / "object-storage")
    documents_path = tmp_path / "raw-documents.jsonl"
    documents_path.write_text(
        "\n".join(json.dumps(doc, ensure_ascii=False) for doc in _DOCUMENTS) + "\n",
        encoding="utf-8",
    )
    await storage.put_object(
        _BUCKET,
        "extracted-documents/partition=worker-a/documents-000000.jsonl",
        documents_path.read_bytes(),
    )
    await write_partition_manifest(
        storage,
        _BUCKET,
        "extracted-documents",
        PartitionManifestEntry(
            partition_key="worker-a",
            document_count=len(_DOCUMENTS),
            discarded_count=0,
            part_file_count=1,
        ),
    )
    await IndexingPipeline(
        IndexingPipelineConfig(bucket=_BUCKET, extract_prefix="extracted-documents", compress=True),
        storage=storage,
    ).run()
    await ShardIndexPipeline(
        ShardIndexPipelineConfig(bucket=_BUCKET, num_shards=1), storage=storage
    ).run()

    registry = InMemoryServiceRegistry()
    config = _replica_config(
        replica_id="replica-d", port=random.randint(20000, 60000), ttl_seconds=15.0
    )
    service = await ShardReplicaService.start(config, storage=storage, registry=registry)
    try:
        async with DistributedQueryServingPipeline(
            registry, service_name=_SERVICE_NAME
        ) as pipeline:
            result = await pipeline.search_text("python", top_k=10)
        assert result.failed_shard_ids == ()
        assert {hit.doc_id for hit in result.merged} == {0}
    finally:
        await service.shutdown()
