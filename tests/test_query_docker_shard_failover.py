"""Test de extremo a extremo de fase 5 contra contenedores Docker *reales* --
no subprocesos, no dobles: levanta este mismo `docker-compose.yml` (MinIO,
Consul, y los servicios `shard-0`/`shard-1`) vía la CLI real de `docker
compose`, construye un índice diminuto de verdad, particiona con `shard-index`
dentro de la propia red de contenedores, mata contenedores de shard de verdad
(`docker kill`) y comprueba que el coordinador degrada -- el mismo criterio
que `distributed-index-sharding/tests/test_cluster_end_to_end.py` ya aplica a
un subproceso matado, aquí contra un contenedor que Docker programó y que ya
no se puede alcanzar en absoluto.

Se salta automáticamente si el daemon de Docker no está disponible, o si los
puertos fijos que `docker-compose.yml` publica (9000, 9001, 6379, 8500, 8600)
ya están ocupados -- señal de que hay otro stack de este mismo proyecto
corriendo en la máquina que este test no debe tocar.

La consulta de prueba se ejecuta *dentro* de la red de Docker Compose (vía
`docker compose run`), nunca desde el proceso de test en el host: cada
réplica de shard se anuncia con su propio hostname corto de contenedor
(`socket.gethostname()`, ver `shard_replica_service.py`), que solo el DNS
embebido de Docker resuelve para otros contenedores de la misma red, nunca
para el host -- así que el cliente de `DistributedQueryServingPipeline` tiene
que vivir dentro de esa misma red para poder conectar con lo que Consul le
devuelve.

Las dos escenas de fallo (una de dos réplicas de shard 0 muere; la única
réplica de shard 1 muere) se ejercitan en secuencia dentro de un único test,
contra un único stack ya levantado -- en vez de un test por escena, cada uno
con su propio `docker compose build` desde cero (el primer build de este
proyecto, con seis dependencias de Git incluyendo paquetes con extensiones
nativas, tarda varios minutos; repetirlo por escena sería puro desperdicio
sin ganar nada en aislamiento, porque la segunda escena no depende del
resultado de la primera más allá del stack seguir vivo).
"""

from __future__ import annotations

import asyncio
import json
import shutil
import socket
import subprocess
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import aiohttp
import pytest
import pytest_asyncio

from beacon_scale_infra.extract.manifest import PartitionManifestEntry, write_partition_manifest
from beacon_scale_infra.index.models import IndexingPipelineConfig
from beacon_scale_infra.index.pipeline import IndexingPipeline
from beacon_scale_infra.storage.s3 import S3ObjectStorage

_COMPOSE_FILE = Path(__file__).resolve().parents[1] / "docker-compose.yml"
_PUBLISHED_PORTS = (9000, 9001, 6379, 8500, 8600)
_BUCKET = "beacon-scale-dev"
_SERVICE_NAME = "beacon-scale-shard"
# El primer 'docker compose run'/'up' de este proyecto en la máquina tiene que
# construir la imagen (git clone + pip install de seis dependencias, alguna
# con extensiones nativas) antes de poder arrancar nada -- varios minutos en
# frío, segundos en caliente. Generoso a propósito para no fallar por lentitud
# de build en vez de por el comportamiento que el test realmente verifica.
_BUILD_TIMEOUT_SECONDS = 480.0

_DOCUMENTS: tuple[dict[str, str], ...] = (
    {"url": "https://example.com/0", "title": "Python", "main_text": "python tutorial basics"},
    {"url": "https://example.com/1", "title": "Cooking", "main_text": "cooking recipes at home"},
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


def _docker_daemon_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10, check=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return False
    return True


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        return probe.connect_ex(("127.0.0.1", port)) != 0


def _all_published_ports_free() -> bool:
    return all(_port_is_free(port) for port in _PUBLISHED_PORTS)


pytestmark = pytest.mark.skipif(
    not _docker_daemon_available(),
    reason="requiere el daemon de Docker corriendo localmente",
)


async def _run_compose(
    project: str, *args: str, timeout: float = 60.0
) -> subprocess.CompletedProcess[bytes]:
    command = ["docker", "compose", "-p", project, "-f", str(_COMPOSE_FILE), *args]
    return await asyncio.to_thread(subprocess.run, command, capture_output=True, timeout=timeout)


async def _compose_container_ids(project: str, service: str) -> list[str]:
    result = await _run_compose(project, "ps", "-q", service)
    return [line for line in result.stdout.decode("utf-8").splitlines() if line.strip()]


async def _wait_for_container_exit_zero(container_id: str, *, timeout_seconds: float) -> None:
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while True:
        inspect_result = await asyncio.to_thread(
            subprocess.run,
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Status}} {{.State.ExitCode}}",
                container_id,
            ],
            capture_output=True,
            timeout=10,
        )
        status, _, exit_code = inspect_result.stdout.decode("utf-8").strip().partition(" ")
        if status == "exited":
            assert exit_code == "0", f"{container_id} salió con código {exit_code}"
            return
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(
                f"{container_id} no terminó en {timeout_seconds}s (status={status!r})"
            )
        await asyncio.sleep(0.5)


async def _consul_passing_instances(session: aiohttp.ClientSession) -> list[dict[str, Any]]:
    async with session.get(
        f"http://localhost:8500/v1/health/service/{_SERVICE_NAME}",
        params={"passing": "true"},
        timeout=aiohttp.ClientTimeout(total=5.0),
    ) as response:
        if response.status != 200:
            return []
        data: list[dict[str, Any]] = await response.json()
        return data


async def _wait_for_passing_count(
    session: aiohttp.ClientSession, expected_count: int, *, timeout_seconds: float
) -> list[dict[str, Any]]:
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while True:
        instances = await _consul_passing_instances(session)
        if len(instances) == expected_count:
            return instances
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(
                f"esperaba {expected_count} instancias 'passing' de {_SERVICE_NAME!r}, "
                f"quedan {len(instances)} tras {timeout_seconds}s: {instances}"
            )
        await asyncio.sleep(1.0)


async def _seed_global_index() -> None:
    """Escribe un índice global diminuto pero real en el MinIO del stack --
    desde el host, vía el puerto publicado, sin ninguna dependencia de la red
    interna de Compose (a diferencia de la consulta, este paso no necesita
    resolver el hostname de ningún contenedor)."""
    storage = S3ObjectStorage(
        endpoint_url="http://localhost:9000",
        access_key="beacon-dev",
        secret_key="beacon-dev-secret",
        region_name="us-east-1",
    )
    try:
        body = ("\n".join(json.dumps(doc, ensure_ascii=False) for doc in _DOCUMENTS) + "\n").encode(
            "utf-8"
        )
        await storage.put_object(
            _BUCKET,
            "extracted-documents/partition=worker-a/documents-000000.jsonl",
            body,
            content_type="application/jsonl",
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
            IndexingPipelineConfig(
                bucket=_BUCKET, extract_prefix="extracted-documents", compress=True
            ),
            storage=storage,
        ).run()
    finally:
        await storage.aclose()


_QUERY_SCRIPT = """
import asyncio, json, sys
from beacon_scale_infra.query.pipeline import DistributedQueryServingPipeline
from beacon_scale_infra.registry.consul import ConsulServiceRegistry

async def main() -> None:
    registry = ConsulServiceRegistry(base_url="http://consul:8500")
    try:
        async with DistributedQueryServingPipeline(
            registry, service_name="beacon-scale-shard", timeout_seconds=5.0
        ) as pipeline:
            result = await pipeline.search_text(sys.argv[1], top_k=10)
        print(json.dumps({
            "merged_doc_ids": sorted(hit.doc_id for hit in result.merged),
            "failed_shard_ids": sorted(result.failed_shard_ids),
            "healthy_shard_ids": sorted(result.healthy_shard_ids),
        }))
    finally:
        await registry.aclose()

asyncio.run(main())
"""


async def _run_query_inside_network(project: str, text: str) -> dict[str, Any]:
    result = await _run_compose(
        project,
        "run",
        "--rm",
        "-T",
        "--no-deps",
        "--entrypoint",
        "python",
        "shard-0",
        "-c",
        _QUERY_SCRIPT,
        text,
        timeout=30.0,
    )
    assert result.returncode == 0, (
        f"la consulta dentro de la red falló: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    last_line = result.stdout.decode("utf-8").strip().splitlines()[-1]
    return dict(json.loads(last_line))


@pytest_asyncio.fixture
async def docker_stack() -> AsyncIterator[str]:
    if not _all_published_ports_free():
        pytest.skip("puertos publicados por docker-compose.yml ya en uso: hay otro stack corriendo")

    project = f"beacon-scale-test-{uuid.uuid4().hex[:8]}"
    try:
        up_result = await _run_compose(
            project, "up", "-d", "minio-init", "consul", timeout=_BUILD_TIMEOUT_SECONDS
        )
        assert up_result.returncode == 0, up_result.stderr.decode("utf-8")

        async with aiohttp.ClientSession() as session:
            deadline = asyncio.get_event_loop().time() + 60.0
            while True:
                try:
                    async with session.get(
                        "http://localhost:8500/v1/status/leader",
                        timeout=aiohttp.ClientTimeout(total=3.0),
                    ) as response:
                        if response.status == 200:
                            break
                except aiohttp.ClientError:
                    pass
                if asyncio.get_event_loop().time() > deadline:
                    raise TimeoutError("consul no respondió a tiempo")
                await asyncio.sleep(1.0)

        # 'docker compose up -d' devuelve el control en cuanto arranca
        # minio-init, no cuando termina de crear el bucket (es un contenedor
        # de un solo uso, sin healthcheck) -- esperar su salida explícitamente
        # en vez de asumir que ya ha corrido evita una carrera con el primer
        # put_object contra un bucket que puede no existir todavía.
        minio_init_ids = await _compose_container_ids(project, "minio-init")
        assert len(minio_init_ids) == 1
        await _wait_for_container_exit_zero(minio_init_ids[0], timeout_seconds=30.0)

        await _seed_global_index()

        # Primer 'run' de este proyecto: construye la imagen 'shard-0' antes
        # de poder ejecutar nada -- ver _BUILD_TIMEOUT_SECONDS.
        shard_index_result = await _run_compose(
            project,
            "run",
            "--rm",
            "-T",
            "--no-deps",
            "--entrypoint",
            "python",
            "shard-0",
            "-m",
            "beacon_scale_infra",
            "shard-index",
            "--bucket",
            _BUCKET,
            "--storage-backend",
            "s3",
            "--num-shards",
            "2",
            timeout=_BUILD_TIMEOUT_SECONDS,
        )
        assert shard_index_result.returncode == 0, shard_index_result.stderr.decode("utf-8")

        up_shards_result = await _run_compose(
            project,
            "up",
            "-d",
            "--scale",
            "shard-0=2",
            "--scale",
            "shard-1=1",
            "shard-0",
            "shard-1",
            timeout=_BUILD_TIMEOUT_SECONDS,
        )
        assert up_shards_result.returncode == 0, up_shards_result.stderr.decode("utf-8")

        async with aiohttp.ClientSession() as session:
            await _wait_for_passing_count(session, 3, timeout_seconds=60.0)

        yield project
    finally:
        await _run_compose(project, "down", "-v", "--remove-orphans", timeout=60.0)


async def test_real_docker_shard_failover_and_degradation(docker_stack: str) -> None:
    project = docker_stack

    # --- Escena 0: sano, ambos shards responden ----------------------------
    baseline = await _run_query_inside_network(project, "python")
    assert baseline["failed_shard_ids"] == []
    assert baseline["healthy_shard_ids"] == [0, 1]
    assert baseline["merged_doc_ids"] == [0, 2, 3]

    # --- Escena 1: muere una de las dos réplicas de shard 0 -> failover ----
    shard0_container_ids = await _compose_container_ids(project, "shard-0")
    assert len(shard0_container_ids) == 2
    kill_result = await asyncio.to_thread(
        subprocess.run, ["docker", "kill", shard0_container_ids[0]], capture_output=True, timeout=15
    )
    assert kill_result.returncode == 0, kill_result.stderr

    # Tras un docker kill real (SIGKILL, sin desregistro), Consul solo dejará
    # de devolver esta réplica cuando su TTL health check expire -- ver
    # ARCHITECTURE.md, fase 5, sección 2. Hasta entonces sigue "passing" en
    # el registro aunque el contenedor ya no exista.
    async with aiohttp.ClientSession() as session:
        await _wait_for_passing_count(session, 2, timeout_seconds=45.0)

    after_shard0_kill = await _run_query_inside_network(project, "python")
    # La partición del shard 0 sigue respondiendo -- a través de su réplica
    # superviviente, sin que el coordinador degrade en absoluto.
    assert after_shard0_kill["failed_shard_ids"] == []
    assert after_shard0_kill["healthy_shard_ids"] == [0, 1]
    assert after_shard0_kill["merged_doc_ids"] == [0, 2, 3]

    # --- Escena 2: muere la única réplica de shard 1 -> degrada sin lanzar -
    shard1_container_ids = await _compose_container_ids(project, "shard-1")
    assert len(shard1_container_ids) == 1
    kill_result = await asyncio.to_thread(
        subprocess.run, ["docker", "kill", shard1_container_ids[0]], capture_output=True, timeout=15
    )
    assert kill_result.returncode == 0, kill_result.stderr

    async with aiohttp.ClientSession() as session:
        await _wait_for_passing_count(session, 1, timeout_seconds=45.0)

    after_shard1_kill = await _run_query_inside_network(project, "python")
    # Ninguna réplica viva de shard 1: resolve_shard_targets simplemente lo
    # omite (ver ARCHITECTURE.md, fase 5, sección 0) -- el coordinador no
    # levanta ninguna excepción, solo responde con lo que shard 0 tiene.
    assert after_shard1_kill["failed_shard_ids"] == []
    assert after_shard1_kill["healthy_shard_ids"] == [0]
    assert after_shard1_kill["merged_doc_ids"] == [0, 2]
