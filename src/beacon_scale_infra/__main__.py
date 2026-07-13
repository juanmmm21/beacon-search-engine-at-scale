"""CLI de `beacon-search-engine-at-scale`.

Fase 0 (`storage-demo`, `queue-demo`, `registry-demo`): ejercita las tres
piezas de sustrato compartido contra su implementación local de desarrollo o
contra los servicios reales levantados por `docker-compose.yml` (MinIO,
Redis, Consul), seleccionable con `--backend`.

Fase 1 (`crawl-worker`): arranca un worker de crawl distribuido (bloqueante,
pensado para correr como servicio de `docker-compose`, escalable a N
réplicas -- ver README, sección "Lanzar varios workers").

Fase 2 (`extract-worker`): arranca un worker de extracción distribuido
(bloqueante, mismo patrón de servicio escalable que `crawl-worker`, pero sin
`--coordination-backend`: la extracción no necesita deduplicador ni rate
limiter compartidos, ver `extract/worker.py`).

Fase 3 (`build-index`): ejecuta el pipeline de indexación distribuida una
única vez (job por lotes, no un servicio escalable a réplicas como los dos
comandos anteriores -- ver `ARCHITECTURE.md`, fase 3, sección 0), después de
que la fase 2 haya terminado de escribir todas sus particiones.

Fase 4 (`compute-pagerank`): ejecuta el pipeline de PageRank distribuido una
única vez (job por lotes, mismo criterio que `build-index` -- ver
`ARCHITECTURE.md`, fase 4, sección 0), después de que `build-index` haya
dejado `search-index/documents.jsonl` listo.

Fase 5 (`shard-index`, `shard-replica`, `search`): particiona el índice
global de fase 3 en shards (`shard-index`, job por lotes), sirve una réplica
escalable de un shard descubierta dinámicamente por el registro de servicio
(`shard-replica`, servicio de larga duración) y lanza una consulta distribuida
contra los shards que estén vivos en cada momento (`search`) -- ver
`ARCHITECTURE.md`, fase 5.

Fase 6 (`train-reranker`, `serve-console`): entrena y publica el modelo LTR
en el almacenamiento de objetos (`train-reranker`, job por lotes) y arranca
una réplica de la API de la consola sobre el clúster de fase 5
(`serve-console`, servicio de larga duración, escalable a N réplicas tras un
balanceador) -- ver `ARCHITECTURE.md`, fase 6.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import socket
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import aiohttp
import redis.asyncio as redis_asyncio
import uvicorn
from distributed_index_sharding.models import FanOutResult
from html_content_extractor.models import ExtractionConfig
from pagerank_link_analysis.models import PageRankParams
from web_crawler_scheduler.fetcher import AiohttpFetcher
from web_crawler_scheduler.robots import RobotsCache

from beacon_scale_infra.console.reranker_job import (
    RerankerTrainingConfig,
    RerankerTrainingPipeline,
)
from beacon_scale_infra.crawl.dedup import InMemorySharedDeduplicator, RedisSharedDeduplicator
from beacon_scale_infra.crawl.models import CrawlWorkerConfig
from beacon_scale_infra.crawl.rate_limiter import (
    CoordinatedRateLimiter,
    InMemoryCoordinatedRateLimiter,
    RedisCoordinatedRateLimiter,
)
from beacon_scale_infra.crawl.worker import CrawlWorker
from beacon_scale_infra.errors import BeaconScaleInfraError
from beacon_scale_infra.extract.models import ExtractWorkerConfig
from beacon_scale_infra.extract.worker import ExtractWorker
from beacon_scale_infra.index.models import IndexingPipelineConfig
from beacon_scale_infra.index.pipeline import IndexingPipeline
from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.pagerank.models import PageRankPipelineConfig
from beacon_scale_infra.pagerank.pipeline import DistributedPageRankPipeline
from beacon_scale_infra.protocols import MessageQueue, ObjectStorage, ServiceRegistry
from beacon_scale_infra.query.models import ShardIndexPipelineConfig, ShardReplicaConfig
from beacon_scale_infra.query.pipeline import DistributedQueryServingPipeline
from beacon_scale_infra.query.shard_index_pipeline import ShardIndexPipeline
from beacon_scale_infra.query.shard_replica_service import ShardReplicaService
from beacon_scale_infra.queue.memory import InMemoryMessageQueue
from beacon_scale_infra.queue.redis_streams import RedisStreamsMessageQueue
from beacon_scale_infra.registry.consul import ConsulServiceRegistry
from beacon_scale_infra.registry.local import InMemoryServiceRegistry
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage
from beacon_scale_infra.storage.s3 import S3ObjectStorage


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="beacon-scale-infra",
        description=(
            "Demuestra el sustrato compartido de beacon-search-engine-at-scale: "
            "almacenamiento de objetos, cola de mensajes y registro de servicio."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    storage_parser = subparsers.add_parser(
        "storage-demo", help="Escribe, lee, lista y borra un objeto de demostración"
    )
    storage_parser.add_argument("--backend", choices=["local", "s3"], default="local")
    storage_parser.add_argument("--bucket", default="beacon-scale-dev")
    storage_parser.add_argument("--local-root", type=Path, default=Path(".local-object-storage"))

    queue_parser = subparsers.add_parser(
        "queue-demo", help="Publica y consume un mensaje de demostración"
    )
    queue_parser.add_argument("--backend", choices=["memory", "redis"], default="memory")
    queue_parser.add_argument("--stream", default="beacon-scale-dev-stream")
    queue_parser.add_argument("--group", default="beacon-scale-dev-group")

    registry_parser = subparsers.add_parser(
        "registry-demo",
        help="Registra, renueva, descubre y da de baja una instancia de servicio de demostración",
    )
    registry_parser.add_argument("--backend", choices=["local", "consul"], default="local")
    registry_parser.add_argument("--service-name", default="beacon-scale-demo-shard")

    crawl_worker_parser = subparsers.add_parser(
        "crawl-worker",
        help="Arranca un worker de crawl distribuido que consume la frontera compartida",
    )
    crawl_worker_parser.add_argument(
        "--worker-id",
        default=None,
        help="Identificador de este worker (por defecto, el hostname del contenedor -- "
        "distinto por réplica bajo 'docker compose up --scale')",
    )
    crawl_worker_parser.add_argument(
        "--seed",
        action="append",
        dest="seed_urls",
        default=[],
        help="URL semilla (repetible); si se omite, se lee de BEACON_CRAWL_SEED_URLS "
        "(lista separada por comas)",
    )
    crawl_worker_parser.add_argument(
        "--queue-backend", choices=["memory", "redis"], default="redis"
    )
    crawl_worker_parser.add_argument("--storage-backend", choices=["local", "s3"], default="s3")
    crawl_worker_parser.add_argument(
        "--coordination-backend",
        choices=["memory", "redis"],
        default="redis",
        help="Backend de deduplicación y rate limiting compartidos -- 'memory' solo tiene "
        "sentido para una demo de un único worker sin Docker, nunca coordina entre procesos",
    )
    crawl_worker_parser.add_argument("--bucket", default="beacon-scale-dev")
    crawl_worker_parser.add_argument("--object-key-prefix", default="crawl-pages")
    crawl_worker_parser.add_argument("--num-hash-shards", type=int, default=16)
    crawl_worker_parser.add_argument("--stream", default="beacon-scale-crawl-frontier")
    crawl_worker_parser.add_argument("--group", default="beacon-scale-crawl-workers")
    crawl_worker_parser.add_argument("--max-depth", type=int, default=3)
    crawl_worker_parser.add_argument("--max-pages", type=int, default=None)
    crawl_worker_parser.add_argument("--max-concurrent-per-domain", type=int, default=2)
    crawl_worker_parser.add_argument("--min-delay-seconds", type=float, default=1.0)
    crawl_worker_parser.add_argument("--request-timeout-seconds", type=float, default=15.0)
    crawl_worker_parser.add_argument("--max-retries", type=int, default=3)
    crawl_worker_parser.add_argument(
        "--idle-polls-before-shutdown",
        type=int,
        default=6,
        help="Sondeos consecutivos sin trabajo nuevo antes de detenerse",
    )
    crawl_worker_parser.add_argument(
        "--no-idle-shutdown",
        action="store_true",
        help="No detenerse nunca al quedarse sin trabajo (servicio de larga duración)",
    )
    crawl_worker_parser.add_argument(
        "--obey-robots-txt", action=argparse.BooleanOptionalAction, default=True
    )
    crawl_worker_parser.add_argument(
        "--local-storage-root", type=Path, default=Path(".local-object-storage")
    )

    extract_worker_parser = subparsers.add_parser(
        "extract-worker",
        help="Arranca un worker de extracción distribuido que consume páginas crawleadas",
    )
    extract_worker_parser.add_argument(
        "--worker-id",
        default=None,
        help="Identificador de este worker, y clave de su partición de documentos extraídos "
        "(por defecto, el hostname del contenedor -- distinto por réplica bajo "
        "'docker compose up --scale')",
    )
    extract_worker_parser.add_argument(
        "--queue-backend", choices=["memory", "redis"], default="redis"
    )
    extract_worker_parser.add_argument("--storage-backend", choices=["local", "s3"], default="s3")
    extract_worker_parser.add_argument("--bucket", default="beacon-scale-dev")
    extract_worker_parser.add_argument("--object-key-prefix", default="extracted-documents")
    extract_worker_parser.add_argument("--stream", default="beacon-scale-extract-frontier")
    extract_worker_parser.add_argument("--group", default="beacon-scale-extract-workers")
    extract_worker_parser.add_argument("--min-main-content-chars", type=int, default=200)
    extract_worker_parser.add_argument("--min-block-text-chars", type=int, default=25)
    extract_worker_parser.add_argument("--link-density-threshold", type=float, default=0.5)
    extract_worker_parser.add_argument("--flush-every-pages", type=int, default=50)
    extract_worker_parser.add_argument("--max-pages", type=int, default=None)
    extract_worker_parser.add_argument(
        "--idle-polls-before-shutdown",
        type=int,
        default=6,
        help="Sondeos consecutivos sin trabajo nuevo antes de detenerse",
    )
    extract_worker_parser.add_argument(
        "--no-idle-shutdown",
        action="store_true",
        help="No detenerse nunca al quedarse sin trabajo (servicio de larga duración)",
    )
    extract_worker_parser.add_argument(
        "--local-storage-root", type=Path, default=Path(".local-object-storage")
    )

    build_index_parser = subparsers.add_parser(
        "build-index",
        help="Ejecuta el pipeline de indexación distribuida (map-reduce) sobre el corpus "
        "particionado que dejó la fase 2, una única vez",
    )
    build_index_parser.add_argument("--storage-backend", choices=["local", "s3"], default="s3")
    build_index_parser.add_argument("--bucket", default="beacon-scale-dev")
    build_index_parser.add_argument("--extract-prefix", default="extracted-documents")
    build_index_parser.add_argument("--index-output-prefix", default="search-index")
    build_index_parser.add_argument(
        "--corpus-object-key", default="search-index/corpus/documents.jsonl"
    )
    build_index_parser.add_argument(
        "--no-compress",
        action="store_true",
        help="No invocar index-compression-codec tras fusionar el índice global",
    )
    build_index_parser.add_argument("--compressed-output-prefix", default="search-index-compressed")
    build_index_parser.add_argument(
        "--local-storage-root", type=Path, default=Path(".local-object-storage")
    )

    compute_pagerank_parser = subparsers.add_parser(
        "compute-pagerank",
        help="Ejecuta el pipeline de PageRank distribuido sobre el corpus indexado por la "
        "fase 3 y el grafo de enlaces crudo de la fase 1, una única vez",
    )
    compute_pagerank_parser.add_argument("--storage-backend", choices=["local", "s3"], default="s3")
    compute_pagerank_parser.add_argument("--bucket", default="beacon-scale-dev")
    compute_pagerank_parser.add_argument("--crawl-pages-prefix", default="crawl-pages")
    compute_pagerank_parser.add_argument(
        "--documents-object-key", default="search-index/documents.jsonl"
    )
    compute_pagerank_parser.add_argument("--output-prefix", default="pagerank-scores")
    compute_pagerank_parser.add_argument(
        "--max-concurrent-reads",
        type=int,
        default=64,
        help="Lecturas concurrentes acotadas al materializar link_graph.jsonl desde "
        "crawl-pages/ (ver ARCHITECTURE.md, fase 4, sección 3)",
    )
    compute_pagerank_parser.add_argument("--damping-factor", type=float, default=0.85)
    compute_pagerank_parser.add_argument("--tolerance", type=float, default=1.0e-6)
    compute_pagerank_parser.add_argument("--max-iterations", type=int, default=100)
    compute_pagerank_parser.add_argument(
        "--local-storage-root", type=Path, default=Path(".local-object-storage")
    )

    shard_index_parser = subparsers.add_parser(
        "shard-index",
        help="Particiona el índice global de fase 3 en N shards y los sube al almacenamiento "
        "de objetos, una única vez, para que las réplicas de 'shard-replica' los descarguen",
    )
    shard_index_parser.add_argument("--storage-backend", choices=["local", "s3"], default="s3")
    shard_index_parser.add_argument("--bucket", default="beacon-scale-dev")
    shard_index_parser.add_argument(
        "--source-index-prefix",
        default="search-index-compressed",
        help="Prefijo del índice global de fase 3 -- 'search-index-compressed' si 'build-index' "
        "comprimió (por defecto), o 'search-index' si se corrió con --no-compress",
    )
    shard_index_parser.add_argument("--shard-index-prefix", default="shard-index")
    shard_index_parser.add_argument("--num-shards", type=int, default=3)
    shard_index_parser.add_argument(
        "--local-storage-root", type=Path, default=Path(".local-object-storage")
    )

    shard_replica_parser = subparsers.add_parser(
        "shard-replica",
        help="Arranca una réplica escalable de un shard: descarga su partición, sirve "
        "'distributed-index-sharding serve-shard' y se anuncia en el registro de servicio",
    )
    shard_replica_parser.add_argument("--shard-id", type=int, required=True)
    shard_replica_parser.add_argument(
        "--replica-id",
        default=None,
        help="Identificador de esta réplica dentro de su shard_id (por defecto, el hostname del "
        "contenedor -- distinto por réplica bajo 'docker compose up --scale')",
    )
    shard_replica_parser.add_argument("--storage-backend", choices=["local", "s3"], default="s3")
    shard_replica_parser.add_argument("--bucket", default="beacon-scale-dev")
    shard_replica_parser.add_argument("--shard-index-prefix", default="shard-index")
    shard_replica_parser.add_argument(
        "--registry-backend", choices=["local", "consul"], default="consul"
    )
    shard_replica_parser.add_argument("--service-name", default="beacon-scale-shard")
    shard_replica_parser.add_argument(
        "--host", default="0.0.0.0", help="Dirección de bind del servidor HTTP del shard"
    )
    shard_replica_parser.add_argument("--port", type=int, default=9300)
    shard_replica_parser.add_argument(
        "--announce-host",
        default=None,
        help="Dirección con la que esta réplica se anuncia en el registro de servicio (por "
        "defecto, el hostname del contenedor -- resoluble por los demás contenedores de la "
        "misma red de 'docker-compose.yml')",
    )
    shard_replica_parser.add_argument("--ttl-seconds", type=float, default=15.0)
    shard_replica_parser.add_argument("--heartbeat-interval-seconds", type=float, default=5.0)
    shard_replica_parser.add_argument("--health-check-timeout-seconds", type=float, default=30.0)
    shard_replica_parser.add_argument(
        "--local-storage-root", type=Path, default=Path(".local-object-storage")
    )

    search_parser = subparsers.add_parser(
        "search",
        help="Lanza una consulta distribuida contra los shards vivos en este momento, "
        "descubiertos vía el registro de servicio (no una lista fija de --shard)",
    )
    search_parser.add_argument("--registry-backend", choices=["local", "consul"], default="consul")
    search_parser.add_argument("--service-name", default="beacon-scale-shard")
    search_parser.add_argument("--top-k", type=int, default=10)
    search_parser.add_argument("--timeout-seconds", type=float, default=2.0)
    search_parser.add_argument("--text", help="Query en crudo, sin operadores")
    search_parser.add_argument(
        "--parsed-query-file", type=Path, help="JSON de ParsedQuery (query-parser-autocomplete)"
    )

    train_reranker_parser = subparsers.add_parser(
        "train-reranker",
        help="Entrena el modelo LTR (learning-to-rank-reranker, dataset sintético "
        "determinista) y lo sube al almacenamiento de objetos, una única vez, para que "
        "las réplicas de la consola lo descarguen al arrancar",
    )
    train_reranker_parser.add_argument("--storage-backend", choices=["local", "s3"], default="s3")
    train_reranker_parser.add_argument("--bucket", default="beacon-scale-dev")
    train_reranker_parser.add_argument("--model-output-prefix", default="ltr-model")
    train_reranker_parser.add_argument("--num-queries", type=int, default=300)
    train_reranker_parser.add_argument("--candidates-per-query", type=int, default=25)
    train_reranker_parser.add_argument("--seed", type=int, default=42)
    train_reranker_parser.add_argument(
        "--local-storage-root", type=Path, default=Path(".local-object-storage")
    )

    serve_console_parser = subparsers.add_parser(
        "serve-console",
        help="Arranca una réplica de la API de la consola (fase 6): descarga los "
        "artefactos del índice, descubre los shards de fase 5 vía el registro de "
        "servicio y sirve el contrato /api/v1 -- escalable a N réplicas tras un "
        "balanceador (ver docker-compose.yml, servicios console-api/console-lb)",
    )
    serve_console_parser.add_argument("--host", default="127.0.0.1")
    serve_console_parser.add_argument("--port", type=int, default=8000)

    return parser


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"variable de entorno requerida no definida: {name}")
    return value


async def _run_storage_demo(args: argparse.Namespace) -> None:
    storage: ObjectStorage
    if args.backend == "local":
        storage = LocalFilesystemObjectStorage(args.local_root)
    else:
        storage = S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )
    key = "demo/hello.txt"
    payload = b"hello from beacon-search-engine-at-scale\n"
    print(f"[storage-demo:{args.backend}] put_object({args.bucket!r}, {key!r})")
    metadata = await storage.put_object(args.bucket, key, payload, content_type="text/plain")
    print(f"  -> {metadata}")
    print(f"[storage-demo:{args.backend}] get_object({args.bucket!r}, {key!r})")
    read_back = await storage.get_object(args.bucket, key)
    if read_back != payload:
        raise BeaconScaleInfraError("el contenido leído no coincide con el escrito")
    print(f"  -> {read_back!r}")
    print(f"[storage-demo:{args.backend}] list_objects({args.bucket!r}, prefix='demo/')")
    async for entry in storage.list_objects(args.bucket, prefix="demo/"):
        print(f"  -> {entry}")
    print(f"[storage-demo:{args.backend}] delete_object({args.bucket!r}, {key!r})")
    await storage.delete_object(args.bucket, key)
    if await storage.object_exists(args.bucket, key):
        raise BeaconScaleInfraError("el objeto debería haber desaparecido tras delete_object")
    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()
    print("OK")


async def _run_queue_demo(args: argparse.Namespace) -> None:
    queue: MessageQueue
    if args.backend == "memory":
        queue = InMemoryMessageQueue()
    else:
        queue = RedisStreamsMessageQueue.from_url(
            os.environ.get("BEACON_REDIS_URL", "redis://localhost:6379/0")
        )
    await queue.ensure_group(args.stream, args.group)
    payload = {"kind": "demo", "message": "hello from beacon-search-engine-at-scale"}
    print(f"[queue-demo:{args.backend}] publish({args.stream!r}, {payload!r})")
    message_id = await queue.publish(args.stream, payload)
    print(f"  -> message_id={message_id!r}")
    print(f"[queue-demo:{args.backend}] consume({args.stream!r}, {args.group!r})")
    messages = await queue.consume(
        args.stream, args.group, "demo-consumer", count=10, block_ms=2000
    )
    if not messages:
        raise BeaconScaleInfraError("no se recibió el mensaje recién publicado")
    for message in messages:
        print(f"  -> {message}")
        await queue.ack(args.stream, args.group, message.message_id)
    if isinstance(queue, RedisStreamsMessageQueue):
        await queue.aclose()
    print("OK")


async def _run_registry_demo(args: argparse.Namespace) -> None:
    registry: ServiceRegistry
    if args.backend == "local":
        registry = InMemoryServiceRegistry()
    else:
        registry = ConsulServiceRegistry(
            base_url=os.environ.get("BEACON_CONSUL_BASE_URL", "http://localhost:8500")
        )
    instance = ServiceInstance(
        service_id="demo-shard-0",
        service_name=args.service_name,
        host="127.0.0.1",
        port=9300,
        metadata={"shard_id": "0"},
    )
    print(f"[registry-demo:{args.backend}] register({instance!r})")
    await registry.register(instance, ttl_seconds=15.0)
    print(f"[registry-demo:{args.backend}] heartbeat({instance.service_id!r})")
    await registry.heartbeat(instance.service_id)
    print(f"[registry-demo:{args.backend}] discover({args.service_name!r})")
    discovered = await registry.discover(args.service_name)
    for found in discovered:
        print(f"  -> {found}")
    if not any(found.service_id == instance.service_id for found in discovered):
        raise BeaconScaleInfraError("la instancia recién registrada no aparece en discover()")
    print(f"[registry-demo:{args.backend}] deregister({instance.service_id!r})")
    await registry.deregister(instance.service_id)
    if isinstance(registry, ConsulServiceRegistry):
        await registry.aclose()
    print("OK")


def _resolve_seed_urls(args: argparse.Namespace) -> tuple[str, ...]:
    if args.seed_urls:
        return tuple(args.seed_urls)
    env_value = os.environ.get("BEACON_CRAWL_SEED_URLS", "")
    seeds = tuple(url.strip() for url in env_value.split(",") if url.strip())
    if not seeds:
        raise SystemExit("no se especificaron URLs semilla: usa --seed o BEACON_CRAWL_SEED_URLS")
    return seeds


async def _run_crawl_worker(args: argparse.Namespace) -> None:
    worker_id = args.worker_id or socket.gethostname()
    config = CrawlWorkerConfig(
        worker_id=worker_id,
        seed_urls=_resolve_seed_urls(args),
        stream=args.stream,
        group=args.group,
        bucket=args.bucket,
        object_key_prefix=args.object_key_prefix,
        num_hash_shards=args.num_hash_shards,
        max_depth=args.max_depth,
        max_pages=args.max_pages,
        max_concurrent_per_domain=args.max_concurrent_per_domain,
        default_min_delay_seconds=args.min_delay_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        max_retries=args.max_retries,
        obey_robots_txt=args.obey_robots_txt,
        idle_polls_before_shutdown=(
            None if args.no_idle_shutdown else args.idle_polls_before_shutdown
        ),
    )

    queue: MessageQueue
    if args.queue_backend == "memory":
        queue = InMemoryMessageQueue()
    else:
        queue = RedisStreamsMessageQueue.from_url(
            os.environ.get("BEACON_REDIS_URL", "redis://localhost:6379/0")
        )

    storage: ObjectStorage
    if args.storage_backend == "local":
        storage = LocalFilesystemObjectStorage(args.local_storage_root)
    else:
        storage = S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )

    # El deduplicador y el rate limiter comparten la misma conexión Redis
    # cuando ambos usan el backend real: son dos namespaces de claves
    # distintos (`beacon:crawl:seen-urls` vs. `beacon:crawl:rl:*`) sobre la
    # misma instancia que ya aloja la cola de mensajes, sin justificar una
    # segunda conexión ni un segundo servicio.
    coordination_client: redis_asyncio.Redis | None = None
    dedup: InMemorySharedDeduplicator | RedisSharedDeduplicator
    rate_limiter: CoordinatedRateLimiter
    if args.coordination_backend == "memory":
        dedup = InMemorySharedDeduplicator()
        rate_limiter = InMemoryCoordinatedRateLimiter(
            max_concurrent_per_domain=args.max_concurrent_per_domain,
            default_min_delay_seconds=args.min_delay_seconds,
        )
    else:
        coordination_client = redis_asyncio.Redis.from_url(
            os.environ.get("BEACON_REDIS_URL", "redis://localhost:6379/0"), decode_responses=True
        )
        dedup = RedisSharedDeduplicator(client=coordination_client)
        rate_limiter = RedisCoordinatedRateLimiter(
            client=coordination_client,
            max_concurrent_per_domain=args.max_concurrent_per_domain,
            default_min_delay_seconds=args.min_delay_seconds,
        )

    async with aiohttp.ClientSession() as session:
        fetcher = AiohttpFetcher(
            session,
            max_retries=config.max_retries,
            backoff_base_seconds=config.backoff_base_seconds,
            backoff_max_seconds=config.backoff_max_seconds,
            user_agent=config.user_agent,
        )
        robots = RobotsCache(session, timeout_seconds=config.request_timeout_seconds)
        worker = CrawlWorker(
            config,
            queue=queue,
            storage=storage,
            dedup=dedup,
            rate_limiter=rate_limiter,
            fetcher=fetcher,
            robots=robots,
        )
        print(f"[crawl-worker:{worker_id}] arrancando (seeds={list(config.seed_urls)!r})")
        stats = await worker.run()
        print(f"[crawl-worker:{worker_id}] terminado: {stats}")

    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()
    if isinstance(queue, RedisStreamsMessageQueue):
        await queue.aclose()
    if coordination_client is not None:
        await coordination_client.aclose()


async def _run_extract_worker(args: argparse.Namespace) -> None:
    worker_id = args.worker_id or socket.gethostname()
    config = ExtractWorkerConfig(
        worker_id=worker_id,
        stream=args.stream,
        group=args.group,
        bucket=args.bucket,
        object_key_prefix=args.object_key_prefix,
        extraction_config=ExtractionConfig(
            min_main_content_chars=args.min_main_content_chars,
            min_block_text_chars=args.min_block_text_chars,
            link_density_threshold=args.link_density_threshold,
        ),
        flush_every_pages=args.flush_every_pages,
        max_pages=args.max_pages,
        idle_polls_before_shutdown=(
            None if args.no_idle_shutdown else args.idle_polls_before_shutdown
        ),
    )

    queue: MessageQueue
    if args.queue_backend == "memory":
        queue = InMemoryMessageQueue()
    else:
        queue = RedisStreamsMessageQueue.from_url(
            os.environ.get("BEACON_REDIS_URL", "redis://localhost:6379/0")
        )

    storage: ObjectStorage
    if args.storage_backend == "local":
        storage = LocalFilesystemObjectStorage(args.local_storage_root)
    else:
        storage = S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )

    worker = ExtractWorker(config, queue=queue, storage=storage)
    print(f"[extract-worker:{worker_id}] arrancando")
    stats = await worker.run()
    print(f"[extract-worker:{worker_id}] terminado: {stats}")

    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()
    if isinstance(queue, RedisStreamsMessageQueue):
        await queue.aclose()


async def _run_build_index(args: argparse.Namespace) -> None:
    config = IndexingPipelineConfig(
        bucket=args.bucket,
        extract_prefix=args.extract_prefix,
        index_output_prefix=args.index_output_prefix,
        corpus_object_key=args.corpus_object_key,
        compress=not args.no_compress,
        compressed_output_prefix=args.compressed_output_prefix,
    )

    storage: ObjectStorage
    if args.storage_backend == "local":
        storage = LocalFilesystemObjectStorage(args.local_storage_root)
    else:
        storage = S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )

    pipeline = IndexingPipeline(config, storage=storage)
    print(
        f"[build-index] arrancando (bucket={args.bucket!r}, extract_prefix={args.extract_prefix!r})"
    )
    stats = await pipeline.run()
    print(f"[build-index] terminado: {stats}")

    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()


async def _run_compute_pagerank(args: argparse.Namespace) -> None:
    config = PageRankPipelineConfig(
        bucket=args.bucket,
        crawl_pages_prefix=args.crawl_pages_prefix,
        documents_object_key=args.documents_object_key,
        output_prefix=args.output_prefix,
        max_concurrent_reads=args.max_concurrent_reads,
        pagerank_params=PageRankParams(
            damping_factor=args.damping_factor,
            tolerance=args.tolerance,
            max_iterations=args.max_iterations,
        ),
    )

    storage: ObjectStorage
    if args.storage_backend == "local":
        storage = LocalFilesystemObjectStorage(args.local_storage_root)
    else:
        storage = S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )

    pipeline = DistributedPageRankPipeline(config, storage=storage)
    print(
        f"[compute-pagerank] arrancando (bucket={args.bucket!r}, "
        f"crawl_pages_prefix={args.crawl_pages_prefix!r})"
    )
    stats = await pipeline.run()
    print(f"[compute-pagerank] terminado: {stats}")

    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()


async def _run_shard_index(args: argparse.Namespace) -> None:
    config = ShardIndexPipelineConfig(
        bucket=args.bucket,
        source_index_prefix=args.source_index_prefix,
        shard_index_prefix=args.shard_index_prefix,
        num_shards=args.num_shards,
    )

    storage: ObjectStorage
    if args.storage_backend == "local":
        storage = LocalFilesystemObjectStorage(args.local_storage_root)
    else:
        storage = S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )

    pipeline = ShardIndexPipeline(config, storage=storage)
    print(
        f"[shard-index] arrancando (bucket={args.bucket!r}, "
        f"source_index_prefix={args.source_index_prefix!r}, num_shards={args.num_shards})"
    )
    stats = await pipeline.run()
    print(f"[shard-index] terminado: {stats}")

    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()


async def _run_shard_replica(args: argparse.Namespace) -> None:
    replica_id = args.replica_id or socket.gethostname()
    config = ShardReplicaConfig(
        shard_id=args.shard_id,
        replica_id=replica_id,
        bucket=args.bucket,
        shard_index_prefix=args.shard_index_prefix,
        service_name=args.service_name,
        host=args.host,
        port=args.port,
        announce_host=args.announce_host,
        ttl_seconds=args.ttl_seconds,
        heartbeat_interval_seconds=args.heartbeat_interval_seconds,
        health_check_timeout_seconds=args.health_check_timeout_seconds,
    )

    storage: ObjectStorage
    if args.storage_backend == "local":
        storage = LocalFilesystemObjectStorage(args.local_storage_root)
    else:
        storage = S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )

    registry: ServiceRegistry
    if args.registry_backend == "local":
        registry = InMemoryServiceRegistry()
    else:
        registry = ConsulServiceRegistry(
            base_url=os.environ.get("BEACON_CONSUL_BASE_URL", "http://localhost:8500")
        )

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    # SIGTERM es lo que 'docker stop'/'docker compose down' envía: sin un
    # manejador propio, Python lo trata como terminación inmediata y esta
    # réplica nunca llegaría a desregistrarse (ver shard_replica_service.py,
    # sección sobre apagado con aviso vs. sin aviso). add_signal_handler no
    # existe en Windows, pero este binario solo corre en contenedores Linux.
    with contextlib.suppress(NotImplementedError):
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, shutdown_event.set)

    print(f"[shard-replica:{config.shard_id}:{replica_id}] arrancando")
    service = await ShardReplicaService.start(config, storage=storage, registry=registry)
    try:
        await shutdown_event.wait()
    finally:
        print(f"[shard-replica:{config.shard_id}:{replica_id}] deteniendo")
        await service.shutdown()

    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()
    if isinstance(registry, ConsulServiceRegistry):
        await registry.aclose()


def _print_fan_out_result(result: FanOutResult) -> None:
    for outcome in result.outcomes:
        status_note = "" if outcome.status == "ok" else f" ({outcome.error_message})"
        print(
            f"  shard {outcome.shard_id}: {outcome.status}{status_note} "
            f"[{outcome.latency_ms:.1f} ms, {len(outcome.hits)} hits]",
            file=sys.stderr,
        )
    if not result.merged:
        print("Sin resultados")
        return
    for position, hit in enumerate(result.merged, start=1):
        print(f"{position}. doc_id={hit.doc_id} score={hit.score:.4f}")


async def _run_search(args: argparse.Namespace) -> None:
    if bool(args.text) == bool(args.parsed_query_file):
        raise SystemExit("pasa exactamente uno de --text o --parsed-query-file")

    registry: ServiceRegistry
    if args.registry_backend == "local":
        registry = InMemoryServiceRegistry()
    else:
        registry = ConsulServiceRegistry(
            base_url=os.environ.get("BEACON_CONSUL_BASE_URL", "http://localhost:8500")
        )

    async with DistributedQueryServingPipeline(
        registry, service_name=args.service_name, timeout_seconds=args.timeout_seconds
    ) as pipeline:
        if args.text:
            result = await pipeline.search_text(args.text, top_k=args.top_k)
        else:
            parsed_query_file: Path = args.parsed_query_file
            if not parsed_query_file.exists():
                raise SystemExit(f"no existe parsed_query_file={parsed_query_file}")
            parsed_query: dict[str, Any] = json.loads(parsed_query_file.read_text(encoding="utf-8"))
            result = await pipeline.search_parsed_query(parsed_query, top_k=args.top_k)

    _print_fan_out_result(result)

    if isinstance(registry, ConsulServiceRegistry):
        await registry.aclose()


async def _run_train_reranker(args: argparse.Namespace) -> None:
    config = RerankerTrainingConfig(
        bucket=args.bucket,
        model_output_prefix=args.model_output_prefix,
        num_queries=args.num_queries,
        candidates_per_query=args.candidates_per_query,
        seed=args.seed,
    )

    storage: ObjectStorage
    if args.storage_backend == "local":
        storage = LocalFilesystemObjectStorage(args.local_storage_root)
    else:
        storage = S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )

    pipeline = RerankerTrainingPipeline(config, storage=storage)
    print(
        f"[train-reranker] arrancando (bucket={args.bucket!r}, "
        f"model_output_prefix={args.model_output_prefix!r}, seed={args.seed})"
    )
    stats = await pipeline.run()
    print(f"[train-reranker] terminado: {stats}")

    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()


def _run_serve_console(args: argparse.Namespace) -> None:
    # uvicorn gestiona su propio event loop: este subcomando se despacha en
    # main() antes de asyncio.run, a diferencia del resto (ver _dispatch).
    uvicorn.run("beacon_scale_infra.console.app:app", host=args.host, port=args.port)


async def _dispatch(args: argparse.Namespace) -> None:
    if args.command == "storage-demo":
        await _run_storage_demo(args)
    elif args.command == "queue-demo":
        await _run_queue_demo(args)
    elif args.command == "registry-demo":
        await _run_registry_demo(args)
    elif args.command == "crawl-worker":
        await _run_crawl_worker(args)
    elif args.command == "extract-worker":
        await _run_extract_worker(args)
    elif args.command == "build-index":
        await _run_build_index(args)
    elif args.command == "compute-pagerank":
        await _run_compute_pagerank(args)
    elif args.command == "shard-index":
        await _run_shard_index(args)
    elif args.command == "shard-replica":
        await _run_shard_replica(args)
    elif args.command == "search":
        await _run_search(args)
    elif args.command == "train-reranker":
        await _run_train_reranker(args)
    else:  # pragma: no cover - argparse restringe 'command' a los subparsers de arriba
        raise AssertionError(f"comando desconocido: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "serve-console":
            _run_serve_console(args)
        else:
            asyncio.run(_dispatch(args))
    except BeaconScaleInfraError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
