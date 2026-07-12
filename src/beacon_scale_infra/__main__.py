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
"""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
import sys
from collections.abc import Sequence
from pathlib import Path

import aiohttp
import redis.asyncio as redis_asyncio
from html_content_extractor.models import ExtractionConfig
from web_crawler_scheduler.fetcher import AiohttpFetcher
from web_crawler_scheduler.robots import RobotsCache

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
from beacon_scale_infra.protocols import MessageQueue, ObjectStorage, ServiceRegistry
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
    else:  # pragma: no cover - argparse restringe 'command' a los subparsers de arriba
        raise AssertionError(f"comando desconocido: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        asyncio.run(_dispatch(args))
    except BeaconScaleInfraError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
