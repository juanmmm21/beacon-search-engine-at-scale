"""CLI de demostración de `beacon-search-engine-at-scale`: ejercita las tres
piezas de sustrato compartido (`storage-demo`, `queue-demo`,
`registry-demo`) contra su implementación local de desarrollo o contra los
servicios reales levantados por `docker-compose.yml` (MinIO, Redis, Consul),
seleccionable con `--backend`. Sirve como comprobación end-to-end de que el
`docker-compose` de desarrollo funciona de verdad, sin necesitar todavía
ninguna pieza de dominio de `beacon-search-engine`.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from beacon_scale_infra.errors import BeaconScaleInfraError
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


async def _dispatch(args: argparse.Namespace) -> None:
    if args.command == "storage-demo":
        await _run_storage_demo(args)
    elif args.command == "queue-demo":
        await _run_queue_demo(args)
    elif args.command == "registry-demo":
        await _run_registry_demo(args)
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
