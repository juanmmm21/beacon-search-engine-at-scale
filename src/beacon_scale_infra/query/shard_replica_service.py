"""Réplica escalable de un shard: descarga su partición del índice desde el
almacenamiento de objetos compartido, arranca un subproceso real
`distributed-index-sharding serve-shard` sobre ella, se anuncia en el
`ServiceRegistry` de fase 0 con su `shard_id` en la metadata, y mantiene su
TTL vivo con heartbeats periódicos -- el "contenedor/máquina real" al que este
repo lleva la simulación local de subprocesos de
`distributed_index_sharding.cluster.LocalShardCluster` (ver `ARCHITECTURE.md`,
fase 5). El servidor HTTP del shard en sí (`serve-shard`) no se toca: se sigue
lanzando exactamente como lo hace `LocalShardCluster`, solo que aquí un único
proceso por réplica, no un clúster completo de N shards en un mismo proceso
padre.

Ante una caída sin aviso (proceso matado, contenedor matado con `docker kill`
/ SIGKILL), no hay ninguna oportunidad de ejecutar código de este proceso para
desregistrarse -- exactamente el escenario para el que existe la expiración
por TTL, tanto en `ConsulServiceRegistry` como en `InMemoryServiceRegistry`
(ver sus propios docstrings): tras `ttl_seconds` sin heartbeat, `discover()`
deja de devolver esa réplica sola, sin que este código tenga que hacer nada.
`shutdown()` cubre el otro camino, el de apagado *con aviso* (SIGTERM de
`docker stop`, Ctrl+C): desregistro explícito antes de terminar el subproceso.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil
import socket
import sys
import tempfile
import time
from pathlib import Path
from types import TracebackType
from typing import Final

import aiohttp

from beacon_scale_infra.errors import ObjectNotFoundError, QueryServingError, ServiceRegistryError
from beacon_scale_infra.index.index_version import (
    INDEX_VERSION_MARKER_BASENAME,
    parse_index_version_marker,
)
from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.protocols import ObjectStorage, ServiceRegistry
from beacon_scale_infra.query.models import ShardReplicaConfig
from beacon_scale_infra.query.shard_discovery import (
    INDEX_VERSION_METADATA_KEY,
    SHARD_ID_METADATA_KEY,
)

logger = logging.getLogger(__name__)

_HEALTH_CHECK_POLL_INTERVAL_SECONDS: Final[float] = 0.2
_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS: Final[float] = 5.0


def _default_announce_host() -> str:
    """Hostname corto que Docker asigna a cada contenedor, resoluble por los
    demás contenedores de la misma red definida por el usuario en
    `docker-compose.yml` -- el mismo mecanismo que `CrawlWorker`/`ExtractWorker`
    ya usan como `--worker-id` por defecto (ver `docker-compose.yml`,
    comentario en el servicio `crawl-worker`), aplicado aquí a una dirección
    de red real en vez de a un simple identificador."""
    return socket.gethostname()


class ShardReplicaService:
    """Gestor de contexto asíncrono. Constrúyase con `ShardReplicaService.start`,
    nunca con `__init__` directamente (necesita `await` para descargar la
    partición, arrancar el subproceso y registrarse)."""

    def __init__(
        self,
        config: ShardReplicaConfig,
        process: asyncio.subprocess.Process,
        registry: ServiceRegistry,
        shard_dir: Path,
        index_version: str | None,
    ) -> None:
        self._config = config
        self._process = process
        self._registry = registry
        self._shard_dir = shard_dir
        self._index_version = index_version
        self._heartbeat_task: asyncio.Task[None] | None = None

    @classmethod
    async def start(
        cls,
        config: ShardReplicaConfig,
        *,
        storage: ObjectStorage,
        registry: ServiceRegistry,
    ) -> ShardReplicaService:
        shard_dir = Path(tempfile.mkdtemp(prefix=f"beacon-scale-shard-{config.shard_id}-replica-"))
        downloaded = await cls._download_shard(storage, config, shard_dir)
        if downloaded == 0:
            shutil.rmtree(shard_dir, ignore_errors=True)
            raise QueryServingError(
                f"no hay ningún objeto bajo {config.shard_object_prefix!r} en el bucket "
                f"{config.bucket!r}: ¿ha terminado 'shard-index' para este shard_id?"
            )
        index_version = await cls._read_index_version(storage, config)

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "distributed_index_sharding",
            "serve-shard",
            str(shard_dir),
            "--shard-id",
            str(config.shard_id),
            "--host",
            config.host,
            "--port",
            str(config.port),
        )
        service = cls(config, process, registry, shard_dir, index_version)
        try:
            await service._wait_until_healthy()
            await service._register()
        except Exception:
            await service._terminate_process()
            shutil.rmtree(shard_dir, ignore_errors=True)
            raise
        service._heartbeat_task = asyncio.create_task(service._heartbeat_loop())
        logger.info(
            "réplica de shard %s (%s) arrancada en %s:%s",
            config.shard_id,
            config.replica_id,
            config.host,
            config.port,
        )
        return service

    @staticmethod
    async def _read_index_version(storage: ObjectStorage, config: ShardReplicaConfig) -> str | None:
        """Versión de contenido del índice que este `shard-index/` sirve
        (propagada por `ShardIndexPipeline` desde el marcador de fase 3).
        `None` si el prefijo se particionó con una versión de `shard-index`
        anterior al marcador: la réplica arranca y sirve igualmente (metadata
        sin versión), pero la consola no podrá validar coherencia ni cachear
        contra ella -- degradación explícita, nunca un arranque fallido por
        un artefacto antiguo. Un marcador presente pero ilegible sí es error:
        indica corrupción, no antigüedad."""
        marker_key = f"{config.shard_index_prefix}/{INDEX_VERSION_MARKER_BASENAME}"
        try:
            raw = await storage.get_object(config.bucket, marker_key)
        except ObjectNotFoundError:
            logger.warning(
                "sin marcador de versión de índice en %r: la réplica de shard %s se anuncia "
                "sin 'index_version' (re-ejecuta 'shard-index' para publicarlo)",
                marker_key,
                config.shard_id,
            )
            return None
        try:
            return parse_index_version_marker(raw)
        except ValueError as exc:
            raise QueryServingError(f"marcador {marker_key!r} ilegible: {exc}") from exc

    @staticmethod
    async def _download_shard(
        storage: ObjectStorage, config: ShardReplicaConfig, shard_dir: Path
    ) -> int:
        count = 0
        prefix = config.shard_object_prefix
        async for entry in storage.list_objects(config.bucket, prefix=f"{prefix}/"):
            relative_name = entry.key[len(prefix) + 1 :]
            if not relative_name or "/" in relative_name:
                continue
            data = await storage.get_object(config.bucket, entry.key)
            (shard_dir / relative_name).write_bytes(data)
            count += 1
        return count

    async def _wait_until_healthy(self) -> None:
        # 0.0.0.0 es una dirección de bind válida pero no una a la que se
        # pueda hacer *connect* desde el propio proceso -- probamos contra
        # localhost cuando el bind es "todas las interfaces", igual que
        # cualquier health-check local tendría que hacer.
        probe_host = "127.0.0.1" if self._config.host == "0.0.0.0" else self._config.host
        probe_url = f"http://{probe_host}:{self._config.port}/health"
        deadline = time.monotonic() + self._config.health_check_timeout_seconds
        async with aiohttp.ClientSession() as session:
            while True:
                if self._process.returncode is not None:
                    raise QueryServingError(
                        f"el subproceso de shard {self._config.shard_id} terminó "
                        f"prematuramente (returncode={self._process.returncode}) antes de "
                        "responder a /health"
                    )
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"shard {self._config.shard_id} ({probe_url}) no respondió a "
                        f"/health en {self._config.health_check_timeout_seconds}s"
                    )
                try:
                    async with session.get(
                        probe_url, timeout=aiohttp.ClientTimeout(total=1.0)
                    ) as response:
                        if response.status == 200:
                            return
                except aiohttp.ClientError:
                    pass
                await asyncio.sleep(_HEALTH_CHECK_POLL_INTERVAL_SECONDS)

    async def _register(self) -> None:
        announce_host = self._config.announce_host or _default_announce_host()
        metadata = {SHARD_ID_METADATA_KEY: str(self._config.shard_id)}
        if self._index_version is not None:
            metadata[INDEX_VERSION_METADATA_KEY] = self._index_version
        instance = ServiceInstance(
            service_id=self._config.service_id,
            service_name=self._config.service_name,
            host=announce_host,
            port=self._config.port,
            metadata=metadata,
        )
        await self._registry.register(instance, ttl_seconds=self._config.ttl_seconds)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.heartbeat_interval_seconds)
            try:
                await self._registry.heartbeat(self._config.service_id)
            except ServiceRegistryError as exc:
                # Nunca aborta el proceso del shard por un heartbeat fallido
                # (p. ej. Consul momentáneamente inalcanzable): se registra y
                # se reintenta en el siguiente ciclo -- degradar y continuar,
                # nunca tumbar una réplica sana por un fallo transitorio del
                # propio registro (ver `~/Desarrollo/beacon-search-engine/CLAUDE.md`,
                # regla de manejo de errores de E/S).
                logger.warning(
                    "heartbeat fallido para la réplica de shard %s (%s): %s",
                    self._config.shard_id,
                    self._config.replica_id,
                    exc,
                )

    async def kill_process(self) -> None:
        """Mata sin gracia (`SIGKILL`) el subproceso de esta réplica, sin
        desregistrarla del `ServiceRegistry` -- para poder probar la
        expiración por TTL ante una caída sin aviso (el mismo caso que un
        `docker kill` real produce) sin esperar de verdad a que un proceso o
        un contenedor mueran por sí solos. Mismo papel que
        `distributed_index_sharding.cluster.LocalShardCluster.kill` cumple
        para sus propios tests end-to-end (ver ese módulo)."""
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None
        self._process.kill()
        await self._process.wait()

    async def __aenter__(self) -> ShardReplicaService:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.shutdown()

    async def shutdown(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None
        with contextlib.suppress(ServiceRegistryError):
            await self._registry.deregister(self._config.service_id)
        await self._terminate_process()
        shutil.rmtree(self._shard_dir, ignore_errors=True)
        logger.info(
            "réplica de shard %s (%s) detenida", self._config.shard_id, self._config.replica_id
        )

    async def _terminate_process(self) -> None:
        if self._process.returncode is None:
            self._process.terminate()
        try:
            await asyncio.wait_for(self._process.wait(), timeout=_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.warning(
                "el subproceso de shard %s (pid=%s) no terminó a tiempo, forzando kill",
                self._config.shard_id,
                self._process.pid,
            )
            self._process.kill()
            await self._process.wait()
