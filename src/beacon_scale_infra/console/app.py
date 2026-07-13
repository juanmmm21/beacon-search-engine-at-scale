"""Punto de entrada FastAPI de la consola (fase 6): construye los backends de
sustrato desde variables de entorno, levanta el estado compartido de la
réplica en el ciclo de vida de la app y monta las tres rutas del contrato
versionado (`/api/v1/search`, `/api/v1/autocomplete`, `/api/v1/index/stats`)
-- el mismo contrato, prefijo incluido, que `beacon-search-console`.

Cada réplica de este proceso es intercambiable detrás de un balanceador: no
arranca subprocesos de shard, no fija puertos de shard locales y no mantiene
ningún estado mutable propio que otra réplica no reconstruya idénticamente
(ver `dependencies.py` y `ARCHITECTURE.md`, fase 6).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from beacon_scale_infra.cache.memory import InMemoryCacheStore
from beacon_scale_infra.cache.redis_cache import RedisCacheStore
from beacon_scale_infra.console.config import ConsoleAppConfig
from beacon_scale_infra.console.dependencies import ConsoleAppState
from beacon_scale_infra.console.routes import autocomplete, search, stats
from beacon_scale_infra.protocols import CacheStore, ObjectStorage, ServiceRegistry
from beacon_scale_infra.registry.consul import ConsulServiceRegistry
from beacon_scale_infra.registry.local import InMemoryServiceRegistry
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage
from beacon_scale_infra.storage.s3 import S3ObjectStorage


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"variable de entorno requerida no definida: {name}")
    return value


def storage_from_env() -> ObjectStorage:
    backend = os.environ.get("BEACON_CONSOLE_STORAGE_BACKEND", "s3")
    if backend == "local":
        return LocalFilesystemObjectStorage(
            Path(os.environ.get("BEACON_LOCAL_STORAGE_ROOT", ".local-object-storage"))
        )
    if backend == "s3":
        return S3ObjectStorage(
            endpoint_url=_require_env("BEACON_S3_ENDPOINT_URL"),
            access_key=_require_env("BEACON_S3_ACCESS_KEY"),
            secret_key=_require_env("BEACON_S3_SECRET_KEY"),
            region_name=os.environ.get("BEACON_S3_REGION", "us-east-1"),
        )
    raise RuntimeError(f"BEACON_CONSOLE_STORAGE_BACKEND desconocido: {backend!r}")


def registry_from_env() -> ServiceRegistry:
    backend = os.environ.get("BEACON_CONSOLE_REGISTRY_BACKEND", "consul")
    if backend == "local":
        # Solo para demos de un único proceso: un registro en memoria nunca
        # ve las réplicas de shard registradas desde otros procesos.
        return InMemoryServiceRegistry()
    if backend == "consul":
        return ConsulServiceRegistry(
            base_url=os.environ.get("BEACON_CONSUL_BASE_URL", "http://localhost:8500")
        )
    raise RuntimeError(f"BEACON_CONSOLE_REGISTRY_BACKEND desconocido: {backend!r}")


def cache_store_from_env() -> CacheStore:
    backend = os.environ.get("BEACON_CONSOLE_CACHE_BACKEND", "redis")
    if backend == "memory":
        # Solo para demos/tests de un único proceso: una caché en memoria por
        # réplica no comparte aciertos entre réplicas (y con más de una,
        # reintroduce exactamente la divergencia que la fase 6 elimina).
        return InMemoryCacheStore()
    if backend == "redis":
        return RedisCacheStore.from_url(
            os.environ.get("BEACON_REDIS_URL", "redis://localhost:6379/0")
        )
    raise RuntimeError(f"BEACON_CONSOLE_CACHE_BACKEND desconocido: {backend!r}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    config = ConsoleAppConfig.from_env()
    storage = storage_from_env()
    registry = registry_from_env()
    cache_store = cache_store_from_env()
    try:
        state = await ConsoleAppState.build(
            config, storage=storage, registry=registry, cache_store=cache_store
        )
    except Exception:
        await _close_backends(storage, registry, cache_store)
        raise
    app.state.console_state = state
    try:
        yield
    finally:
        await state.close()
        await _close_backends(storage, registry, cache_store)


async def _close_backends(
    storage: ObjectStorage, registry: ServiceRegistry, cache_store: CacheStore
) -> None:
    if isinstance(storage, S3ObjectStorage):
        await storage.aclose()
    if isinstance(registry, ConsulServiceRegistry):
        await registry.aclose()
    if isinstance(cache_store, RedisCacheStore):
        await cache_store.aclose()


def create_app() -> FastAPI:
    app = FastAPI(
        title="beacon-search-console-at-scale",
        description=(
            "La consola de búsqueda del ecosistema beacon-search-engine servida sobre el "
            "clúster distribuido real: descubrimiento dinámico de shards, caché compartida "
            "de resultados y réplicas de API intercambiables."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        # Mismo razonamiento que beacon-search-console: buscador de solo
        # lectura sobre un corpus público, sin credenciales ni cookies que un
        # origen abierto pueda explotar.
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    app.include_router(search.router, prefix="/api/v1")
    app.include_router(autocomplete.router, prefix="/api/v1")
    app.include_router(stats.router, prefix="/api/v1")
    return app


app = create_app()
