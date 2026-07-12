"""Rate limiting por dominio coordinado entre varios workers de crawl.

`web_crawler_scheduler.rate_limiter.DomainRateLimiter` reparte huecos de
concurrencia y aplica demora mínima *dentro de un único proceso*. Con varios
workers, cada uno con su propio `DomainRateLimiter` en memoria, ninguno sabe
lo que están haciendo los demás -- si cuatro workers deciden en paralelo que
"les toca" pedir a `example.com` porque cada uno, aisladamente, respeta su
propia demora mínima, el dominio recibe cuatro peticiones simultáneas. Es
exactamente el riesgo de tumbar un dominio ajeno "sin querer" que
`ARCHITECTURE.md` señala como el punto más fácil de romper al distribuir el
crawler. Este módulo mueve el mismo contrato (demora mínima + concurrencia
máxima por dominio) a Redis, compartido por todos los workers, reutilizando
`extract_domain` de `web_crawler_scheduler.urlnorm` para que la noción de
"dominio" sea idéntica a la del crawler de un solo proceso.

Dos primitivas independientes, ambas con clave por dominio:

1. **Puerta de demora mínima** (`SET clave NX PX <delay_ms>`): solo un
   worker puede "abrir la puerta" para un dominio dentro de la ventana de
   demora; los demás la encuentran cerrada (la clave ya existe, con TTL) y
   reintentan tras `poll_interval_seconds`. Por sí sola ya garantiza que, en
   todo el clúster, arranca como mucho una petición nueva por ventana de
   demora -- la defensa principal contra tumbar un dominio ajeno.
2. **Semáforo de concurrencia con lease** (sorted set de tokens con TTL,
   `ZADD`/`ZREMRANGEBYSCORE`/`ZCARD` dentro de una transacción
   `WATCH`/`MULTI`): necesario además de (1) porque una petición puede tardar
   más que la demora mínima entre arranques -- sin este semáforo, varias
   peticiones lentas al mismo dominio podrían seguir solapándose aunque cada
   una respete la demora de arranque. El TTL del lease se limpia solo en la
   siguiente adquisición (`ZREMRANGEBYSCORE`), así que un worker que muere a
   mitad de una petición no deja el hueco bloqueado para siempre -- mismo
   criterio de TTL que `ServiceRegistry` ya aplica a instancias vivas (ver
   `protocols.py`).
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Protocol, runtime_checkable

import redis.asyncio as redis_asyncio
from redis.exceptions import RedisError, WatchError
from web_crawler_scheduler.urlnorm import extract_domain

from beacon_scale_infra.errors import CoordinatedRateLimiterError

_DELAY_GATE_PREFIX = "beacon:crawl:rl:gate:"
_SEMAPHORE_PREFIX = "beacon:crawl:rl:sem:"
_DEFAULT_LEASE_TTL_SECONDS = 30.0
_DEFAULT_POLL_INTERVAL_SECONDS = 0.05


@runtime_checkable
class CoordinatedRateLimiter(Protocol):
    """A diferencia de `web_crawler_scheduler.protocols.RateLimiter`,
    `acquire` devuelve un token de lease y `release` lo recibe de vuelta: el
    semáforo de un solo proceso identifica el hueco a liberar solo por
    dominio porque el propio objeto en memoria ya sabe qué tarea lo pidió,
    pero un semáforo distribuido necesita un identificador explícito de qué
    lease concreto liberar -- de lo contrario un worker podría liberar por
    error el hueco de otro."""

    async def acquire(self, url: str, min_delay_seconds: float | None = None) -> str: ...

    async def release(self, url: str, lease_token: str) -> None: ...


class InMemoryCoordinatedRateLimiter:
    """Doble de desarrollo/test: mismo contrato con tokens de lease que la
    versión Redis, pero coordina solo tareas `asyncio` de un mismo proceso --
    útil para testear la lógica de `CrawlWorker` con varios workers
    *simulados* como tareas concurrentes, sin Redis real."""

    def __init__(
        self,
        *,
        max_concurrent_per_domain: int,
        default_min_delay_seconds: float,
        lease_ttl_seconds: float = _DEFAULT_LEASE_TTL_SECONDS,
    ) -> None:
        if max_concurrent_per_domain <= 0:
            raise ValueError("max_concurrent_per_domain debe ser positivo")
        if default_min_delay_seconds < 0:
            raise ValueError("default_min_delay_seconds no puede ser negativo")
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds debe ser positivo")
        self._max_concurrent_per_domain = max_concurrent_per_domain
        self._default_min_delay_seconds = default_min_delay_seconds
        self._lease_ttl_seconds = lease_ttl_seconds
        self._lock = asyncio.Lock()
        self._last_request_monotonic: dict[str, float] = {}
        # dominio -> {token de lease: instante monotónico de expiración}, el
        # mismo modelo que el sorted set de leases de la versión Redis --
        # un worker que muere sin liberar no deja el hueco bloqueado para
        # siempre.
        self._active_leases: dict[str, dict[str, float]] = {}

    async def acquire(self, url: str, min_delay_seconds: float | None = None) -> str:
        delay = self._default_min_delay_seconds if min_delay_seconds is None else min_delay_seconds
        if delay < 0:
            raise ValueError("min_delay_seconds no puede ser negativo")
        domain = extract_domain(url)
        while True:
            async with self._lock:
                now = time.monotonic()
                last = self._last_request_monotonic.get(domain)
                delay_elapsed = last is None or (now - last) >= delay
                active = self._active_leases.setdefault(domain, {})
                self._expire_stale_leases(active, now)
                if delay_elapsed and len(active) < self._max_concurrent_per_domain:
                    self._last_request_monotonic[domain] = now
                    token = str(uuid.uuid4())
                    active[token] = now + self._lease_ttl_seconds
                    return token
            await asyncio.sleep(_DEFAULT_POLL_INTERVAL_SECONDS)

    @staticmethod
    def _expire_stale_leases(active: dict[str, float], now: float) -> None:
        expired = [token for token, expires_at in active.items() if expires_at <= now]
        for token in expired:
            del active[token]

    async def release(self, url: str, lease_token: str) -> None:
        domain = extract_domain(url)
        async with self._lock:
            self._active_leases.get(domain, {}).pop(lease_token, None)


class RedisCoordinatedRateLimiter:
    """Implementación real, coordinada vía Redis entre todos los workers."""

    def __init__(
        self,
        *,
        client: redis_asyncio.Redis,
        max_concurrent_per_domain: int,
        default_min_delay_seconds: float,
        lease_ttl_seconds: float = _DEFAULT_LEASE_TTL_SECONDS,
        poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        if max_concurrent_per_domain <= 0:
            raise ValueError("max_concurrent_per_domain debe ser positivo")
        if default_min_delay_seconds < 0:
            raise ValueError("default_min_delay_seconds no puede ser negativo")
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds debe ser positivo")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds debe ser positivo")
        self._client = client
        self._max_concurrent_per_domain = max_concurrent_per_domain
        self._default_min_delay_seconds = default_min_delay_seconds
        self._lease_ttl_seconds = lease_ttl_seconds
        self._poll_interval_seconds = poll_interval_seconds

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> RedisCoordinatedRateLimiter:
        return cls(client=redis_asyncio.Redis.from_url(url, decode_responses=True), **kwargs)

    async def acquire(self, url: str, min_delay_seconds: float | None = None) -> str:
        delay = self._default_min_delay_seconds if min_delay_seconds is None else min_delay_seconds
        if delay < 0:
            raise ValueError("min_delay_seconds no puede ser negativo")
        domain = extract_domain(url)
        token = str(uuid.uuid4())
        try:
            while True:
                delay_gate_open = delay <= 0 or await self._try_pass_delay_gate(domain, delay)
                if delay_gate_open and await self._try_acquire_semaphore(domain, token):
                    return token
                await asyncio.sleep(self._poll_interval_seconds)
        except RedisError as exc:
            raise CoordinatedRateLimiterError(
                f"fallo al adquirir rate limit coordinado para {url!r}: {exc}"
            ) from exc

    async def _try_pass_delay_gate(self, domain: str, delay_seconds: float) -> bool:
        gate_key = f"{_DELAY_GATE_PREFIX}{domain}"
        acquired = await self._client.set(gate_key, "1", nx=True, px=int(delay_seconds * 1000))
        return bool(acquired)

    async def _try_acquire_semaphore(self, domain: str, token: str) -> bool:
        key = f"{_SEMAPHORE_PREFIX}{domain}"
        now = time.time()
        async with self._client.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(key)
                    await pipe.zremrangebyscore(key, "-inf", now)
                    count = await pipe.zcard(key)
                    # redis-py no anota el tipo de retorno de Pipeline.multi()/
                    # .unwatch() en redis.asyncio.client -- llamadas legítimas
                    # sin contraparte tipada, no un import sin stubs.
                    pipe.multi()  # type: ignore[no-untyped-call]
                    if count < self._max_concurrent_per_domain:
                        pipe.zadd(key, {token: now + self._lease_ttl_seconds})
                        await pipe.execute()
                        return True
                    await pipe.unwatch()  # type: ignore[no-untyped-call]
                    return False
                except WatchError:
                    # otro worker modificó la clave entre watch() y multi():
                    # reintentar la sección crítica desde cero.
                    continue

    async def release(self, url: str, lease_token: str) -> None:
        domain = extract_domain(url)
        key = f"{_SEMAPHORE_PREFIX}{domain}"
        try:
            await self._client.zrem(key, lease_token)
        except RedisError as exc:
            raise CoordinatedRateLimiterError(
                f"fallo al liberar el lease {lease_token!r} para {url!r}: {exc}"
            ) from exc

    async def aclose(self) -> None:
        await self._client.aclose()
