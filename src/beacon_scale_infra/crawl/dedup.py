"""Deduplicación de URLs compartida entre varios workers de crawl.

`web_crawler_scheduler.urlnorm.HashSetDeduplicator` (crawler de un solo
proceso) expone `seen()` + `mark_seen()` como dos llamadas separadas:
correcto ahí porque nunca hay una tarea concurrente entre la comprobación y
el marcado salvo las propias tareas de ese mismo proceso, que cooperan en un
único event loop y nunca ceden el control entre `seen()` y `mark_seen()`.
Entre varios procesos/workers eso deja de ser cierto -- dos workers pueden
observar `seen() == False` para la misma URL al mismo tiempo y ambos
encolarla o descargarla. Por eso este módulo expone una única operación
atómica, `try_claim(url) -> bool`, que devuelve `True` solo al worker que
efectivamente gana el derecho a procesar esa URL.
"""

from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable

import redis.asyncio as redis_asyncio
from redis.exceptions import RedisError
from web_crawler_scheduler.urlnorm import url_hash

from beacon_scale_infra.errors import SharedDeduplicatorError

_DEFAULT_KEY = "beacon:crawl:seen-urls"


@runtime_checkable
class SharedDeduplicator(Protocol):
    """Reclama URLs de forma atómica para que como mucho un worker procese
    cada URL, sin importar cuántos workers compartan la misma frontera."""

    async def try_claim(self, url: str) -> bool:
        """Devuelve `True` si `url` no se había reclamado todavía (este
        worker gana la reclamación), `False` si ya la tenía otro worker -- o
        este mismo, ante una redelivery de la cola."""
        ...


class InMemorySharedDeduplicator:
    """Doble de desarrollo/test: mismo contrato atómico que la versión Redis,
    pero solo válido dentro de un proceso (`asyncio.Lock`) -- para testear la
    lógica de `CrawlWorker` sin infraestructura real, igual que el resto del
    sustrato (ver `ARCHITECTURE.md`, patrón protocolo + dos
    implementaciones). Sirve también para simular varios workers *dentro* del
    mismo proceso como tareas `asyncio` concurrentes que comparten la misma
    instancia, ejercitando la sección crítica igual que lo haría Redis.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._claimed: set[str] = set()

    async def try_claim(self, url: str) -> bool:
        digest = url_hash(url)
        async with self._lock:
            if digest in self._claimed:
                return False
            self._claimed.add(digest)
            return True


class RedisSharedDeduplicator:
    """Implementación real: un `SET` de Redis con hashes normalizados de URL.

    Un `SET` plano (`SADD`), no un Bloom filter: un Bloom filter reduciría
    memoria a costa de falsos positivos (URLs nunca crawleadas descartadas en
    silencio como "ya vistas"), y requeriría el módulo RedisBloom, que
    `docker-compose.yml` no levanta -- introducir infraestructura adicional
    sin una carga de trabajo que la justifique ya se descartó en fase 0 (ver
    `ARCHITECTURE.md`, sección "Orquestación"). Al volumen objetivo de esta
    fase (unos pocos millones de páginas), un `SET` de hashes sha256
    hexadecimales cabe cómodamente en el mismo nodo Redis único que ya aloja
    la cola de mensajes -- el mismo límite de escala que `ARCHITECTURE.md` ya
    acepta para `MessageQueue`.

    `SADD` es atómico en Redis (un único comando, sin round-trip de
    comprobación+escritura desde el cliente) y devuelve cuántos elementos se
    añadieron de verdad: `1` si `digest` no estaba en el set (este worker
    gana la reclamación), `0` si ya estaba.
    """

    def __init__(self, *, client: redis_asyncio.Redis, key: str = _DEFAULT_KEY) -> None:
        self._client = client
        self._key = key

    @classmethod
    def from_url(cls, url: str, *, key: str = _DEFAULT_KEY) -> RedisSharedDeduplicator:
        return cls(client=redis_asyncio.Redis.from_url(url, decode_responses=True), key=key)

    async def try_claim(self, url: str) -> bool:
        digest = url_hash(url)
        try:
            # redis-py tipa sadd() como Union[Awaitable[int], int] porque el
            # mismo mixin sirve al cliente síncrono y al asíncrono; con
            # redis.asyncio.Redis siempre es awaitable en tiempo de ejecución.
            added = await self._client.sadd(self._key, digest)  # type: ignore[misc]
        except RedisError as exc:
            raise SharedDeduplicatorError(
                f"fallo al reclamar {url!r} en el deduplicador compartido: {exc}"
            ) from exc
        return bool(added)

    async def aclose(self) -> None:
        await self._client.aclose()
