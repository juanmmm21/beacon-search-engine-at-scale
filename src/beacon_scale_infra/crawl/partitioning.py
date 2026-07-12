"""Particionado de las páginas crudas dentro del almacenamiento de objetos.

Combina dos ejes de partición, cada uno resolviendo un problema distinto:

- **Por fecha** (`YYYY-MM-DD` de `fetched_at`, UTC): útil para retención y
  para poder razonar sobre "qué se crawleó tal día" sin listar el bucket
  entero -- el mismo motivo por el que un pipeline de datos convencional
  particiona por fecha de ingesta.
- **Por shard de hash de URL** (`url_hash(url)` módulo `num_hash_shards`):
  reparte las páginas de un mismo día entre `num_hash_shards` prefijos
  distintos para que un crawl grande de un solo día no concentre millones de
  objetos bajo un único prefijo -- listar un prefijo de S3/MinIO pagina por
  claves ordenadas lexicográficamente, así que miles de páginas de dominios
  distintos cayendo en el mismo prefijo de fecha sin más partición
  degradaría el paralelismo de cualquier lector posterior (indexación
  distribuida) que quiera repartirse el trabajo por prefijo.

El shard se deriva del hash normalizado de la URL (no de `url_hash` truncado
a un entero pequeño y then módulo, sino de sus primeros bytes interpretados
como entero) para que la distribución entre shards sea uniforme y estable:
la misma URL cae siempre en el mismo shard, sin importar qué worker la
procesó ni cuándo.
"""

from __future__ import annotations

from datetime import datetime

from web_crawler_scheduler.urlnorm import url_hash


def hash_shard_for_url(url: str, num_hash_shards: int) -> int:
    """Shard determinista en `[0, num_hash_shards)` para `url`."""
    if num_hash_shards <= 0:
        raise ValueError("num_hash_shards debe ser positivo")
    digest = url_hash(url)
    return int(digest[:8], 16) % num_hash_shards


def object_key_for_page(
    url: str,
    fetched_at: datetime,
    *,
    prefix: str = "crawl-pages",
    num_hash_shards: int = 16,
) -> str:
    """Clave de objeto para la página crawleada de `url`, con el formato
    `<prefix>/date=<YYYY-MM-DD>/shard=<NNN>/<url_hash>.json`.

    `fetched_at` se interpreta en UTC (se asume ya normalizada a UTC por el
    llamador, igual que `CrawledPageRecord.fetched_at`); dos workers en husos
    horarios distintos deben producir la misma clave para la misma página.
    """
    shard = hash_shard_for_url(url, num_hash_shards)
    date_prefix = fetched_at.strftime("%Y-%m-%d")
    digest = url_hash(url)
    return f"{prefix}/date={date_prefix}/shard={shard:03d}/{digest}.json"
