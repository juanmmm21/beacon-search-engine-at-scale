"""Lista curada de fuentes del buscador vertical (`deploy/vertical-sources.json`).

Cada fuente declara qué se rastrea (semillas y prefijos de URL permitidos),
cuánto (`max_pages`, además del presupuesto global) y bajo qué licencia se
publica su contenido. La licencia no es decorativa: la consola la muestra
junto a cada resultado (atribución + enlace), que es lo que piden CC-BY-SA
(MDN) o la PSF, y una fuente sin licencia conocida no entra en la lista.

`robots.txt` y `Crawl-delay` no se configuran aquí: los aplica siempre
`web-crawler-scheduler`, sin opción de desactivarlo desde este módulo.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

_FORMAT_VERSION: Final[int] = 1


class VerticalSourcesError(ValueError):
    """Fichero de fuentes ausente, de otra versión o inválido."""


@dataclass(frozen=True, slots=True)
class DocSource:
    source_id: str
    name: str
    seeds: tuple[str, ...]
    allowed_prefixes: tuple[str, ...]
    max_pages: int
    license: str
    license_url: str
    attribution: str
    enabled: bool = True
    disabled_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.source_id or not self.source_id.replace("-", "").isalnum():
            raise VerticalSourcesError(f"source_id inválido: {self.source_id!r}")
        if not self.seeds or not self.allowed_prefixes:
            raise VerticalSourcesError(f"{self.source_id}: faltan semillas o prefijos")
        if self.max_pages <= 0:
            raise VerticalSourcesError(f"{self.source_id}: max_pages debe ser positivo")
        if not self.license or not self.license_url:
            raise VerticalSourcesError(f"{self.source_id}: una fuente necesita su licencia")
        for seed in self.seeds:
            if not seed.startswith(self.allowed_prefixes):
                raise VerticalSourcesError(f"{self.source_id}: la semilla {seed} queda fuera")
        if not self.enabled and not self.disabled_reason:
            raise VerticalSourcesError(f"{self.source_id}: desactivada sin motivo")

    def owns(self, url: str) -> bool:
        return url.startswith(self.allowed_prefixes)

    def public_json(self) -> dict[str, Any]:
        """Lo que la consola necesita mostrar: nombre, licencia y atribución."""
        return {
            "source_id": self.source_id,
            "name": self.name,
            "license": self.license,
            "license_url": self.license_url,
            "attribution": self.attribution,
            "home_url": self.seeds[0],
        }


@dataclass(frozen=True, slots=True)
class VerticalSources:
    total_page_budget: int
    sources: tuple[DocSource, ...]

    @property
    def enabled(self) -> tuple[DocSource, ...]:
        return tuple(source for source in self.sources if source.enabled)

    def source_for(self, url: str) -> DocSource | None:
        for source in self.sources:
            if source.owns(url):
                return source
        return None

    def budgets(self) -> dict[str, int]:
        """Páginas por fuente activa: su `max_pages`, recortado en orden de
        declaración hasta agotar `total_page_budget`."""
        remaining = self.total_page_budget
        budgets: dict[str, int] = {}
        for source in self.enabled:
            budgets[source.source_id] = max(0, min(source.max_pages, remaining))
            remaining -= budgets[source.source_id]
        return budgets


def load_sources(path: Path) -> VerticalSources:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise VerticalSourcesError(f"no se pudo leer {path}: {error}") from error
    if int(data.get("format_version", -1)) != _FORMAT_VERSION:
        raise VerticalSourcesError(f"{path}: format_version no soportado")
    try:
        sources = tuple(
            DocSource(
                source_id=str(entry["source_id"]),
                name=str(entry["name"]),
                seeds=tuple(str(seed) for seed in entry["seeds"]),
                allowed_prefixes=tuple(str(prefix) for prefix in entry["allowed_prefixes"]),
                max_pages=int(entry["max_pages"]),
                license=str(entry["license"]),
                license_url=str(entry["license_url"]),
                attribution=str(entry["attribution"]),
                enabled=bool(entry.get("enabled", True)),
                disabled_reason=(
                    None if entry.get("disabled_reason") is None else str(entry["disabled_reason"])
                ),
            )
            for entry in data["sources"]
        )
        budget = int(data["total_page_budget"])
    except (KeyError, TypeError) as error:
        raise VerticalSourcesError(f"{path}: entrada incompleta ({error})") from error
    ids = [source.source_id for source in sources]
    if len(ids) != len(set(ids)):
        raise VerticalSourcesError(f"{path}: source_id repetido")
    if budget <= 0:
        raise VerticalSourcesError(f"{path}: total_page_budget debe ser positivo")
    return VerticalSources(total_page_budget=budget, sources=sources)
