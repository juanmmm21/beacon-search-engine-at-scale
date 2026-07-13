"""Versión de contenido del índice global (fase 3): un hash sha256 sobre los
artefactos exactos que la fase de consulta sirve (los cuatro ficheros de
`inverted-index-builder` fusionados más el corpus alineado por `doc_id`).

Es una *versión de contenido*, no un timestamp ni un contador de builds: dos
ejecuciones de `build-index` sobre el mismo corpus de fase 2 producen
byte-a-byte el mismo índice (determinismo de fase 3, ver `ARCHITECTURE.md`,
fase 3, sección 4) y por tanto la misma versión -- correcto, porque los
resultados de búsqueda cacheados bajo esa versión siguen siendo válidos. Solo
un corpus distinto (recrawl, re-extracción) produce una versión distinta, y
eso es exactamente el evento que debe invalidar la caché de resultados de la
consola (ver `ARCHITECTURE.md`, fase 6, decisión de invalidación).

El marcador (`index_version.json`) se escribe como objeto hermano de cada
prefijo de índice publicado (`search-index/`, `search-index-compressed/`), lo
propaga `shard-index` (fase 5) a `shard-index/index_version.json`, y cada
réplica de shard lo anuncia en su metadata de registro -- la cadena completa
que permite a la consola saber qué versión están sirviendo los shards *ahora
mismo*, no cuál había en el bucket al arrancar.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Final

INDEX_VERSION_MARKER_BASENAME: Final[str] = "index_version.json"

_MARKER_FORMAT_VERSION: Final[int] = 1
_HASH_CHUNK_BYTES: Final[int] = 1 << 20


def compute_index_version(paths: Sequence[Path]) -> str:
    """Hash sha256 (hex) del contenido de `paths`, en el orden recibido,
    incluyendo el nombre base de cada fichero en el hash: renombrar un
    artefacto sin cambiar su contenido también es un índice distinto desde el
    punto de vista de quien lo consume por nombre."""
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\x00")
        with path.open("rb") as handle:
            while chunk := handle.read(_HASH_CHUNK_BYTES):
                digest.update(chunk)
        digest.update(b"\x00")
    return digest.hexdigest()


def index_version_marker_body(index_version: str) -> bytes:
    return json.dumps(
        {"format_version": _MARKER_FORMAT_VERSION, "index_version": index_version},
        ensure_ascii=False,
    ).encode("utf-8")


def parse_index_version_marker(raw: bytes) -> str:
    """Extrae `index_version` de un marcador serializado. Levanta `ValueError`
    (con contexto) ante JSON ilegible o un marcador sin el campo esperado --
    el llamador decide con qué excepción tipada de su fase envolverlo."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"marcador de versión de índice ilegible: {exc}") from exc
    version = data.get("index_version") if isinstance(data, dict) else None
    if not isinstance(version, str) or not version:
        raise ValueError(
            f"marcador de versión de índice sin campo 'index_version' no vacío: {raw[:200]!r}"
        )
    return version
