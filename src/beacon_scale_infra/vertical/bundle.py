"""Exporta el estado publicado del vertical a un *bundle* en disco que la
consola (`beacon-search-console`) carga y recarga en caliente.

La consola no puede leer el almacenamiento de objetos de este repo: este
repo depende de ella, no al revés. El contrato entre ambos es un directorio
con un formato documentado (bundle del vertical, formato v1):

```text
<root>/
├── current -> bundle-<versión[:16]>     # enlace simbólico, se cambia de forma atómica
└── bundle-<versión[:16]>/
    ├── manifest.json     # format, format_version, index_version, created_at, documents, sources
    ├── index/            # índice segmentado de inverted-index-builder (segments.json v1)
    ├── documents.jsonl   # un documento vivo por línea: doc_id, url, title, main_text, source_id
    └── sources.json      # fuentes con nombre, licencia, URL de licencia y atribución
```

Cada exportación escribe un directorio nuevo y solo al final mueve
`current`; un lector que abrió el bundle anterior sigue leyéndolo entero.
Se conservan los dos últimos bundles y se borran los demás.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from inverted_index_builder.segments import SegmentedIndex, SegmentedIndexError

from beacon_scale_infra.errors import IncrementalIndexingError, ObjectNotFoundError
from beacon_scale_infra.incremental.indexer import document_key
from beacon_scale_infra.incremental.snapshot import (
    SNAPSHOT_POINTER,
    parse_snapshot_pointer,
    restore_snapshot,
)
from beacon_scale_infra.protocols import ObjectStorage
from beacon_scale_infra.vertical.sources import VerticalSources

logger = logging.getLogger(__name__)

BUNDLE_FORMAT: Final[str] = "beacon-vertical-bundle"
BUNDLE_FORMAT_VERSION: Final[int] = 1
CURRENT_LINK: Final[str] = "current"
_KEEP_BUNDLES: Final[int] = 2


@dataclass(frozen=True, slots=True)
class BundleExport:
    path: Path
    index_version: str
    documents: int
    missing_documents: int
    reused: bool


def current_bundle_version(root: Path) -> str | None:
    manifest = root / CURRENT_LINK / "manifest.json"
    if not manifest.exists():
        return None
    return str(json.loads(manifest.read_text("utf-8"))["index_version"])


async def export_bundle(
    storage: ObjectStorage,
    bucket: str,
    index_prefix: str,
    document_store_prefix: str,
    sources: VerticalSources,
    root: Path,
) -> BundleExport | None:
    """Exporta la última instantánea publicada. `None` si aún no hay
    ninguna; si `current` ya es esa versión, no hace nada."""
    try:
        pointer = await storage.get_object(bucket, f"{index_prefix}/{SNAPSHOT_POINTER}")
    except ObjectNotFoundError:
        return None
    version, manifest_sha256 = parse_snapshot_pointer(pointer)
    target = root / f"bundle-{version[:16]}"
    if current_bundle_version(root) == version:
        return BundleExport(target, version, 0, 0, reused=True)

    root.mkdir(parents=True, exist_ok=True)
    staging = root / f".staging-{version[:16]}"
    if staging.exists():
        shutil.rmtree(staging)
    if not await restore_snapshot(
        storage,
        bucket,
        index_prefix,
        staging / "index",
        expected_manifest_sha256=manifest_sha256,
    ):
        return None
    try:
        index = SegmentedIndex.open(staging / "index")
    except SegmentedIndexError as error:
        raise IncrementalIndexingError(f"instantánea ilegible: {error}") from error

    documents = 0
    missing = 0
    lines: list[tuple[int, str]] = []
    live_urls = _live_urls(staging / "index")
    for url in sorted(live_urls):
        doc_id = index.live_doc_id(url)
        if doc_id is None:
            continue
        try:
            raw = await storage.get_object(bucket, document_key(document_store_prefix, url))
        except ObjectNotFoundError:
            # Un documento vivo sin texto: se exporta el índice igualmente y
            # la consola omitirá ese resultado; se cuenta para que se vea.
            missing += 1
            continue
        record = json.loads(raw)
        lines.append(
            (
                doc_id,
                json.dumps(
                    {
                        "doc_id": doc_id,
                        "url": url,
                        "title": record.get("title") or url,
                        "main_text": record.get("main_text", ""),
                        "source_id": record.get("source_id"),
                    },
                    ensure_ascii=False,
                ),
            )
        )
        documents += 1
    lines.sort()
    (staging / "documents.jsonl").write_text(
        "".join(line + "\n" for _, line in lines), encoding="utf-8"
    )
    (staging / "sources.json").write_text(
        json.dumps(
            {
                "sources": [source.public_json() for source in sources.enabled],
                "disabled": [
                    {"source_id": source.source_id, "reason": source.disabled_reason}
                    for source in sources.sources
                    if not source.enabled
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (staging / "manifest.json").write_text(
        json.dumps(
            {
                "format": BUNDLE_FORMAT,
                "format_version": BUNDLE_FORMAT_VERSION,
                "index_version": version,
                "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "documents": documents,
                "missing_documents": missing,
                "sources": [source.source_id for source in sources.enabled],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    if target.exists():
        shutil.rmtree(target)
    staging.rename(target)
    _point_current(root, target.name)
    _prune(root, keep=target.name)
    return BundleExport(target, version, documents, missing, reused=False)


def _live_urls(index_root: Path) -> set[str]:
    """URLs vivas leyendo los `documents.jsonl` de cada segmento y quitando
    los `doc_id` de sus tombstones — el formato segmentado v1 documentado en
    `inverted-index-builder`."""
    manifest = json.loads((index_root / "segments.json").read_text("utf-8"))
    urls: set[str] = set()
    for segment in manifest["segments"]:
        deleted: set[int] = set()
        if segment.get("deletes_file"):
            data = json.loads((index_root / segment["deletes_file"]).read_text("utf-8"))
            deleted = {int(doc_id) for doc_id in data["deleted_doc_ids"]}
        with (index_root / segment["name"] / "documents.jsonl").open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    record = json.loads(line)
                    if int(record["doc_id"]) not in deleted:
                        urls.add(str(record["url"]))
    return urls


def _point_current(root: Path, name: str) -> None:
    temporary = root / f".{CURRENT_LINK}-{os.getpid()}"
    if temporary.is_symlink() or temporary.exists():
        temporary.unlink()
    temporary.symlink_to(name)
    os.replace(temporary, root / CURRENT_LINK)


def _prune(root: Path, *, keep: str) -> None:
    bundles = sorted(
        (path for path in root.glob("bundle-*") if path.is_dir()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    survivors = {keep} | {path.name for path in bundles[:_KEEP_BUNDLES]}
    for path in bundles:
        if path.name not in survivors:
            logger.info("borrando bundle antiguo %s", path)
            shutil.rmtree(path)
