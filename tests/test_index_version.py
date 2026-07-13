"""Tests de la versión de contenido del índice (`index/index_version.py`):
determinismo del hash, sensibilidad a contenido y a nombre de fichero, y el
round-trip del marcador serializado."""

from __future__ import annotations

from pathlib import Path

import pytest

from beacon_scale_infra.index.index_version import (
    compute_index_version,
    index_version_marker_body,
    parse_index_version_marker,
)


def _write(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return path


def test_same_content_produces_the_same_version(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    first = _write(tmp_path / "a", "x.json", "hola")
    second = _write(tmp_path / "b", "x.json", "hola")
    assert compute_index_version([first]) == compute_index_version([second])


def test_different_content_produces_a_different_version(tmp_path: Path) -> None:
    original = _write(tmp_path, "stats.json", "contenido")
    changed = _write(tmp_path, "stats2.json", "contenido distinto")
    assert compute_index_version([original]) != compute_index_version([changed])


def test_renaming_a_file_changes_the_version(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    as_documents = _write(tmp_path / "a", "documents.jsonl", "igual")
    as_postings = _write(tmp_path / "b", "postings.jsonl", "igual")
    # El nombre base participa del hash: el mismo contenido bajo otro nombre
    # es otro artefacto para quien lo consume por nombre.
    assert compute_index_version([as_documents]) != compute_index_version([as_postings])


def test_file_order_matters_and_is_the_callers_contract(tmp_path: Path) -> None:
    first = _write(tmp_path, "a.json", "uno")
    second = _write(tmp_path, "b.json", "dos")
    assert compute_index_version([first, second]) != compute_index_version([second, first])


def test_marker_roundtrip() -> None:
    body = index_version_marker_body("abc123")
    assert parse_index_version_marker(body) == "abc123"


def test_marker_without_version_field_is_rejected() -> None:
    with pytest.raises(ValueError):
        parse_index_version_marker(b'{"format_version": 1}')


def test_unreadable_marker_is_rejected() -> None:
    with pytest.raises(ValueError):
        parse_index_version_marker(b"esto no es json")
