"""Indexación distribuida (fase 3): fusiona los documentos particionados que
la fase 2 escribió en el almacenamiento compartido en un único índice
invertido global, en el mismo formato en disco que ya produce
`inverted-index-builder` -- ver `ARCHITECTURE.md`, fase 3.
"""

from __future__ import annotations
