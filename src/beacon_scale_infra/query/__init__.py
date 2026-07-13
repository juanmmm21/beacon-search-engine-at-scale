"""Fase 5: query serving distribuido.

Lleva los shards de `distributed-index-sharding` (subprocesos locales
gestionados por `LocalShardCluster`) a réplicas escalables en infraestructura
real, descubiertas dinámicamente vía el `ServiceRegistry` de fase 0 en vez de
una lista fija de `ShardTarget` -- ver `ARCHITECTURE.md`, fase 5.
"""

from __future__ import annotations
