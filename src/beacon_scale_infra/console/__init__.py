"""Fase 6: la consola de búsqueda (la app insignia `beacon-search-console`)
servida sobre el clúster real de fase 5 -- API FastAPI multi-réplica con el
mismo contrato versionado `/api/v1/...`, descubrimiento dinámico de shards,
caché compartida de resultados en Redis y resolución de snippets contra las
particiones reales de fase 2 (ver `ARCHITECTURE.md`, fase 6)."""
