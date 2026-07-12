# beacon-search-engine-at-scale

Distributed-systems substrate for scaling
[`beacon-search-engine`](https://github.com/juanmmm21/beacon-search-engine) —
a from-scratch web search engine already completed as a 10-repo portfolio
over a ~180-page demo corpus — several orders of magnitude up, to a few
million pages over a bounded domain, on real multi-container
infrastructure instead of local processes.

## What this is

This is phase 0 of that scale-up: before touching any crawling, indexing or
ranking logic, this repository decides and builds the shared substrate every
later phase will run on top of — object storage for raw pages, extracted
documents and built indexes; a message queue for distributed crawl/indexing
work; a service registry for dynamic shard discovery; and the development
orchestration to run all of it locally. See [`ARCHITECTURE.md`](ARCHITECTURE.md)
for the full reasoning behind each decision, the alternatives considered,
and how each piece is meant to evolve toward Kubernetes.

This repo does not implement a crawler, an indexer, or a query server, and
it does not modify any of the ten existing `beacon-search-engine`
repositories. It is a sibling repository that later phases extend.

## Role in `beacon-search-engine`

```text
beacon-search-engine (10 repos, portfolio, closed scope)
  web-crawler-scheduler, html-content-extractor, inverted-index-builder,
  index-compression-codec, bm25-ranking-engine, pagerank-link-analysis,
  learning-to-rank-reranker, query-parser-autocomplete,
  distributed-index-sharding, beacon-search-console
        |
        | consumed unchanged, as real package dependencies or over the
        | same JSON contracts those repos already document
        v
beacon-search-engine-at-scale (this repo)
  ┌─────────────────────────────────────────────────────────┐
  │                 shared infrastructure substrate           │
  │                                                             │
  │   ObjectStorage        MessageQueue        ServiceRegistry │
  │  (pages, docs,       (crawl frontier,      (dynamic shard  │
  │   built indexes)      indexing jobs)         discovery)    │
  │                                                             │
  │   local: filesystem   local: in-memory     local: in-memory │
  │   real:  MinIO/S3      real: Redis Streams   real: Consul   │
  └─────────────────────────────────────────────────────────┘
        |
        v
  future phases: distributed crawl, distributed indexing,
  distributed query serving — built on top of this substrate,
  not implemented in this repo yet
```

## Goal and skills demonstrated

Designing the infrastructure layer of a distributed system *before* the
application logic that will run on it: choosing object storage, a message
queue, an orchestration story and a service registry for a specific,
bounded workload — and justifying every choice against a concrete
alternative (S3 vs. local disk, Redis Streams vs. Kafka, Consul vs. etcd,
Compose vs. Kubernetes) rather than defaulting to whatever is fashionable.
It also demonstrates the protocol-plus-two-implementations pattern that lets
the same calling code run against zero-dependency local doubles in tests and
against real infrastructure in development/production, and testing
infrastructure clients against faithful doubles of their real SDKs (`moto`
for S3, `fakeredis` for Redis Streams, a real `aiohttp.web` test server for
Consul's HTTP API) instead of skipping their tests for lack of live
services.

## How it works

Three `Protocol` interfaces (`ObjectStorage`, `MessageQueue`,
`ServiceRegistry`) in `src/beacon_scale_infra/protocols.py` describe the
substrate's contract without committing to a backend. Each has:

- a **local development implementation** with no network dependency
  (`storage/local.py`, `queue/memory.py`, `registry/local.py`) — deterministic,
  used in unit tests and for developing without Docker running;
- a **real implementation** (`storage/s3.py`, `queue/redis_streams.py`,
  `registry/consul.py`) against the services `docker-compose.yml` brings up
  locally, unchanged against real infrastructure later.

All operations are `async`, including the local filesystem/in-memory
backends, so calling code never branches between "local mode" and "real
mode" — only which concrete class it was constructed with differs.

## Architecture

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full decision record. In
short:

| Concern | Development | Real |
|---|---|---|
| Object storage | `LocalFilesystemObjectStorage` | `S3ObjectStorage` (MinIO / any S3-compatible endpoint) |
| Message queue | `InMemoryMessageQueue` | `RedisStreamsMessageQueue` |
| Service registry | `InMemoryServiceRegistry` | `ConsulServiceRegistry` |
| Orchestration | `docker-compose.yml` | Kubernetes (`Deployment`/`StatefulSet` per service — documented, not implemented here) |

## Requirements and installation

- Python `>=3.11`
- [Docker](https://www.docker.com/) and Docker Compose, to run the real
  backends locally (MinIO, Redis, Consul)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Bring up the development infrastructure:

```bash
docker compose up -d
```

This starts MinIO (API on `:9000`, console on `:9001`), Redis with AOF
persistence (`:6379`) and a single Consul dev agent (API/UI on `:8500`), and
creates the `beacon-scale-dev` bucket automatically via a bootstrap
container.

## CLI usage

A demonstration CLI exercises each piece of the substrate end to end,
against either backend:

```bash
# Local backends, no Docker required
python -m beacon_scale_infra storage-demo
python -m beacon_scale_infra queue-demo
python -m beacon_scale_infra registry-demo

# Real backends, against docker-compose services
BEACON_S3_ENDPOINT_URL=http://localhost:9000 \
BEACON_S3_ACCESS_KEY=beacon-dev \
BEACON_S3_SECRET_KEY=beacon-dev-secret \
python -m beacon_scale_infra storage-demo --backend s3

BEACON_REDIS_URL=redis://localhost:6379/0 \
python -m beacon_scale_infra queue-demo --backend redis

BEACON_CONSUL_BASE_URL=http://localhost:8500 \
python -m beacon_scale_infra registry-demo --backend consul
```

Each subcommand puts/gets/lists/deletes (storage), publishes/consumes/acks
(queue), or registers/discovers/deregisters (registry) a demonstration
object, message, or service instance, and prints every step.

## Data formats / interfaces exposed

- **Objects** are addressed by `(bucket, key)` with no directory semantics
  beyond a flat `prefix`, matching S3/MinIO's own model — see
  `ObjectMetadata` in `models.py`.
- **Queue messages** carry an opaque backend-assigned `message_id` and a
  JSON-serializable `payload` (`Mapping[str, Any]`) — future phases define
  their own payload shape (e.g. a crawl job, an indexing job) inside that
  mapping, the same way the ten original repos never share Python model
  imports across repo boundaries, only serialized contracts.
- **Service instances** are `(service_id, service_name, host, port,
  metadata)` — `ServiceRegistry.discover(service_name)` returns only
  instances whose liveness (TTL heartbeat locally, a Consul TTL health
  check for real) hasn't expired.

## Programmatic usage

```python
import asyncio

from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.registry.local import InMemoryServiceRegistry
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage


async def main() -> None:
    storage = LocalFilesystemObjectStorage("/tmp/beacon-scale-dev")
    await storage.put_object("beacon-scale-dev", "pages/0.html", b"<html>...</html>")
    print(await storage.get_object("beacon-scale-dev", "pages/0.html"))

    registry = InMemoryServiceRegistry()
    await registry.register(
        ServiceInstance(service_id="shard-0", service_name="index-shard", host="10.0.0.5", port=9300),
        ttl_seconds=30.0,
    )
    print(await registry.discover("index-shard"))


asyncio.run(main())
```

Swap `LocalFilesystemObjectStorage`/`InMemoryServiceRegistry` for
`S3ObjectStorage`/`ConsulServiceRegistry` (or `RedisStreamsMessageQueue` for
`MessageQueue`) to run the exact same calling code against real
infrastructure — no other code changes.

## Development

```bash
pytest
ruff check .
ruff format --check .
mypy --strict src/
```

Local backends are tested directly, with no mocks. Real backends are tested
against faithful doubles of their SDKs: `moto` for S3/MinIO, `fakeredis` for
Redis Streams, and a real `aiohttp.web` application (served via
`aiohttp.test_utils.TestServer`) standing in for Consul's HTTP API — chosen
over the `aioresponses` mocking library after it turned out to be
incompatible with current `aiohttp` versions (its response-building code
predates a breaking constructor change in `aiohttp`'s `ClientResponse`).

## Troubleshooting

- **`mc ready local` health check never turns healthy / `minio-init` keeps
  restarting:** MinIO can take a few seconds to initialize its data volume
  on first boot; `docker compose up -d` followed by `docker compose logs
  minio-init` shows whether the bucket bootstrap script actually ran.
- **`queue-demo --backend redis` hangs instead of returning quickly:**
  confirm the consumer group was actually created (`ensure_group` runs
  automatically before `publish`/`consume` in the CLI) and that no other
  process already consumed the same message under the same consumer group —
  Redis Streams consumer groups never redeliver an already-delivered message
  to a different consumer without an explicit `XCLAIM`, which this substrate
  does not implement yet (see `queue/redis_streams.py`).
- **`registry-demo --backend consul` can't discover an instance right after
  registering it:** a Consul TTL check starts in the `critical` state until
  its first pass; `ConsulServiceRegistry.register` already calls `heartbeat`
  once immediately for this reason — if instances still don't show up,
  check that the agent's clock and the TTL you passed actually leave enough
  room before the next heartbeat.
- **`mypy` complains about `mypy_boto3_s3`/`boto3-stubs` imports:** those are
  a `dev` extra, used only under `TYPE_CHECKING` — install with
  `pip install -e ".[dev]"`, not just the base package.

## License

MIT — see [`LICENSE`](LICENSE).
