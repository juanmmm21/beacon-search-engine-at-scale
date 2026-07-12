# beacon-search-engine-at-scale

Distributed-systems substrate for scaling
[`beacon-search-engine`](https://github.com/juanmmm21/beacon-search-engine) —
a from-scratch web search engine already completed as a 10-repo portfolio
over a ~180-page demo corpus — several orders of magnitude up, to a few
million pages over a bounded domain, on real multi-container
infrastructure instead of local processes.

## What this is

Phase 0 decided and built the shared substrate every later phase runs on top
of, before touching any crawling, indexing or ranking logic — object storage
for raw pages, extracted documents and built indexes; a message queue for
distributed crawl/indexing work; a service registry for dynamic shard
discovery; and the development orchestration to run all of it locally.
Phase 1, built on top of that substrate, is the first real workload: a
distributed web crawler that runs as N coordinated workers instead of one
process. See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full reasoning
behind every decision in both phases, the alternatives considered, and how
each piece of phase 0 is meant to evolve toward Kubernetes.

This repo does not implement an indexer or a query server, and it does not
modify any of the ten existing `beacon-search-engine` repositories —
`web-crawler-scheduler`'s own crawler logic is reused as a real package
dependency, unchanged (see "Distributed crawling (phase 1)" below). It is a
sibling repository that later phases extend.

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
  │  phase 0 — shared infrastructure substrate                 │
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
  ┌─────────────────────────────────────────────────────────┐
  │  phase 1 — distributed crawling (N CrawlWorker replicas)   │
  │                                                             │
  │   frontier = phase-0 MessageQueue (shared, not per-worker) │
  │   dedup    = SharedDeduplicator   (Redis SET, atomic claim)│
  │   rate limit = CoordinatedRateLimiter (Redis, per-domain)  │
  │   pages    -> phase-0 ObjectStorage, partitioned by         │
  │               date + URL-hash shard                        │
  │                                                             │
  │   crawl logic reused unchanged from web-crawler-scheduler: │
  │   AiohttpFetcher, RobotsCache, extract_outlinks             │
  └─────────────────────────────────────────────────────────┘
        |
        v
  future phases: distributed indexing, distributed query serving —
  built on top of this substrate, not implemented in this repo yet
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

## Distributed crawling (phase 1)

`src/beacon_scale_infra/crawl/` orchestrates several `CrawlWorker` instances
that share a frontier, a deduplication set and per-domain rate limits
through the phase-0 substrate, instead of each worker keeping that state to
itself the way `web-crawler-scheduler`'s single-process `CrawlPipeline`
does. See [`ARCHITECTURE.md`](ARCHITECTURE.md), section "Phase 1 —
Distributed crawling", for the full reasoning; in short:

| Concern | Single process (`web-crawler-scheduler`) | Distributed (this repo) |
|---|---|---|
| Frontier | `PriorityFrontier` (in-memory heap) | phase-0 `MessageQueue` (Redis Streams stream, FIFO) |
| Deduplication | `HashSetDeduplicator` (`seen()` + `mark_seen()`) | `SharedDeduplicator.try_claim()` (atomic `SADD`) |
| Rate limiting | `DomainRateLimiter` (per-process) | `CoordinatedRateLimiter` (Redis delay gate + lease semaphore) |
| Fetching, robots.txt, link extraction | `AiohttpFetcher`, `RobotsCache`, `extract_outlinks` | the same classes, reused unchanged as a package dependency |
| Raw page storage | local JSONL files | phase-0 `ObjectStorage`, partitioned by date + URL-hash shard |

Every `CrawlWorker` dependency is a protocol (`MessageQueue`, `ObjectStorage`,
`SharedDeduplicator`, `CoordinatedRateLimiter`, plus `web-crawler-scheduler`'s
own `PageFetcher`/`RobotsPolicy`) — the worker itself never picks a concrete
backend; whoever constructs it does (see `__main__.py`'s `crawl-worker`
subcommand).

### Launching N workers locally

```bash
docker compose up -d                              # MinIO, Redis, Consul, bucket bootstrap
BEACON_CRAWL_SEED_URLS=https://example.com/ \
docker compose up -d --scale crawl-worker=4        # 4 workers sharing one frontier
docker compose logs -f crawl-worker
```

Each replica gets a distinct container hostname, which `CrawlWorker` uses as
its `--worker-id` by default — no per-replica configuration needed, and no
separate seeding step: every worker publishes the seed URLs on startup, and
the atomic claim in `SharedDeduplicator` makes the redundant publishes from
the other replicas harmless (see `CrawlWorker._seed_frontier`'s docstring).
Workers stop on their own once the frontier has been idle for a few
consecutive polls — `docker compose ps` will show `crawl-worker` replicas
exit with code `0` once a bounded-domain crawl finishes.

To see the frontier actually split across workers rather than one replica
doing all the work, inspect the pages each one wrote — every stored
`CrawledPageRecord` carries a `fetched_by_worker` field:

```bash
docker compose exec minio mc alias set local http://localhost:9000 beacon-dev beacon-dev-secret
docker compose exec minio mc find local/beacon-scale-dev/crawl-pages --exec \
  "mc cat {} | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"fetched_by_worker\"])'"
```

Different worker IDs appearing across that output is the frontier sharing
working as intended: no single worker owns the whole crawl.

### Running a single worker without Docker

For a quick smoke test with no infrastructure running at all, use the
in-memory/local backends (this only makes sense for one worker per process —
`memory`/`local` backends never coordinate across separate `python -m`
invocations):

```bash
BEACON_CRAWL_SEED_URLS=https://example.com/ \
python -m beacon_scale_infra crawl-worker \
  --queue-backend memory --storage-backend local --coordination-backend memory \
  --local-storage-root .local-object-storage --idle-polls-before-shutdown 3
```

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
container. `docker compose up -d --scale crawl-worker=N` additionally builds
the `crawl-worker` image from this repo's `Dockerfile` and needs network
access at build time to install `web-crawler-scheduler` from its Git URL
(see `pyproject.toml`).

## CLI usage

This section covers the phase-0 substrate demo commands
(`storage-demo`/`queue-demo`/`registry-demo`); see "Distributed crawling
(phase 1)" above for `crawl-worker`. A demonstration CLI exercises each
piece of the substrate end to end, against either backend:

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
- **`crawl-worker` exits immediately with "no se especificaron URLs
  semilla":** pass at least one `--seed`, or set `BEACON_CRAWL_SEED_URLS`
  (comma-separated) — a worker with no seeds and an empty frontier has
  nothing to do and shuts down on its first idle poll.
- **`docker compose up -d --scale crawl-worker=N` only ever shows one
  replica doing work:** with `default_min_delay_seconds` at its default
  (`1.0`) and a single seed domain, the very first page is claimed by
  whichever replica happens to poll first — that is expected for a tiny
  site with only a handful of pages. Point `BEACON_CRAWL_SEED_URLS` at a
  site with more outlinks per page, or check `fetched_by_worker` across a
  longer-running crawl before concluding the frontier isn't splitting.
- **A `crawl-worker` replica that was killed mid-page never gets its
  in-flight URL retried by another replica:** Redis Streams pending entries
  are not reclaimed (`XCLAIM`) in this phase — same limitation already
  documented above for `queue-demo --backend redis`, now visible under real
  crawl load too (see `ARCHITECTURE.md`, phase 1, "Known limitation carried
  over from phase 0").

## License

MIT — see [`LICENSE`](LICENSE).
