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
process. Phase 2, built on top of phase 1's output, turns those raw crawled
pages into clean, indexable documents with N coordinated extraction workers
consuming pages as the crawler produces them, not in a batch at the end.
Phase 3, built on top of phase 2's partitioned output, is a one-shot
map-reduce job that assigns a global, deterministic `doc_id` to every
document and merges the whole corpus into a single inverted index, in the
same on-disk format a single-machine `inverted-index-builder` run would
produce. Phase 4, built on top of phase 3's index and phase 1's raw pages,
computes PageRank authority scores over the whole corpus as a one-shot job —
and, after measuring the real memory/time cost of the reused algorithm at
this project's target scale, runs it on a single process rather than
building a distributed power-iteration engine the corpus does not need (see
[`ARCHITECTURE.md`](ARCHITECTURE.md), "Phase 4 — PageRank", for the
measurements). See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full
reasoning behind every decision across all four phases, the alternatives
considered, and how each piece of phase 0 is meant to evolve toward
Kubernetes.

This repo does not implement a query server, and it does not modify any of
the ten existing `beacon-search-engine` repositories — `web-crawler-scheduler`'s
own crawler logic (see "Distributed crawling (phase 1)" below),
`html-content-extractor`'s own extraction logic (see "Distributed extraction
(phase 2)" below), `inverted-index-builder`'s / `index-compression-codec`'s
own indexing and compression logic (see "Distributed indexing (phase 3)"
below), and `pagerank-link-analysis`'s own ranking algorithm (see "PageRank
(phase 4)" below) are all reused as real package dependencies, unchanged. It
is a sibling repository that later phases extend.

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
  ┌─────────────────────────────────────────────────────────┐
  │  phase 2 — distributed extraction (N ExtractWorker replicas)│
  │                                                             │
  │   trigger  = phase-0 MessageQueue (CrawlWorker publishes   │
  │              a job per page, ExtractWorker consumes it)    │
  │   no dedup / rate limiter needed -- every page independent │
  │   documents -> phase-0 ObjectStorage, partitioned by        │
  │                worker_id, plus a partitioned manifest       │
  │                                                             │
  │   extraction logic reused unchanged from                   │
  │   html-content-extractor: resolve_encoding, parse_html,    │
  │   extract_main_content, extract_metadata, normalize_text   │
  └─────────────────────────────────────────────────────────┘
        |
        v
  ┌─────────────────────────────────────────────────────────┐
  │  phase 3 — distributed indexing (one batch job, map-reduce) │
  │                                                             │
  │   doc_id  = contiguous per-partition range, from the       │
  │             phase-2 manifest (no centralized counter)      │
  │   map     = IndexBuilder.build per partition + remap        │
  │   reduce  = merge.py concatenates disjoint, sorted          │
  │             partial indexes (no merge-sort needed)          │
  │   output  -> phase-0 ObjectStorage: search-index/           │
  │              (inverted-index-builder format, unmodified),   │
  │              search-index-compressed/ (index-compression-   │
  │              codec, unmodified), search-index/corpus/       │
  │              documents.jsonl (doc_id-aligned, for snippets) │
  │                                                             │
  │   indexing/compression logic reused unchanged from          │
  │   inverted-index-builder and index-compression-codec        │
  └─────────────────────────────────────────────────────────┘
        |
        v
  ┌─────────────────────────────────────────────────────────┐
  │  phase 4 — PageRank (one batch job, single process)          │
  │                                                             │
  │   capacity check first: measured ~180 B/edge + ~1 KB/doc,  │
  │   worst case at this project's scale <50 GB / <15 min --   │
  │   no distributed power iteration built (see ARCHITECTURE)  │
  │   link graph  <- phase-0 ObjectStorage, crawl-pages/,       │
  │                  concurrent bounded fan-out (many small     │
  │                  objects, unlike phase 3's few big parts)   │
  │   doc_id space <- phase-3 search-index/documents.jsonl      │
  │   output      -> phase-0 ObjectStorage: pagerank-scores/    │
  │                  (doc_id -> pagerank_score, unmodified      │
  │                  pagerank-link-analysis on-disk format)     │
  │                                                             │
  │   PageRank algorithm reused unchanged from                  │
  │   pagerank-link-analysis: resolver, graph builder,          │
  │   sparse power iteration, on-disk score format               │
  └─────────────────────────────────────────────────────────┘
        |
        v
  future phases: distributed query serving —
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

## Distributed extraction (phase 2)

`src/beacon_scale_infra/extract/` orchestrates several `ExtractWorker`
instances that turn the raw pages phase 1 wrote into clean, indexable
documents — consuming pages *as the crawler produces them*, through a queue
`CrawlWorker` publishes to, rather than reading a `pages.jsonl` in one batch
the way `html-content-extractor`'s single-process `ExtractionPipeline` does.
See [`ARCHITECTURE.md`](ARCHITECTURE.md), section "Phase 2 — Distributed
extraction", for the full reasoning; in short:

| Concern | Single process (`html-content-extractor`) | Distributed (this repo) |
|---|---|---|
| Trigger | reads a whole `pages.jsonl` at once | `CrawlWorker` publishes one job per page, as it's crawled |
| Coordination between workers | N/A (one process) | none needed — every page is independent, only the phase-0 `MessageQueue`'s consumer groups split the work |
| Extraction logic | `ExtractionPipeline` (encoding, parsing, density, metadata, normalization) | the same underlying functions, reused unchanged as a package dependency |
| Document storage | two local files (`documents.jsonl`, `discarded.jsonl`) | phase-0 `ObjectStorage`, partitioned by `worker_id` into many immutable part files |
| Knowing what was written | read the files directly | a partitioned manifest (`extract/manifest.py`), aggregated across all partitions on read |

Unlike `CrawlWorker`, `ExtractWorker` needs no deduplicator and no rate
limiter: extracting an already-downloaded page makes no outbound request
and shares no resource with extracting any other page — the easiest stage
of the whole pipeline to parallelize. Every `ExtractWorker` dependency is a
protocol (`MessageQueue`, `ObjectStorage`) — same pattern as `CrawlWorker`;
whoever constructs it (`__main__.py`'s `extract-worker` subcommand) decides
the concrete backend.

### Launching N workers locally

```bash
docker compose up -d                                     # MinIO, Redis, Consul, bucket bootstrap
BEACON_CRAWL_SEED_URLS=https://example.com/ \
docker compose up -d --scale crawl-worker=4 --scale extract-worker=4
docker compose logs -f extract-worker
```

Each replica gets a distinct container hostname, used as `--worker-id` by
default — the same default `CrawlWorker` uses, and for the same reason: no
per-replica configuration needed, and a restarted container gets a fresh
partition instead of reusing (and colliding with) a previous one (see
`ARCHITECTURE.md`, phase 2, "Known limitations"). `extract-worker` needs no
seed URLs of its own — it only reacts to jobs `crawl-worker` publishes.

To see documents actually split across workers, inspect the manifest
fragments each one wrote:

```bash
docker compose exec minio mc alias set local http://localhost:9000 beacon-dev beacon-dev-secret
docker compose exec minio mc cat local/beacon-scale-dev/extracted-documents/manifest/partition=<worker-id>.json
```

Different `worker-id` fragments appearing under `extracted-documents/manifest/`
is the extraction work sharing as intended.

### Running a single worker without Docker

```bash
python -m beacon_scale_infra extract-worker \
  --queue-backend memory --storage-backend local \
  --local-storage-root .local-object-storage --idle-polls-before-shutdown 3
```
Same caveat as `crawl-worker`: `memory`/`local` backends never coordinate
across separate process invocations, so this only makes sense for a single
worker with no `docker compose` running.

## Distributed indexing (phase 3)

`src/beacon_scale_infra/index/` turns the documents phase 2 partitioned
across N workers into a single global inverted index, in the exact on-disk
format `inverted-index-builder` already defines and `index-compression-codec`
already consumes. Unlike phases 1–2, this is a one-shot batch job
(`build-index`), not a service you scale with `--scale`: it needs phase 2's
manifest counts to be final before it can assign `doc_id`s (see
[`ARCHITECTURE.md`](ARCHITECTURE.md), section "Phase 3 — Distributed
indexing", for the full reasoning).

| Concern | Single process (`inverted-index-builder`) | Distributed (this repo) |
|---|---|---|
| `doc_id` assignment | 0-based counter, line position in one `documents.jsonl` | contiguous per-partition range from the phase-2 manifest, offset added after building each partition locally |
| Indexing logic | `IndexBuilder.build` | the same class, reused unchanged, once per partition (the *map* step) |
| Combining results | N/A (one process) | `merge.py` concatenates already-sorted, disjoint partial indexes (the *reduce* step) — no merge-sort needed |
| Output format | `write_index` (`manifest.json`, `documents.jsonl`, `postings.jsonl`, `stats.json`) | the same function, called once on the merged index |
| Compression | `index-compression-codec`, run by hand afterward | the same package, invoked automatically at the end of the pipeline |

It also writes one extra artifact with no equivalent in the single-machine
pipeline: a global "corpus" file (`search-index/corpus/documents.jsonl` by
default) carrying the full extracted text (`main_text` included, unlike
`inverted-index-builder`'s own stripped-down `documents.jsonl`), assembled so
that line position exactly equals global `doc_id`. This is what preserves
`beacon-search-console`'s snippet resolution (`doc_id → text` by array
position) across a corpus that no longer lives in one file — see
`ARCHITECTURE.md`, phase 3, section 5.

### Running it

```bash
python -m beacon_scale_infra build-index \
  --storage-backend s3 --bucket beacon-scale-dev
```

Requires `BEACON_S3_ENDPOINT_URL`/`BEACON_S3_ACCESS_KEY`/`BEACON_S3_SECRET_KEY`
(same variables as `crawl-worker`/`extract-worker` with `--storage-backend
s3`) and phase 2's `extract-worker` replicas to have already finished. Add
`--no-compress` to skip the `index-compression-codec` pass and inspect the
uncompressed `search-index/` output directly. Without Docker, point
`--storage-backend local --local-storage-root` at the same directory
`extract-worker --storage-backend local` wrote to.

## PageRank (phase 4)

`src/beacon_scale_infra/pagerank/` computes link-authority scores over the
whole corpus phase 3 indexed. Unlike phases 1–3, this phase's core algorithm
is not per-document parallel work — it is a single sparse power iteration
over the entire adjacency matrix at once — so before designing anything
distributed, `ARCHITECTURE.md` measures the real memory/time cost of running
`pagerank-link-analysis`'s own reused code unmodified at graduated scale, and
extrapolates to this project's target (3–5M documents): worst case under
50 GB peak RSS and under 15 minutes of single-core wall time, comfortably
inside one large-memory cloud instance. See
[`ARCHITECTURE.md`](ARCHITECTURE.md), section "Phase 4 — PageRank", for the
full measurements and the extrapolation table. `compute-pagerank`, like
`build-index`, is a one-shot batch job, not a service you scale with
`--scale`.

| Concern | Single process (`pagerank-link-analysis`) | This phase |
|---|---|---|
| `doc_id` resolution | `JsonlDocumentIdResolver` over a single-machine `documents.jsonl` | the same class, reused unchanged, pointed at phase 3's `search-index/documents.jsonl` (already satisfies its invariants — see `ARCHITECTURE.md`, phase 3, section 5) |
| Link graph input | a single local `link_graph.jsonl` from `web-crawler-scheduler` | materialized from phase 1's `crawl-pages/` (one object per page) via bounded-concurrency fan-out (`link_graph_materializer.py`) — sequential reads would make network round-trips, not compute, this phase's real bottleneck |
| Graph building, power iteration | `build_adjacency_matrix`, `compute_pagerank` | the same functions, reused unchanged, called on the materialized local files |
| Output format | `write_pagerank_output` (`manifest.json`, `pagerank_scores.jsonl`, `convergence.json`) | the same function, called once, uploaded to phase-0 `ObjectStorage` |
| Distribution across machines | N/A (one process) | none — section 1 of `ARCHITECTURE.md`'s phase-4 entry measures that this project's corpus fits comfortably on one large machine |

### Running it

```bash
python -m beacon_scale_infra compute-pagerank \
  --storage-backend s3 --bucket beacon-scale-dev
```

Requires the same `BEACON_S3_*` variables as `build-index`, and
`build-index` (phase 3) to have already finished — `compute-pagerank` reads
`search-index/documents.jsonl` directly. Tune `--max-concurrent-reads`
(default `64`) to the object storage endpoint's real capacity when
materializing the link graph from a large `crawl-pages/`. Without Docker,
point `--storage-backend local --local-storage-root` at the same directory
`build-index --storage-backend local` wrote to.

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
container. `docker compose up -d --scale crawl-worker=N --scale
extract-worker=M` additionally builds the shared `crawl-worker`/`extract-worker`
image from this repo's `Dockerfile` and needs network access at build time
to install `web-crawler-scheduler` and `html-content-extractor` from their
Git URLs (see `pyproject.toml`).

## CLI usage

This section covers the phase-0 substrate demo commands
(`storage-demo`/`queue-demo`/`registry-demo`); see "Distributed crawling
(phase 1)" above for `crawl-worker` and "Distributed extraction (phase 2)"
above for `extract-worker`. A demonstration CLI exercises each piece of the
substrate end to end, against either backend:

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
- **The global index** (`search-index/` by default) is exactly
  `inverted-index-builder`'s own on-disk format (`manifest.json`,
  `documents.jsonl`, `postings.jsonl`, `stats.json`) — see that repo's own
  `README.md` for the field-by-field contract. `doc_id` is no longer a line
  position in a single file; see `ARCHITECTURE.md`, phase 3, section 4, for
  the exact rule. The compressed variant
  (`search-index-compressed/` by default) is `index-compression-codec`'s own
  format, unmodified.
- **The corpus file** (`search-index/corpus/documents.jsonl` by default)
  carries the full `ExtractedDocument` fields (including `main_text`) phase
  2 produced, one line per document, ordered so line position equals global
  `doc_id` — see "Distributed indexing (phase 3)" above.
- **PageRank scores** (`pagerank-scores/` by default) are exactly
  `pagerank-link-analysis`'s own on-disk format (`manifest.json`,
  `pagerank_scores.jsonl` — `doc_id -> pagerank_score`, sorted ascending —
  and `convergence.json`), unmodified — see "PageRank (phase 4)" above.

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
- **`extract-worker` reports `pages_missing > 0`:** it consumed a job
  referencing a page that no longer exists at that `(bucket, key)` in object
  storage — either the `publish` after a `put_object` raced ahead of
  eventual consistency (unlikely against MinIO/S3, which are read-after-write
  consistent for new keys) or the object was deleted out-of-band. The worker
  logs the missing key and moves on; see `ARCHITECTURE.md`, phase 2, "Known
  limitations".
- **`extracted-documents/manifest/` only shows fragments for some of the
  `extract-worker` replicas:** a replica that never received any job from
  the shared queue never flushes and therefore never writes its manifest
  fragment — check `docker compose logs extract-worker` for which replicas
  actually consumed messages; with very few pages in flight, one replica
  polling first can plausibly claim all of them (same dynamic already noted
  above for `crawl-worker` and a single seed domain).
- **`build-index` produces an index with fewer documents than
  `extracted-documents/manifest/` reports:** the manifest was still being
  updated by a running `extract-worker` when `build-index` read it — this
  phase requires phase 2 to have fully stopped first (see `ARCHITECTURE.md`,
  phase 3, section 0); re-run `build-index` after confirming no
  `extract-worker` replica is still consuming the extraction frontier.
- **`compute-pagerank` fails immediately with a `PageRankPhaseError` about
  `search-index/documents.jsonl`:** `build-index` (phase 3) hasn't run yet,
  or wrote to a different `--bucket`/`--index-output-prefix` than
  `compute-pagerank`'s `--documents-object-key` points at — run `build-index`
  first, or align the two commands' arguments.
- **`compute-pagerank` reports a high `unresolved_target_links` /
  `unresolved_source_entries` count:** expected for a real crawl — most of a
  bounded-domain site's raw outbound links point outside the indexed corpus
  (external sites, pages discarded during extraction). See `GraphBuildStats`
  in `pagerank-link-analysis`'s own `models.py` for what each count means;
  this is not an error, just visibility into how much of the raw link graph
  fell outside the `doc_id` space (`ARCHITECTURE.md`, phase 4, section 4).

## License

MIT — see [`LICENSE`](LICENSE).
