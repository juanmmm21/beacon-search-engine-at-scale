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
measurements). Phase 5, the last phase before the flagship app, serves
queries against phase 3's index over real, independently restartable
infrastructure: `distributed-index-sharding`'s own partitioning, shard HTTP
server, async fan-out and merge run unmodified, discovered dynamically
through the phase-0 service registry instead of a fixed list of shard
addresses, so that redundant replicas of the same shard fail over into each
other without the caller ever holding a stale target list (see
[`ARCHITECTURE.md`](ARCHITECTURE.md), "Phase 5 — Distributed query serving").
Phase 6 puts the flagship application — `beacon-search-console`, reused as a
real package dependency with its `/api/v1` contract and React frontend
unchanged — in front of that cluster: a FastAPI service runnable as N
interchangeable replicas behind a load balancer, discovering shards
dynamically instead of spawning its own, resolving snippets on demand against
the phase-2 partitions instead of loading the whole corpus per replica, and
sharing a Redis result cache namespaced by a content hash of the index build
so a rebuilt index can never silently serve stale results (see
[`ARCHITECTURE.md`](ARCHITECTURE.md), "Phase 6 — The console over the real
cluster"). See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full reasoning
behind every decision across all phases, the alternatives considered, and how
each piece of phase 0 is meant to evolve toward Kubernetes.

This repo does not modify any of the ten existing `beacon-search-engine`
repositories — `web-crawler-scheduler`'s own crawler logic (see "Distributed
crawling (phase 1)" below), `html-content-extractor`'s own extraction logic
(see "Distributed extraction (phase 2)" below), `inverted-index-builder`'s /
`index-compression-codec`'s own indexing and compression logic (see
"Distributed indexing (phase 3)" below), `pagerank-link-analysis`'s own
ranking algorithm (see "PageRank (phase 4)" below),
`distributed-index-sharding`'s own partitioning/fan-out/merge (see
"Distributed query serving (phase 5)" below) and `beacon-search-console`'s
own API contract, snippet construction and frontend (see "Serving the console
(phase 6)" below) are all reused as real package dependencies, unchanged. It
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
  ┌─────────────────────────────────────────────────────────┐
  │  phase 5 — distributed query serving (shard replicas)       │
  │                                                             │
  │   shard data <- phase-0 ObjectStorage (shard-index job)     │
  │   discovery  =  phase-0 ServiceRegistry (one target per     │
  │                 shard_id, deterministic replica choice)     │
  │   fan-out/merge reused unchanged from                       │
  │   distributed-index-sharding                                │
  └─────────────────────────────────────────────────────────┘
        |
        v
  ┌─────────────────────────────────────────────────────────┐
  │  phase 6 — the console over the real cluster (N API        │
  │  replicas behind a load balancer)                           │
  │                                                             │
  │   contract/frontend reused unchanged from                   │
  │   beacon-search-console (/api/v1)                           │
  │   shards     <- phase-5 cluster, discovered per query       │
  │   result cache = Redis, namespaced by index version         │
  │   snippets   <- phase-2 partitions, on demand               │
  │   LTR model  <- train-reranker batch job -> ObjectStorage   │
  └─────────────────────────────────────────────────────────┘
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
| Shard discovery (phase 5) | fixed `ShardTarget` list into `SearchCoordinator` | `resolve_shard_targets` over `ServiceRegistry.discover`, re-resolved before every query |

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

## Distributed query serving (phase 5)

`src/beacon_scale_infra/query/` answers a search query by fanning it out to
every shard of the index phase 3 built and merging the already-ranked
results — over real, independently restartable containers instead of local
subprocesses. `distributed-index-sharding`'s own partitioning, shard HTTP
server, async fan-out and k-way merge run **unmodified**; this phase adds
only what running shards as real, horizontally-scaled infrastructure needs
and that project explicitly leaves to its caller (see its own README,
"Adding or removing a shard without downtime"): discovering shard addresses
dynamically instead of taking a fixed list, and failing over between
redundant replicas of the same shard. See
[`ARCHITECTURE.md`](ARCHITECTURE.md), section "Phase 5 — Distributed query
serving", for the full reasoning.

| Concern | `distributed-index-sharding` alone | This phase |
|---|---|---|
| Shard addresses | fixed `ShardTarget(shard_id, host, port)` list, typed by hand or from `LocalShardCluster` | discovered dynamically via `resolve_shard_targets` over the phase-0 `ServiceRegistry`, re-resolved before every query |
| Replicas of the same shard | not modeled — a caller passing two targets for one `shard_id` gets duplicated results | exactly one target per `shard_id`, deterministic tie-break among live replicas, real failover when the chosen one stops being alive |
| Running a shard | `serve-shard`, launched as a subprocess by `LocalShardCluster` | the same `serve-shard` subprocess, wrapped by `ShardReplicaService`: downloads its shard from `ObjectStorage`, registers in `ServiceRegistry`, heartbeats its TTL |
| Partitioning the index | `partition_index`, run by hand against a local directory | the same function, unmodified, run by the `shard-index` batch job against phase 3's `ObjectStorage` output, with results uploaded back per-shard |
| Fan-out, merge | `SearchCoordinator` + `merge_shard_outcomes` | the exact same classes, given a dynamically-resolved target list instead of a fixed one |

Shard replicas are **not** interchangeable the way `crawl-worker`/
`extract-worker` are: each one owns a disjoint partition of `doc_id`s, so
`docker-compose.yml` defines one service per `shard_id` (`shard-0`,
`shard-1`, `shard-2`), each independently scalable to add redundancy —
`--scale shard-0=2` gives shard 0 two live replicas, not two different
shards.

### Running it

```bash
docker compose up -d                    # MinIO, Redis, Consul, bucket bootstrap
python -m beacon_scale_infra build-index --storage-backend s3 --bucket beacon-scale-dev

# Partition the global index into shards (one-shot, like build-index)
BEACON_S3_ENDPOINT_URL=http://localhost:9000 \
BEACON_S3_ACCESS_KEY=beacon-dev \
BEACON_S3_SECRET_KEY=beacon-dev-secret \
python -m beacon_scale_infra shard-index --storage-backend s3 --bucket beacon-scale-dev --num-shards 3

# Bring up shard replicas -- 2 for shard 0, 1 each for shards 1 and 2
docker compose up -d --scale shard-0=2 shard-0 shard-1 shard-2

# Query the shards that are alive right now
BEACON_CONSUL_BASE_URL=http://localhost:8500 \
python -m beacon_scale_infra search --registry-backend consul --text "python" --top-k 5
```

To see failover in practice, kill one of the two `shard-0` containers and
run `search` again — the query still returns shard 0's documents through its
surviving replica:

```bash
docker compose ps shard-0
docker kill <one-of-the-two-shard-0-container-ids>
python -m beacon_scale_infra search --registry-backend consul --text "python" --top-k 5
```

### Running a single replica without Docker

```bash
python -m beacon_scale_infra shard-replica \
  --shard-id 0 --storage-backend local --registry-backend local \
  --host 127.0.0.1 --port 9300 --announce-host 127.0.0.1 \
  --local-storage-root .local-object-storage
```

Same caveat as every other `--storage-backend local`/`--registry-backend
local` invocation: neither backend coordinates across separate process
invocations, so this only makes sense to smoke-test one replica in isolation,
never to simulate a multi-replica cluster.

## Serving the console (phase 6)

`src/beacon_scale_infra/console/` serves the flagship application —
[`beacon-search-console`](https://github.com/juanmmm21/beacon-search-console),
reused as a real package dependency — over the phase-5 cluster, with the
exact same versioned `/api/v1/search`, `/api/v1/autocomplete` and
`/api/v1/index/stats` contract (the response models are literally imported
from that package) and its React frontend running unchanged against this
API. See [`ARCHITECTURE.md`](ARCHITECTURE.md), section "Phase 6 — The console
over the real cluster", for the full reasoning, including the
piece-by-piece decision of what became shared state and what is rebuilt per
replica, and what serving the frontend from a CDN would take.

| Concern | `beacon-search-console` alone | This phase |
|---|---|---|
| Shard processes | spawned as local subprocesses by the API itself (`DistributedSearchPipeline.start`, fixed local ports — breaks with a second API replica) | the phase-5 cluster, discovered via the phase-0 `ServiceRegistry` on every query; the API owns no shard |
| Search results | recomputed per request, per process | shared Redis cache (`CacheStore`, phase-0 substrate), keys namespaced by the index version the live shards announce — a rebuilt index changes the namespace, so stale entries are unreachable by construction and expire by TTL |
| Stale-index protection | none (single process, single build) | per-query verification: a shard replica announcing a different index version than the API loaded is excluded from the fan-out and reported as an explicit shard error |
| Snippets (`doc_id -> text`) | whole `documents.jsonl` loaded into process memory, `doc_id` = line number | resolved on demand against the phase-2 part files via the corpus catalog `build-index` publishes (binary search over part ranges, bounded LRU of hot parts) |
| Autocomplete / spellcheck / reranker / stats | loaded once per process from local `data/` | rebuilt identically per replica at startup from the immutable build artifacts in `ObjectStorage` (deterministic, so replicas cannot diverge) |
| LTR model | trained by the bootstrap script into `data/ltr-model` | the `train-reranker` batch job (same training, unmodified) publishes it to `ObjectStorage` |
| API replicas | exactly one | N interchangeable replicas behind `console-lb` (nginx, Docker-DNS round-robin) |

### Running it

```bash
docker compose up -d                    # MinIO, Redis, Consul, bucket bootstrap
# ... crawl + extract as in phases 1-2, then the batch jobs:
export BEACON_S3_ENDPOINT_URL=http://localhost:9000 \
       BEACON_S3_ACCESS_KEY=beacon-dev BEACON_S3_SECRET_KEY=beacon-dev-secret
python -m beacon_scale_infra build-index      --storage-backend s3 --bucket beacon-scale-dev
python -m beacon_scale_infra compute-pagerank --storage-backend s3 --bucket beacon-scale-dev
python -m beacon_scale_infra shard-index     --storage-backend s3 --bucket beacon-scale-dev --num-shards 3
python -m beacon_scale_infra train-reranker  --storage-backend s3 --bucket beacon-scale-dev

# Shard replicas + the console API behind its load balancer
docker compose up -d shard-0 shard-1 shard-2
docker compose up -d console-api console-lb
curl -s "http://localhost:8080/api/v1/search?q=python&limit=5" | python3 -m json.tool

# Scale the API horizontally -- nginx picks the new replicas up via Docker DNS
docker compose up -d --scale console-api=2 console-api
```

Killing one `console-api` replica loses nothing: any replica answers any
request, and a cache entry written through one is read through the others.
After re-crawling and re-running the batch jobs, restart the shard replicas
and then the `console-api` replicas — until the APIs restart, they exclude
the new-version shards explicitly (degraded response naming the reason)
rather than ever mixing two builds silently.

### The frontend

`beacon-search-console`'s frontend is served exactly as that repo documents
(`npm run dev` proxying `/api`, or a static `npm run build`), pointed at
`console-lb` instead of the single-process backend — no frontend change. The
CDN story (what is static, what must reach the backend, and what invalidates
what) is documented in `ARCHITECTURE.md`, phase 6, section 8.

## Requirements and installation

- Python `>=3.11`
- [Docker](https://www.docker.com/) and Docker Compose, to run the real
  backends locally (MinIO, Redis, Consul)
- On macOS, `brew install libomp` — LightGBM's OpenMP runtime, needed by
  `learning-to-rank-reranker` (phase 6); the Docker image installs its Linux
  equivalent (`libgomp1`) itself — the same requirement
  `beacon-search-console` already documents

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
(phase 1)" above for `crawl-worker`, "Distributed extraction (phase 2)"
above for `extract-worker`, "Distributed query serving (phase 5)" above
for `shard-index`/`shard-replica`/`search`, and "Serving the console (phase
6)" above for `train-reranker`/`serve-console`. A demonstration CLI
exercises each piece of the substrate end to end, against either backend:

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
- **Sharded index directories** (`shard-index/shard-<id>/` by default) are
  exactly `distributed-index-sharding`'s own `partition_index` output — an
  uncompressed `inverted-index-builder`-format directory per shard, plus one
  `shard-index/cluster_manifest.json` (`{"num_shards": ..., "shard_dir_names":
  [...]}`) describing them — unmodified, see that repo's own README for the
  field-by-field contract.
- **A shard replica's service instance metadata** carries
  `{"shard_id": "<int>"}` — the convention `resolve_shard_targets`
  (`query/shard_discovery.py`) depends on to group live replicas by which
  partition they serve (see `ARCHITECTURE.md`, phase 5, section 0) — plus,
  when the shard data was partitioned by a marker-aware `shard-index` run,
  `{"index_version": "<sha256>"}`: the content version of the build that
  replica is serving, which the console verifies per query (see
  `ARCHITECTURE.md`, phase 6, section 2).
- **The index version marker** (`index_version.json`, published next to
  `search-index/`, `search-index-compressed/` and `shard-index/`) is
  `{"format_version": 1, "index_version": "<sha256>"}` — a content hash over
  the merged index files plus the corpus file, computed by `build-index`
  (`index/index_version.py`).
- **The corpus catalog** (`search-index/corpus_catalog.json` by default) maps
  every phase-2 part file to its contiguous global `doc_id` range, plus
  `total_documents` and the corpus-wide `last_crawled_at` — what the console
  uses to resolve `doc_id -> text` on demand instead of loading the corpus
  file whole (`index/corpus_catalog.py`).
- **The LTR model** (`ltr-model/` by default) is exactly
  `learning-to-rank-reranker`'s own saved-model directory (`model.txt` +
  `manifest.json`), published by the `train-reranker` job, unmodified.

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

Querying dynamically-discovered shards (phase 5) follows the same shape:

```python
import asyncio

from beacon_scale_infra.query.pipeline import DistributedQueryServingPipeline
from beacon_scale_infra.registry.consul import ConsulServiceRegistry


async def main() -> None:
    registry = ConsulServiceRegistry(base_url="http://localhost:8500")
    try:
        async with DistributedQueryServingPipeline(
            registry, service_name="beacon-scale-shard"
        ) as pipeline:
            result = await pipeline.search_text("python", top_k=10)
            for hit in result.merged:
                print(hit.doc_id, hit.score)
            if result.failed_shard_ids:
                print("degraded, missing shards:", result.failed_shard_ids)
    finally:
        await registry.aclose()


asyncio.run(main())
```

Every call to `search_text`/`search_parsed_query` re-resolves which
`ShardTarget` to fan out to from whatever `registry.discover(...)` reports as
alive *at that moment* — there is no fixed list to keep in sync as replicas
come and go.

## Development

```bash
pytest
ruff check .
ruff format --check .
mypy --strict src/
```

Local backends are tested directly, with no mocks. Real backends are tested
against faithful doubles of their SDKs: `moto` for S3/MinIO, `fakeredis` for
Redis Streams (and for phase 6's Redis result cache), and a real
`aiohttp.web` application (served via `aiohttp.test_utils.TestServer`)
standing in for Consul's HTTP API — chosen over the `aioresponses` mocking
library after it turned out to be incompatible with current `aiohttp`
versions (its response-building code predates a breaking constructor change
in `aiohttp`'s `ClientResponse`).

Phase 5's query-serving layer is tested at four increasingly real levels (see
`ARCHITECTURE.md`, phase 5, "Testing this phase" for the full breakdown):
discovery logic against `InMemoryServiceRegistry` alone, fan-out/merge/failover
against real `distributed-index-sharding` shard servers over real sockets
(`aiohttp.test_utils.TestServer`, no subprocess), a real `serve-shard`
subprocess wrapped by `ShardReplicaService`, and — the only test in this repo
that needs a running Docker daemon —
`tests/test_query_docker_shard_failover.py`, which brings up this repo's
actual `docker-compose.yml` via the `docker compose` CLI, kills real
containers, and asserts the coordinator degrades/fails over correctly. It
skips itself automatically if Docker is not running or if the fixed ports
`docker-compose.yml` publishes are already bound by another stack.

Phase 6's console is tested end to end without Docker
(`tests/test_console_app.py`): the real phase-3/5/6 batch pipelines build the
artifacts, real `distributed-index-sharding` shard servers answer over real
sockets, and the FastAPI app is driven over ASGI — covering merged + reranked
+ snippeted results, cache hits surviving the whole cluster dying, degraded
and version-mismatched clusters, and the stats/autocomplete endpoints. See
`ARCHITECTURE.md`, phase 6, "Testing this phase", for the unit-level suites
(cache backends, corpus catalog, index version, snippet resolver).

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
- **`shard-replica` keeps restarting with `QueryServingError: no hay ningún
  objeto bajo 'shard-index/shard-<N>'...`:** `shard-index` hasn't run yet, or
  ran with a different `--num-shards`/`--shard-index-prefix` than this
  replica's `--shard-id`/`--shard-index-prefix` expect — run `shard-index`
  first (see "Distributed query serving (phase 5)" above); `restart:
  on-failure` keeps retrying the container in the meantime, it does not
  crash-loop opaquely.
- **`search`/`DistributedQueryServingPipeline` raises `QueryServingError:
  ninguna réplica viva registrada`:** every `shard_id` currently has zero live
  replicas — either none have started yet, or all of them let their TTL
  expire (crashed, or Consul/the registry itself was restarted, see
  `ARCHITECTURE.md`, phase 5, "Known limitations"). Check
  `docker compose ps shard-0 shard-1 shard-2` and `GET
  http://localhost:8500/v1/health/service/beacon-scale-shard?passing=true`
  directly against Consul.
- **A shard replica registers, but nothing can ever reach it from outside its
  own container's network:** its `--announce-host` defaults to
  `socket.gethostname()` (the container's own short hostname), resolvable
  only by other containers on the same Docker Compose network — never from
  the host machine, and never across a NAT/firewall boundary in a real
  multi-machine deployment. Pass an explicit `--announce-host` (a real,
  routable address) when running `shard-replica` outside a single Compose
  network.
- **A killed `shard-0` container's replacement still can't be reached even
  though `docker compose ps` shows it running again:** `restart: on-failure`
  starts a *new* container with a *new* hostname (see "Distributed crawling
  (phase 1)" above for the same mechanic applied to `--worker-id`) — it
  re-registers itself under a fresh `service_id`/`host` once it passes its
  own health check; give it a few seconds, then re-check Consul's `passing`
  list rather than assuming the old registration comes back.
- **`console-api` keeps restarting with `ConsoleServingError: no hay ningún
  objeto bajo ...`:** one of the batch jobs the console needs hasn't run yet
  against this bucket — the error names which one (`build-index`,
  `compute-pagerank`, `shard-index` or `train-reranker`); run it and the next
  `restart: on-failure` retry succeeds.
- **`shard-index` fails with `no existe 'search-index-compressed/
  index_version.json'`:** the global index in the bucket was built by a
  `build-index` older than the version marker — re-run `build-index` (it now
  always publishes the marker) before `shard-index`.
- **Every search comes back degraded with `la réplica elegida sirve otra
  versión del índice`:** the shard replicas restarted onto a newer (or older)
  build than the one this `console-api` replica loaded at startup — restart
  the `console-api` replicas after re-running the batch jobs and restarting
  the shards (see `ARCHITECTURE.md`, phase 6, "Known limitations"). The
  degradation is the protection working: the alternative would be snippets
  rendered from the wrong build's documents.
- **Search works but repeated queries never hit the cache (`console-api`
  logs `se sirve sin caché`):** Redis is unreachable from the API container
  (check `BEACON_REDIS_URL` and `docker compose ps redis`), or a shard
  replica without an announced `index_version` is being chosen (run
  `shard-index` once with a marker-aware build and restart that replica) —
  both degrade to serving without cache, never to failing the search.
- **`OSError: libomp.dylib`/`libgomp.so.1` when importing the CLI:**
  LightGBM's OpenMP runtime is missing — `brew install libomp` on macOS; the
  provided `Dockerfile` already installs `libgomp1` on Linux.

## License

MIT — see [`LICENSE`](LICENSE).
