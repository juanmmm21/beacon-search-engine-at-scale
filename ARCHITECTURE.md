# Architecture — beacon-search-engine-at-scale

This document records the phase-0 infrastructure decisions for scaling
[`beacon-search-engine`](https://github.com/juanmmm21/beacon-search-engine)
from a ~180-page single-machine demo to a bounded-domain corpus of a few
million pages running on real, multi-container infrastructure. It exists so
that every later phase (distributed crawling, distributed indexing,
distributed query serving) inherits the same substrate instead of each one
re-deciding storage, messaging, orchestration and discovery on its own.

Nothing here implements crawling, indexing, ranking or query serving. This
repository is infrastructure substrate only — see "Why the ten original repos
stay untouched" below for how domain logic and this substrate are meant to
meet.

## 1. Object storage — MinIO (S3-compatible), not raw local disk or a real cloud account

**Decision:** all bulk artifacts (raw crawled pages, extracted documents,
built indexes, compressed postings) go through an `ObjectStorage` protocol
backed by MinIO in development, and by the S3-compatible API in general —
the exact same client code targets AWS S3 in a real deployment by changing
only `endpoint_url`.

**Why not a real cloud account for development:** the whole point of this
phase is to build and test the substrate without requiring an AWS/GCP
account, billing, or network access to a cloud provider from a developer
laptop or CI. MinIO implements the S3 API closely enough that
`boto3`/`botocore` cannot tell the difference — the same `S3ObjectStorage`
class (`src/beacon_scale_infra/storage/s3.py`) is the "real" implementation
for both.

**Why not stay on raw local filesystem paths:** a filesystem path is not a
distributable resource — it doesn't survive a worker process moving to a
different machine, doesn't offer atomic overwrite semantics across
concurrent writers, and doesn't give a natural bucket/prefix model for
partitioning a few million pages of crawl output. `LocalFilesystemObjectStorage`
still exists (`storage/local.py`) but is explicitly scoped to development and
unit tests, not to anything that runs across more than one process.

**Client shape:** a synchronous `boto3` client wrapped in `asyncio.to_thread`,
rather than `aioboto3`. AWS does not maintain an official async S3 client
with the same coverage and type-stub quality as `boto3`; wrapping the
well-supported sync client keeps the async interface (needed so storage
calls never block the event loop of a future crawler/indexer coordinator)
without taking on a less mature dependency.

## 2. Message queue — Redis Streams, not Kafka

**Decision:** work distribution (crawl frontier, extraction jobs, indexing
jobs) goes through a `MessageQueue` protocol with consumer-group semantics
(`XADD`/`XGROUP CREATE`/`XREADGROUP`/`XACK`), backed by Redis Streams.

**Why not Kafka:** Kafka is built for the regime of many partitions,
long-term log retention, and cross-datacenter replication at the scale of
billions of events per day. The target of this phase is a few million pages
— meaning at most a few million crawl/index jobs total, not per second. At
that volume, Kafka's operational surface (ZooKeeper/KRaft, broker tuning,
partition rebalancing, a JVM to operate) buys reliability guarantees this
project doesn't need yet, in exchange for real operational cost for a
single developer running this on a laptop or a handful of containers. Redis
Streams gives the properties that actually matter here — consumer groups so
several crawler/indexer workers share a queue without duplicating work,
per-message acknowledgment, and a pending-entries list that a later phase
can use for retry/reclaim (`XCLAIM`) — without any of that operational
weight.

**What is explicitly given up, on purpose:** Redis Streams has no
partition-level ordering guarantees across brokers (there is one Redis, not
a partitioned cluster) and no long-term compacted log for event replay at
Kafka's scale. Both are acceptable for a bounded-domain crawl: this system
does not need to replay a year of crawl history, and total throughput stays
inside what a single well-resourced Redis instance handles. If the corpus
target grows past "a few million pages" into a regime where a single Redis
node's throughput or memory becomes the bottleneck, that is the point to
revisit this decision — not before.

**Durability trade-off, made explicit in `docker-compose.yml`:** the
development Redis runs with `--appendonly yes`. A message queue whose queue
disappears on every container restart would defeat the purpose of using it
to coordinate distributed work — even in development, losing an in-flight
crawl frontier on every `docker compose restart` would be a worse debugging
experience than turning on AOF from the start.

## 3. Orchestration — Docker Compose for development, Kubernetes as the deliberate next step

**Decision:** `docker-compose.yml` in this repo brings up the three
infrastructure services (MinIO, Redis, Consul) plus a bootstrap step that
creates the development bucket. It does not run any `beacon-search-engine`
component — no phase after this one has been implemented yet.

**Why Compose now, not Kubernetes now:** a single-machine Compose file is
enough to develop and test the substrate itself (this repo's actual scope).
Standing up a Kubernetes cluster (even a local one via kind/minikube) to run
three stateful services and no application workload yet would be
infrastructure for infrastructure's sake — complexity introduced before
there is a workload that needs it.

**How this evolves to Kubernetes** (documented, not implemented, in this
phase):

- MinIO becomes either a `StatefulSet` with a `PersistentVolumeClaim` per
  replica (self-hosted) or is dropped in favor of a managed S3-compatible
  bucket — the `ObjectStorage` protocol does not change either way, only the
  constructor arguments to `S3ObjectStorage`.
- Redis becomes a `StatefulSet` (or a managed Redis with Streams support) —
  again, `RedisStreamsMessageQueue` only needs a different connection URL.
- Consul becomes a proper 3- or 5-node server cluster (a `StatefulSet` with
  Raft) with the Consul Helm chart, rather than the single `-dev` agent used
  here; application pods run a Consul agent as a sidecar or DaemonSet and
  register themselves on startup.
- Future compute (crawler workers, index-builder workers, shard query
  servers) becomes `Deployment`s that scale horizontally by replica count,
  each instance registering itself in Consul on boot and deregistering (or
  letting its TTL check expire) on shutdown — exactly the same
  `ServiceRegistry.register`/`heartbeat` calls this repo already defines,
  called from inside a pod instead of inside a local process.
- A `docker-compose.dev.yml` split from a `k8s/` manifest/Helm chart
  directory would replace this single file, but that split only earns its
  complexity once there is a second (non-dev) environment to actually target.

## 4. Service registry — Consul, not etcd or a fixed host list

**Decision:** a `ServiceRegistry` protocol lets a future shard coordinator
discover live shard instances dynamically (`discover(service_name)`),
backed by Consul's agent HTTP API in development and in any later
deployment.

**Why not a fixed list of hosts:** `distributed-index-sharding` (one of the
ten original repos) currently takes shard addresses as `--shard
SHARD_ID:HOST:PORT` command-line flags — correct for a fixed local
simulation of 2-3 processes, but it does not survive shards being added,
removed, or rescheduled to a different host, which is exactly what happens
once shards run as replicas in real, elastic infrastructure. Dynamic
discovery replaces "the coordinator is told where shards are" with "the
coordinator asks who is alive right now."

**Why Consul over etcd:** etcd is a raw, strongly-consistent key-value
store — using it as a service registry means building health-checking,
TTL/lease renewal, and a query-by-service-name index by hand on top of it
(this is exactly what Kubernetes itself does internally, since etcd is
Kubernetes' own datastore, not an end-user-facing service catalog). Consul
provides that layer natively: register a service with a TTL health check,
call `/v1/agent/check/pass/<id>` to renew it, and query
`/v1/health/service/<name>?passing=true` to get only the instances that are
actually alive — the liveness bookkeeping this project needs is Consul's
built-in job, not custom code on top of a generic KV store. `etcd` would be
the right choice if this project were already committed to running its own
Kubernetes control plane and wanted to reuse its existing datastore — it
isn't, at this phase.

**Consistency with the local backend:** `InMemoryServiceRegistry` re-derives
the same TTL-expiry behavior by hand (tracking a last-heartbeat timestamp
and filtering on `discover`), specifically so that switching from the local
backend to Consul in a later phase changes zero calling code — the
liveness contract (`register` → `heartbeat` → `discover` returns only live
instances) is identical, only who enforces it differs.

## 5. The protocol-plus-two-implementations pattern

Each of the three pieces above is a `Protocol` in
`src/beacon_scale_infra/protocols.py`, with:

- one **local development implementation** with no network dependency
  (filesystem-backed storage, an in-memory queue, an in-memory registry) —
  used in unit tests and for developing calling code without Docker running
  at all;
- one **real implementation** against the services in `docker-compose.yml`
  (and, unchanged, against real production infrastructure later).

No future phase should import `boto3`, `redis`, or an HTTP client for Consul
directly — always through these protocols, so that swapping the real
backend (e.g. MinIO → a managed S3 bucket) never touches domain code.

## 6. Why the ten original repos stay untouched

`beacon-search-engine`'s ten repositories (`web-crawler-scheduler` through
`beacon-search-console`) are a closed, complete portfolio piece: each one
demonstrates a specific piece of information-retrieval engineering
(tokenization, inverted indexes, BM25, PageRank, learning-to-rank,
sharding...) built from scratch, over a small, deliberately-sized demo
corpus. Rewriting or scaling their internals in place would blur what each
of those repos is *for* — a focused, readable demonstration of one
algorithm or data structure — and would put already-finished, already-public
portfolio work at risk of regressions for the sake of a scaling exercise
that has different goals (infrastructure and distributed-systems
engineering, not new IR algorithms).

`beacon-search-engine-at-scale` is deliberately a sibling repository, not a
new version of an existing one. It **orchestrates and extends**: later
phases in this repo are expected to depend on the existing packages exactly
the way `distributed-index-sharding` already depends on
`bm25-ranking-engine` today (a real package dependency, pinned to a Git
URL, importing the same tokenizer/scorer code) or to talk to a shard process
built from one of those repos over the same JSON contracts those repos
already document in their own READMEs. Nothing here re-implements BM25,
PageRank, tokenization, or postings compression — this repo's job is to let
those unchanged implementations run across many machines with real object
storage, a real work queue, and real dynamic discovery underneath them,
not to replace them.

## Non-goals of phase 0

This phase does not: run a crawler, build an index, serve a query, decide
the sharding/partitioning scheme for a distributed index (that is the
concern of a later phase, extending `distributed-index-sharding`'s existing
by-document partitioning — see its own README — to run its shards as
Kubernetes-ready replicas instead of local subprocesses), or stand up
Kubernetes. It hands the next phase a tested, documented substrate to build
on.

## Phase 1 — Distributed crawling

This phase extends the phase-0 substrate with the first real domain workload
that consumes it: crawling a bounded-domain corpus with several worker
processes running concurrently, coordinated only through the substrate
above — no worker talks to any other worker directly.

`web-crawler-scheduler` (one of the ten original `beacon-search-engine`
repositories) already implements the crawler logic itself — retries with
exponential backoff, `robots.txt` compliance, outbound link extraction — for
a single process, with an in-memory priority frontier and an in-memory
deduplication set. That repository stays untouched (see "Why the ten
original repos stay untouched" above): this phase reuses its retryable
fetcher (`AiohttpFetcher`), its `robots.txt` cache (`RobotsCache`) and its
link extractor (`extract_outlinks`) as a real package dependency (pinned Git
URL in `pyproject.toml`, same pattern as `distributed-index-sharding` →
`bm25-ranking-engine`), and replaces only the three pieces a single
in-memory process cannot share across workers: the frontier, the
deduplication set, and per-domain rate limiting.

### 1. Shared frontier — the phase-0 `MessageQueue`, not an in-memory heap

`web_crawler_scheduler.frontier.PriorityFrontier` is a BFS-priority heap
inside one process's memory. Between workers, the frontier is instead a
Redis Streams stream (`beacon-scale-crawl-frontier` by default) through the
same `MessageQueue` protocol phase 0 already defines: every worker in the
same consumer group (`beacon-scale-crawl-workers`) pulls disjoint batches of
`FrontierJob` messages via `XREADGROUP`, so the frontier splits across
workers for free, with no partitioning logic of its own.

**What is given up on purpose:** Redis Streams delivers FIFO, not by
priority — there is no distributed equivalent of `PriorityFrontier`'s BFS
heap in this phase. This is acceptable because seed URLs are always
published first, and each depth's outlinks are published only once their
parent page has been processed, so FIFO order still approximates
breadth-first crawling in practice, just without a strict guarantee.
Revisiting this (e.g. one stream per depth) is only worth it if a real
corpus shows crawl order actually matters for this project's goals — it
does not for a bounded, polite crawl of a few million pages.

### 2. Shared deduplication — a Redis `SET`, not a Bloom filter

`web_crawler_scheduler.urlnorm.HashSetDeduplicator` exposes `seen()` +
`mark_seen()` as two separate calls — correct for one process, a real race
between workers. `SharedDeduplicator.try_claim(url)` (`crawl/dedup.py`)
replaces both with one atomic operation: a Redis `SADD` on a set of
normalized URL hashes, which reports whether the element was newly added.
Exactly one worker's `try_claim` call for a given URL returns `True`, no
matter how many workers call it at the same instant.

A plain `SET`, not a Bloom filter: a Bloom filter would trade memory for
false positives (URLs that were never actually crawled getting silently
treated as "already seen"), and would need the RedisBloom module, which
`docker-compose.yml` does not run — adding infrastructure a workload does
not yet justify was already rejected once for orchestration (see "Docker
Compose for development, Kubernetes as the deliberate next step" above); the
same reasoning applies here. At this phase's target (a few million pages), a
`SET` of sha256 hex digests fits comfortably on the same Redis node that
already hosts the queue.

### 3. Coordinated rate limiting — the risk this phase names explicitly

`web_crawler_scheduler.rate_limiter.DomainRateLimiter` enforces a minimum
delay and a concurrency cap per domain, but only within one process. With
several independent `DomainRateLimiter` instances — one per worker — none of
them know what the others are doing: four workers each individually
respecting a one-request-per-second limit to the same domain still adds up
to four requests per second against that domain. This is the most direct way
distributing the crawler could turn a polite, rate-limited crawler into an
impolite one against a site this project does not control.

`CoordinatedRateLimiter` (`crawl/rate_limiter.py`) moves both halves of that
contract to Redis, keyed per domain (via the same `extract_domain` the
single-process crawler already uses, so "domain" means the same thing in
both):

- **Minimum-delay gate** (`SET key value NX PX <delay_ms>`): only one worker
  in the whole cluster can "open the gate" for a domain inside a delay
  window; everyone else finds it locked and retries after a short poll
  interval. This alone already guarantees the cluster starts at most one new
  request per domain per delay window.
- **Concurrency semaphore with a lease TTL**: a Redis sorted set of lease
  tokens scored by expiry, acquired inside a `WATCH`/`MULTI` transaction —
  needed in addition to the delay gate because one slow request can outlast
  the minimum delay between request *starts*, and only a real concurrency
  cap prevents several such requests from overlapping. The TTL self-expires
  a lease a crashed worker never released, the same pattern `ServiceRegistry`
  already uses for instance liveness (see "Consistency with the local
  backend" above) — a dead worker never permanently starves a domain's rate
  limit.

### 4. Raw pages — partitioned by date and by URL-hash shard

Crawled pages (HTML plus extracted outlinks, combined into one
`CrawledPageRecord` per page rather than the two separate JSONL streams
`web-crawler-scheduler` produces for a single process) are written to the
phase-0 `ObjectStorage` under
`<prefix>/date=<YYYY-MM-DD>/shard=<NNN>/<url_hash>.json`
(`crawl/partitioning.py`). Date partitioning keeps a day's crawl inspectable
without listing the whole bucket; hash-sharding within a day spreads pages
across `num_hash_shards` prefixes so a large single-day crawl does not
concentrate millions of keys under one lexicographic prefix — which would
otherwise limit how well a later phase (distributed indexing) could split
that prefix range across its own workers.

### 5. Worker orchestration — `docker-compose.yml`'s `crawl-worker` service

The `crawl-worker` service builds from this repo's `Dockerfile` and runs
`python -m beacon_scale_infra crawl-worker` against the real backends
(`--queue-backend redis --storage-backend s3 --coordination-backend redis`).
It takes no fixed identity: `--worker-id` defaults to the container's
hostname, so `docker compose up -d --scale crawl-worker=N` gives N workers
with N distinct IDs automatically, with no per-replica configuration needed.
Every worker publishes its seed URLs on startup regardless of how many other
replicas are doing the same (see `CrawlWorker._seed_frontier`'s docstring)
— the atomic claim in `SharedDeduplicator` makes redundant seeding harmless,
so no separate one-shot seeding step is needed before scaling workers up.

A worker stops on its own (exit code `0`) once the frontier has been idle
for `idle_polls_before_shutdown` consecutive polls — appropriate for this
project's bounded-domain crawl, not an always-on firehose; `restart:
on-failure` in `docker-compose.yml` only restarts a worker that actually
crashed, never one that finished its share of an already-completed crawl.

### Known limitation carried over from phase 0

Redis Streams pending entries for a worker that crashes mid-message are
never reclaimed (`XCLAIM`) in this phase — the message queue section above
already flagged this as a deliberate gap for a later phase, and it remains
one here: a crashed worker's in-flight job stays claimed-but-unprocessed in
Redis until a human intervenes, rather than being picked up automatically by
another worker.

### Non-goals of phase 1

This phase does not: extract or clean page content (that is
`html-content-extractor`'s job, unchanged, over the raw HTML this phase
stores), build an index of any kind, decide a global page budget across the
whole cluster (`CrawlWorkerConfig.max_pages` is a per-worker cap, not a
cluster-wide one — see its docstring for why), or reclaim a crashed worker's
pending queue entries automatically. It hands the next phase (distributed
indexing) a corpus of raw, partitioned pages in object storage to build on.
