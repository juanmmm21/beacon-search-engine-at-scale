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

## Phase 2 — Distributed extraction

This phase extends the phase-0 substrate with the second domain workload:
turning the raw, partitioned pages phase 1 wrote into object storage into
clean, indexable documents, with several worker processes running
concurrently, each one consuming pages *as the crawler produces them* rather
than in one batch after the crawl finishes.

`html-content-extractor` (one of the ten original `beacon-search-engine`
repositories) already implements the extraction logic itself — encoding
correction, tolerant DOM parsing, the text-density boilerplate-removal
heuristic, metadata extraction, Unicode normalization — for a single process
reading one `pages.jsonl` file to completion. That repository stays
untouched (see "Why the ten original repos stay untouched" above): this
phase reuses its per-stage functions (`resolve_encoding`, `parse_html`,
`extract_main_content`, `extract_metadata`, `normalize_text`) as a real
package dependency (pinned Git URL in `pyproject.toml`, same pattern as
phase 1 with `web-crawler-scheduler`), through `extract_single_page`
(`src/beacon_scale_infra/extract/page_extractor.py`) — a function that
processes one already-deserialized page and returns one result, instead of
`html_content_extractor.pipeline.ExtractionPipeline`, which is deliberately
coupled to opening two output files and reading one input file to exhaustion
in a single process. What this phase adds is the orchestration between
several workers over individual queue messages, not the extraction logic
itself — exactly the same shape of integration phase 1 already used for
`web-crawler-scheduler`.

### 1. Producer/consumer wiring — `CrawlWorker` publishes, `ExtractWorker` consumes

Phase 1's `CrawlWorker._write_page` now does one thing in addition to the
`put_object` it already did: after the page lands in object storage, it
publishes a small message (`{"bucket": ..., "key": ...}`) to a second
phase-0 `MessageQueue` stream (`beacon-scale-extract-frontier` by default,
`CrawlWorkerConfig.extract_stream`) — never the frontier stream itself, a
distinct one. This is the *only* way phase 2 learns that a new page exists:
there is no polling of object storage, no timer, no listing. A payload
carries a storage reference, not the page body — the HTML already lives in
object storage exactly for this reason, and copying it again into Redis
would waste the message queue's memory on data already durably stored
elsewhere.

The two worker types are wired through a plain `dict` payload, not a shared
Python import between `crawl/` and `extract/`: `crawl/worker.py` never
imports anything from `beacon_scale_infra.extract`, so a change to phase 2's
internals never has to touch phase 1's module. The contract between them is
the serialized message shape itself — the same principle the whole
`beacon-search-engine` ecosystem already applies between repos (see
`~/Desarrollo/beacon-search-engine/CLAUDE.md`), applied here one level down,
between two phases of the same repo.

### 2. No coordination between extraction workers — the easiest stage to parallelize

Unlike phase 1, `ExtractWorker` needs no `SharedDeduplicator` and no
`CoordinatedRateLimiter` equivalent. Both of those exist in phase 1 because
several crawl jobs can target the *same* URL or the *same* domain at the
same time, and something has to arbitrate that shared resource. Extraction
has no such resource: each `ExtractJob` references a page that was already
downloaded once, already lives at a unique object-storage key, and produces
an output that depends on nothing outside that one page. The only shared
state between `ExtractWorker` replicas is the phase-0 `MessageQueue`'s
consumer-group semantics — the same mechanism that already splits the crawl
frontier across `CrawlWorker` replicas in phase 1 — which is sufficient on
its own to guarantee that N workers process disjoint messages without ever
talking to each other.

### 3. Extracted documents — partitioned by worker, not by hash shard

Unlike phase 1's raw pages (partitioned by date + URL-hash shard, because
*every* `CrawlWorker` can write a page for any date/shard combination and
those need to be spread evenly), extracted documents are partitioned by
`worker_id` (`src/beacon_scale_infra/extract/partitioning.py`): each
`ExtractWorker` owns one partition for its entire run and never writes
outside it. No hash function is needed to keep two workers from colliding —
they simply never share a key prefix. Within its own partition, a worker
never overwrites a previous write: `ObjectStorage.put_object` has no native
*append*, so accumulating ever more documents into one growing key would
retransmit the whole partition's content on every flush, a quadratic cost
at this phase's target scale (a few million pages). Instead, each flush
(every `flush_every_pages` processed pages, plus once more at shutdown)
writes a new, immutable part file (`documents-NNNNNN.jsonl` /
`discarded-NNNNNN.jsonl`) — the same shape Spark/Hive use for partitioned
output.

### 4. The manifest — itself partitioned, for the same reason

The next phase (distributed indexing) needs to know which partitions exist
and how many documents each one has, without listing and counting every
part file at startup. `src/beacon_scale_infra/extract/manifest.py` gives it
that as a small, aggregated view — but the manifest that indexing reads is,
underneath, exactly as distributed as the partitions it describes: each
`ExtractWorker` owns one manifest *fragment*
(`manifest/partition=<worker_id>.json`), overwritten with updated running
totals on every flush, and never reads or writes any other worker's
fragment. Overwriting is cheap here (unlike a part file) because a fragment
is a handful of counters, not document content. `read_manifest` reconstructs
the aggregate manifest by listing the `manifest/` prefix
(`ObjectStorage.list_objects` already streams rather than buffering a whole
bucket, see `protocols.py`) and summing each fragment — no locking, no
compare-and-swap, no coordination between workers is needed to keep this
consistent, for the same reason section 2 above gives: nothing here is
shared.

### Known limitations

- **A `put_object` that succeeds followed by a `publish` that fails leaves a
  page stuck in storage, never extracted.** `_write_page` does the two
  calls sequentially with no compensating transaction; a page in this state
  is invisible to phase 2 forever, with no automatic reconciliation in this
  phase (a future phase could add a periodic sweep comparing
  `crawl-pages/` against the manifest, but that is out of scope here).
- **`ExtractWorker` handles a missing referenced page (`ObjectNotFoundError`)
  by counting it and moving on** (`ExtractWorkerStats.pages_missing`), never
  by retrying or crashing — the same "never lose a page in silence, but
  never abort the batch for one bad page" principle
  `~/Desarrollo/beacon-search-engine/CLAUDE.md` already requires for
  malformed HTML, applied here to a missing object instead.
- **A worker that restarts under the same `worker_id` resets its part-file
  sequence counter to zero, and could overwrite `documents-000000.jsonl`
  from its previous run.** In practice this risk is already mitigated by
  the same design choice phase 1 made for `CrawlWorker`: `--worker-id`
  defaults to the container hostname, and a restarted container under
  `docker compose up -d --scale` gets a new one, not a reused one — see
  phase 1, section 5. A deployment that pins a stable, reused `worker_id`
  across restarts (e.g. a Kubernetes `StatefulSet` pod name) would need to
  persist the sequence counter externally to avoid this; not needed at this
  phase's `docker-compose` orchestration.
- Same Redis Streams limitation carried over from phase 0 and phase 1:
  pending entries for a worker that crashes mid-message are never reclaimed
  (`XCLAIM`).

### Non-goals of phase 2

This phase does not: build an inverted index of any kind (that is the next
phase, distributed indexing, extending `inverted-index-builder`), merge the
per-worker partitions into a single `documents.jsonl` (the manifest gives
indexing everything it needs to read all partitions without that merge
step), reconcile pages that were stored but never got their extraction job
published, or decide a global document budget across the whole cluster
(`ExtractWorkerConfig.max_pages` is a per-worker cap, same reasoning as
`CrawlWorkerConfig.max_pages` in phase 1). It hands the next phase a corpus
of clean, partitioned documents in object storage, plus a manifest
describing exactly where to find them, to build on.

## Phase 3 — Distributed indexing

This phase extends the phase-0 substrate with the third domain workload:
turning the partitioned, clean documents phase 2 wrote into object storage
into a single global inverted index, in the exact on-disk format
`inverted-index-builder` already defines and `index-compression-codec`
already consumes.

`inverted-index-builder` (one of the ten original `beacon-search-engine`
repositories) already implements the algorithm — tokenization, positional
postings, document-frequency accounting — for a single process reading one
`documents.jsonl` file to completion (`IndexBuilder.build`,
`src/inverted_index_builder/pipeline.py`). That repository stays untouched
(see "Why the ten original repos stay untouched" above): this phase reuses
`IndexBuilder.build` and `inverted_index_builder.serialization.write_index`
unmodified, as a real package dependency (pinned Git URL in `pyproject.toml`,
same pattern as phases 1–2), and adds only what a single process reading one
file cannot do — build a consistent global index out of N part-files written
independently by N phase-2 workers with no coordination between them.

### 0. Why this phase cannot start until phase 2 is "done" — a batch boundary, not a stream

Phases 1→2 are a continuous pipeline: `ExtractWorker` consumes pages *as the
crawler produces them*, with no notion of the crawl having "finished" before
extraction can start. Phase 3 is deliberately different: it needs the phase-2
manifest's `document_count` per partition to be **final**, because those
counts are exactly what the doc_id range assignment below is computed from
(section 1). Computing ranges from a manifest that is still being updated by
running `ExtractWorker` instances would invalidate every range already
handed out the moment any partition's count grows — a partition's range
would need to shift to stay contiguous with the next one, which would in
turn move every doc_id downstream of it, including ones already burned into
postings that were already remapped and merged. This is why phase 3 is
structured as a one-shot batch job (`IndexingPipeline.run()`,
`index/pipeline.py`) invoked once phase 2's workers have stopped, not a
long-running worker consuming a queue like `CrawlWorker`/`ExtractWorker` —
there is no meaningful streaming version of "assign a stable position in a
total order" while the set being ordered is still growing.

### 1. Global `doc_id` assignment — contiguous per-partition ranges from the manifest, not a centralized counter

**The problem:** `IndexBuilder.build(documents_path)` assigns `doc_id` as a
0-based counter over the non-blank lines of a *single* input file, and
documents this as a hard determinism contract (`inverted-index-builder`
`README.md`, "Determinism and correctness"). That contract is meaningless
once the corpus lives across N part-files written independently by N
phase-2 workers: there is no single file, and no single process ever sees
every document.

**Decision:** each partition (one per phase-2 `worker_id`) is assigned a
contiguous, disjoint half-open range of global `doc_id`s
`[start, start + document_count)`, computed from the phase-2 manifest
(`extract/manifest.py`) alone — no document is read to compute a range, only
counted. Ranges are assigned in ascending order of `partition_key` (the
`worker_id` string, compared lexicographically): partition ranges are sorted
once, and each partition's `start` is the running sum of `document_count`
over every partition that sorts before it
(`index/doc_id_ranges.py::compute_doc_id_ranges`). Within a partition, a
document's global `doc_id` is `start + local_doc_id`, where `local_doc_id` is
exactly what `IndexBuilder.build` assigns when it reads that partition's
part-files concatenated in ascending `part_seq` order (`documents-000000
.jsonl`, then `documents-000001.jsonl`, ...) into one materialized file
(`index/partition_indexer.py::materialize_partition_documents`). Because
`part_seq` and `partition_key` are both already deterministic, stable
identifiers written by phase 2, the entire range assignment — and therefore
every global `doc_id` — is a pure function of the manifest plus the part-file
contents, reproducible from the same phase-2 output with no coordination
step of its own.

**Why not a centralized counter (an `INCR` against Redis, one call per
document):** this is the most literal translation of "0-based counter,
strictly increasing" to a distributed setting, and was rejected for the
reason the task itself names — it puts a single point of coordination in the
hot path of indexing millions of documents, exactly the kind of bottleneck
phase 0 already rejected once for the crawl frontier (see "Message queue"
above: the whole reason `MessageQueue` uses consumer-group fan-out instead
of a single arbiter is to avoid one shared piece of mutable state that every
worker must serialize through). A per-partition range needs exactly one
piece of shared state per partition (its `document_count`, already sitting
in the manifest phase 2 produces for free) instead of one round-trip per
document.

**Why not a hash-based `doc_id` (e.g. a stable hash of the URL, truncated to
an integer range):** this was rejected because it breaks the ascending-order
contract `PostingsList` postings depend on. `inverted-index-builder`'s
`README.md` and `index-compression-codec`'s delta encoding both depend on
postings being sorted by ascending `doc_id` *within a term* — a hash
scatters unrelated integers with no relationship to insertion or partition
order, which would force an explicit sort of every postings list after
hashing (an `O(n log n)` step the current single-machine pipeline never
needs) and would also make "which partition owns this `doc_id`" an O(1)
lookup on paper but a red herring in practice, because nothing about a hash
value tells you which partition actually wrote that document — you would
still need a separate global reverse index from `doc_id` to partition,
duplicating exactly the bookkeeping the range scheme gives away for free.

**Why per-partition ranges compress trivially back to "which partition owns
this doc_id":** because ranges are contiguous and sorted by construction, the
question "which partition owns doc_id `d`" is a binary search over at most
(number of partitions) range boundaries — not over documents
(`DocIdRangeAssignment.partition_for`, `index/doc_id_ranges.py`), using
`bisect.bisect_right` over the sorted `start` offsets. At this phase's scale
(a few million documents across, realistically, tens to low hundreds of
worker partitions, not millions of partitions), this is a handful of integer
comparisons — the "cheap, given a doc_id, resolve which partition it belongs
to" property required of any scheme here, without needing an explicit
per-document `doc_id → partition` index at all.

### 2. The map-reduce pipeline

**Map** (`index/partition_indexer.py`, one independent unit of work per
partition, embarrassingly parallel in the same sense phase 2's extraction
was — see phase 2, section 2): for each partition, in ascending
`partition_key` order,

1. materialize that partition's `documents-*.jsonl` part-files, concatenated
   in ascending `part_seq` order, into one local file — this is the same
   shape of input `IndexBuilder.build` already expects, just assembled from
   several part-files instead of coming from a single `html-content-extractor`
   run;
2. call `IndexBuilder().build(materialized_path)` **unmodified** — the
   partition-local index it returns uses `local_doc_id`s 0-indexed within
   that partition, exactly as documented upstream;
3. remap every `doc_id` the partition-local index carries — both
   `InvertedIndex.documents` keys/`DocumentRecord.doc_id` and every
   `Posting.doc_id` inside every `PostingsList` — by adding that partition's
   `start` offset from section 1 (`remap_index_to_global_doc_ids`). Adding a
   constant is order-preserving, so a partition-local postings list that was
   already ascending by `local_doc_id` (guaranteed by `IndexBuilder` itself)
   stays ascending by global `doc_id` after remapping, with no re-sort
   needed.

**Reduce** (`index/merge.py::merge_partition_indexes`): merges the N
already-remapped, partition-local `InvertedIndex` objects into one global
`InvertedIndex`. This is genuinely new logic — no repo in the ecosystem
merges N pre-built indexes into one today — but it is deliberately thin,
because the hard part (producing a correct total order) was already solved
by section 1's range assignment, not by this step:

- `documents`: a plain union of the per-partition dicts. Ranges are disjoint
  by construction, so no two partitions can ever produce the same global
  `doc_id`; the merge asserts this (raising `IndexingError` if it ever sees
  a collision) as a integrity check on section 1's invariant, not as
  a case it needs to actually handle.
- `postings_lists`, per term: **concatenation, not a merge-sort.** Because
  partitions are processed in the same ascending `partition_key` order their
  ranges were assigned in, and each partition's own postings for a term are
  already ascending by global `doc_id` (previous paragraph), concatenating
  partition 1's postings for a term, then partition 2's, then partition 3's,
  ... yields a list that is already fully sorted ascending by `doc_id` —
  the ordering work is a side effect of doing section 1 and the map step
  correctly, not a separate sorting pass over postings. `document_frequency`
  for the merged list is the sum of the per-partition document frequencies
  (safe to sum, not re-derive, because a document is indexed by exactly one
  partition).
- `stats` (`IndexStats`): recomputed once over the merged `documents`/
  `postings_lists`, the same arithmetic `IndexBuilder._compute_stats` does
  internally — not reused directly because that method is private to
  `IndexBuilder` and operates on a single build pass, but it is pure
  aggregation (`sum`/`len`), not indexing logic, so recomputing it here is
  not a reimplementation of anything `inverted-index-builder` is meant to
  own.

The merged `InvertedIndex` is written to disk with
`inverted_index_builder.serialization.write_index` **unmodified** —
`manifest.json`, `documents.jsonl`, `postings.jsonl`, `stats.json`, byte-for-byte
the same format a single-machine `inverted-index-builder` run would produce,
then uploaded to the phase-0 `ObjectStorage` under `index_output_prefix`
(default `search-index/`). No format is written by hand anywhere in this
phase.

### 3. Compression handoff — `index-compression-codec`, unmodified, confirmed by test

`CompressionPipeline.compress(source_index_dir, output_dir)` reads exactly
the four files `write_index` produces and knows nothing about how they were
built — its own contract (`index-compression-codec` `README.md`, "Role in
beacon-search-engine") is "an `inverted-index-builder`-shaped directory in,
a compressed index directory out". Because phase 3's merge step already
produces that exact directory shape via the unmodified `write_index` call
above, `IndexingPipeline.run()` calls `CompressionPipeline().compress()` on
it with zero adaptation. `tests/test_index_pipeline.py` asserts this
directly — compressing the merged output must succeed and the compressed
`documents.jsonl`/`stats.json` (copied through unmodified by the codec) must
still describe the same total document count phase 3 computed — rather than
assuming compatibility from the shared format documentation alone.

### 4. What determinism is preserved, and what changes — verified against a single-file build

`~/Desarrollo/beacon-search-engine/CLAUDE.md`, section 2.B, requires that
building an index from the same input always produces the same result. That
guarantee is preserved by this phase, but its two components are affected
differently:

**Preserved, unchanged: postings order within a term is still stable and
reproducible.** Section 2 above is exactly the argument for why: ascending
`local_doc_id` order (guaranteed by unmodified `IndexBuilder`) plus a
constant per-partition offset (order-preserving) plus partition-ordered
concatenation (not a merge-sort, so nothing depends on a stable-sort
implementation detail) composes into a globally ascending, fully
deterministic order, given the same phase-2 output and the same manifest.

**Changed, and documented here as the new exact rule: `doc_id` is no longer
"line position in a single input file".** The new rule is: *`doc_id` is the
cumulative document count of every partition that sorts lexicographically
before this one, by `partition_key`, plus this document's own line position
within its partition's part-files concatenated in ascending `part_seq`
order.* This is still fully deterministic (a pure function of phase-2's
output and manifest, no wall-clock or process-scheduling dependency
anywhere) — determinism is preserved, but the concrete integers assigned to
any given document generally differ from what a hypothetical single-machine
`inverted-index-builder` run over some arbitrary concatenation of the same
documents would assign, unless that concatenation happens to use this exact
partition order. This is expected and is exactly what
`tests/test_index_pipeline.py::test_partitioned_index_matches_single_file_build_modulo_doc_ids`
verifies: build the same small corpus two ways — (a) through this phase's
map-reduce pipeline over ≥3 simulated partitions, and (b) as one concatenated
`documents.jsonl` fed directly to `inverted-index-builder`'s own
`IndexBuilder.build` — and assert the document *set* (by URL), the term
vocabulary, and every term's per-document frequencies match exactly, while
explicitly not asserting the numeric `doc_id` values match.

### 5. Preserving the two consumers that already depend on `doc_id` resolution

Two pieces of the ecosystem resolve `doc_id`s today, and do so by two
different mechanisms — this phase had to check both, not assume the same fix
covers both:

**`pagerank-link-analysis`'s `JsonlDocumentIdResolver`**
(`src/pagerank_link_analysis/document_resolver.py`) reads `inverted-index-builder`'s
own `documents.jsonl` output and resolves by the explicit `"doc_id"` JSON
field on each line — **not** by line position. It needed nothing special
from this phase beyond a `documents.jsonl` where every `doc_id` is unique
and the ID space is dense from `0` (its `total_documents = max_doc_id + 1`
assumes no gaps). Both hold by construction here: ranges are disjoint
(section 1) and packed with no gaps (`start` of each partition is exactly
the `end` of the previous one, and `document_count` counts only actually-
written documents, never a padded/reserved allocation). This consumer works
unmodified against phase 3's merged `documents.jsonl`, with no deployment
change beyond pointing it at that file instead of a single-machine one.

**`beacon-search-console`'s `SnippetIndex`**
(`backend/src/beacon_search_console/snippets.py`) is the harder case: unlike
the resolver above, it resolves `doc_id → text` by **array position** in
`html-content-extractor`'s *original* `documents.jsonl` (the one carrying
`main_text`, which `inverted-index-builder`'s own output deliberately drops
— see that module's docstring). Its documented contract, verbatim from
`beacon-search-console`'s own `README.md` ("How it works", step 4), is
`doc_id = line number` in whatever file it is pointed at — a positional
contract with no explicit `doc_id` field to fall back on. Breaking this
without an alternative would leave `beacon-search-console` unable to render
result snippets at all in a later phase, which is exactly the risk this
phase was asked to rule out, not just note.

The fix requires no change to `beacon-search-console`'s code, because
`SnippetIndex.from_documents_path` already takes an arbitrary `Path` — the
positional contract is a property of *whichever file it's pointed at*, not
of any specific filename. This phase produces exactly such a file:
while materializing each partition's part-files for the map step (section
2, step 1 — reading `main_text`-bearing `ExtractedDocument` records, not
`inverted-index-builder`'s stripped-down `DocumentRecord`), `IndexingPipeline`
also appends that same materialized partition, in the same ascending
`partition_key` order used for range assignment, to one running global file
(`corpus_object_key`, default `search-index/corpus/documents.jsonl`,
uploaded via the same `ObjectStorage`). Because this file is assembled with
*exactly* the same partition order and intra-partition order used to compute
global `doc_id`s, line position in this file equals global `doc_id` by
construction — the same guarantee `SnippetIndex` already relies on today,
just computed across partitions instead of within one file. A later phase
that stands up `beacon-search-console` against this pipeline's output only
needs a deployment/configuration change — point `documents_path` at this
corpus file instead of a single-process extractor's output — never a code
change to `snippets.py`.

### Known limitations

- **The map step runs sequentially, in-process, over all partitions** — it
  is designed to be embarrassingly parallel (section 2 explicitly notes each
  partition's map step is an independent unit of work, same as phase 2's
  extraction), but this phase does not actually distribute it across worker
  processes the way `CrawlWorker`/`ExtractWorker` distribute across
  replicas. Doing so would need the map step to be invokable standalone
  (already true — `build_index_from_materialized_partition` takes a single
  partition and an already-computed offset, no shared mutable state) plus a
  coordinator that fans the N partition offsets out and collects N partial
  indexes back, e.g. through the same `ObjectStorage` (each map worker
  writes its remapped partial index with `write_index` to a scratch prefix;
  a single reduce step reads them back with `read_index` and merges) instead
  of the in-process list this phase currently keeps. Not implemented because
  at this phase's target (a few million documents), a single process
  building N partition-local indexes sequentially and merging them is not
  the bottleneck the crawl or extraction stages are — revisit only if
  profiling this phase against a real few-million-document corpus shows
  otherwise.
- **No incremental re-indexing.** Every run reads every partition from
  scratch; there is no notion of indexing only documents written since the
  last run. Acceptable for a bounded-domain, one-time-per-crawl corpus (the
  same framing phase 0 already applies to the whole project); revisiting
  this is only worth it for a corpus that is re-crawled and re-indexed on a
  recurring schedule, which is out of this project's stated scope.
- Same general principle phase 2 already applies (`ARCHITECTURE.md`, phase
  2, section "Known limitations"): nothing in this phase aborts the whole
  pipeline over one bad partition or one unreadable part-file — a partition
  that cannot be read raises `IndexingError` with enough context to identify
  which partition failed, rather than silently producing a global index
  missing an unknown slice of the corpus.

### Non-goals of phase 3

This phase does not: change `inverted-index-builder`'s or
`index-compression-codec`'s own algorithms or on-disk formats, run BM25 or
PageRank scoring (that is `bm25-ranking-engine`/`pagerank-link-analysis`,
consuming this phase's output through their own already-documented
contracts), distribute the map step across worker processes (see "Known
limitations" above), or support incremental/streaming index updates. It
hands the next phase (distributed query serving /
`distributed-index-sharding`) one global, compressed, correctly-doc_id'd
index in object storage, plus a corpus file that keeps
`beacon-search-console`'s snippet resolution working unmodified, to build
on.

## Phase 4 — PageRank

This phase extends the phase-0 substrate with the fourth domain workload:
computing link-authority scores over the whole corpus phase 3 indexed, in the
exact on-disk format `pagerank-link-analysis` already defines and
`learning-to-rank-reranker` already consumes (`doc_id -> pagerank_score`).

Like phase 3, this is a one-shot batch job (`PageRankPipelineConfig` +
`DistributedPageRankPipeline`, `pagerank/pipeline.py`), not a long-running
worker consuming a queue: it needs phase 3's `search-index/documents.jsonl`
to be finished (a stable, dense `doc_id` space to resolve URLs against), and
`pagerank-link-analysis`'s own README already frames PageRank as "computed
once per corpus (or recomputed whenever the link graph changes) rather than
per search" — a batch recomputation, not a stream, is the correct shape for
this phase independently of phase 3's own reasons for being one (see phase 3,
section 0).

### 0. Why this phase asks "does a single machine suffice?" before designing anything distributed

Unlike phases 1–3, this phase's core algorithm (`pagerank_link_analysis.pagerank.compute_pagerank`)
is not a per-document, embarrassingly parallel transform — it is a single
sparse power iteration over the *entire* corpus's adjacency matrix at once,
already vectorized with `scipy.sparse`/`numpy` (see that repo's own
docstring: "cost per iteration proportional to the number of edges, never to
the square of the number of pages"). Distributing a power iteration for real
(partitioning the adjacency matrix across workers, exchanging the rank vector
once per iteration over the phase-0 `MessageQueue`/`ObjectStorage`) is a
substantial, genuinely new piece of infrastructure this repo does not
currently have — and building it before checking whether it is actually
needed at this project's stated target (`AGENTS.md`: "a few million pages,
bounded domain") would be exactly the kind of complexity phase 0 already
rejected once for orchestration ("Docker Compose for development, Kubernetes
as the deliberate next step") and once for the message queue ("Redis Streams,
not Kafka"): infrastructure ahead of a demonstrated need.

### 1. Capacity analysis — measured, not assumed

**Methodology.** `pagerank_link_analysis.document_resolver.JsonlDocumentIdResolver`,
`graph_builder.build_adjacency_matrix` and `pagerank.compute_pagerank` were
run **unmodified**, in a fresh process per data point (so one run's freed
memory never masks another's peak), over synthetic `documents.jsonl` /
`link_graph.jsonl` pairs generated at graduated `(total_documents,
avg_out_degree)` sizes. Peak resident memory was read from
`resource.getrusage(RUSAGE_SELF).ru_maxrss` — a high-water mark that never
decreases within a process, so it captures the true peak even across memory
that gets freed later in the same run (e.g. the edge set below, freed once
the sparse matrix is built) — immediately after each stage. Four points,
picked to separate the cost of `total_documents` (N) from the cost of
`resolved_edges` (E):

| Run | N (documents) | E (edges) | Resolver peak RSS | Peak RSS after graph build | Graph-build wall time |
|---|---|---|---|---|---|
| A | 200,000 | 2,000,000 | 191 MB | 533 MB | 8.3 s |
| B | 200,000 | 8,000,000 | 191 MB | 1,538 MB | 31.4 s |
| C | 800,000 | 8,000,000 | 622 MB | 1,955 MB | 36.1 s |
| D | 1,000,000 | 20,000,000 | 782 MB | 3,445 MB | 88.0 s |

Comparing B against C (same E, 4x N) isolates the resolver's own cost at
~800–1,000 bytes/document (two `dict`s of `DocumentRecord`/`doc_id` keyed by
`doc_id` and by normalized URL). Comparing A against B against D (varying E)
isolates the graph-build cost at ~140–180 bytes/**edge** — call it 180 B/edge
and 1,000 B/document for a conservative (upper-bound) capacity estimate.
Wall time is consistently ~4–4.5 µs/edge for the graph-build stage; the
power-iteration stage itself is negligible by comparison (0.04–0.44 s across
all four runs, 7–11 iterations to converge on these synthetic graphs) because
each iteration is one sparse matrix-vector product over the *final* CSR
matrix, which is far smaller than the intermediate structure below.

**Why per-edge cost is ~180 bytes, not the ~12–16 bytes a CSR matrix actually
needs:** `build_adjacency_matrix` (`pagerank-link-analysis`, unmodified)
deduplicates edges with a pure-Python `edges: set[tuple[int, int]]` before
ever constructing a numpy array — see that file's own docstring for why
(repeated nav-menu links must not double-count). Each element is a boxed
`(int, int)` tuple in a hash set, which in CPython costs far more than two
raw 4-byte integers: this is the dominant memory cost of this phase, not the
final sparse matrix, and it is a property of the **unmodified upstream
dependency**, not of any code this phase adds — reducing it would mean
touching `pagerank-link-analysis` (numpy-vectorized dedup, e.g. structured
arrays + `np.unique`), which stays out of scope for the same reason section
"Why the ten original repos stay untouched" gives for every other repo. The
capacity numbers below already account for this real cost, not a
hypothetical optimized one.

**Extrapolation to this project's target scale** (3–5 million documents, the
range `AGENTS.md`/this document's introduction already commit to for the
whole corpus), across a spread of plausible average out-degrees for a
bounded-domain crawl with internal navigation links (15 on the low end, 40 on
the high end for a nav-heavy site), with a 20% margin added on top of the raw
`1,000·N + 180·E` bytes estimate for interpreter/OS overhead:

| Documents | avg out-degree | Edges | Peak RSS (raw) | Peak RSS (+20% margin) | Graph-build wall time |
|---|---|---|---|---|---|
| 3,000,000 | 15 | 45,000,000 | 11.1 GB | 13.3 GB | ~3.4 min |
| 3,000,000 | 25 | 75,000,000 | 16.5 GB | 19.8 GB | ~5.6 min |
| 3,000,000 | 40 | 120,000,000 | 24.6 GB | 29.5 GB | ~9.0 min |
| 5,000,000 | 15 | 75,000,000 | 18.5 GB | 22.2 GB | ~5.6 min |
| 5,000,000 | 25 | 125,000,000 | 27.5 GB | 33.0 GB | ~9.4 min |
| 5,000,000 | 40 | 200,000,000 | 41.0 GB | 49.2 GB | ~15.0 min |

**Verdict: a single large-memory machine suffices; no distributed
power-iteration engine is built in this phase.** Even the worst case in this
project's stated scale (5M documents, avg out-degree 40) needs under 50 GB
peak RSS and under 15 minutes of single-core wall time — comfortably inside
one large-memory cloud instance (e.g. an AWS `r6i.4xlarge`, 128 GB RAM / 16
vCPU, leaves 2.5x headroom over the worst case above; `r6i.8xlarge`, 256 GB,
if a wider safety margin is wanted), run once per crawl (or whenever the link
graph changes — never per query, per `pagerank-link-analysis`'s own framing).
Building a partitioned, multi-worker power iteration exchanging the rank
vector once per iteration over the phase-0 substrate would trade a
few-minutes batch job for a genuinely harder distributed-systems problem
(convergence checking across workers, partition-boundary edges, a
synchronization barrier per iteration) to solve a capacity problem this
corpus does not have. Revisit only if a real crawl's measured average
out-degree or corpus size, after phase 1 actually runs, lands far outside the
table above — the same "revisit only if profiling/real data shows otherwise"
condition phase 0 (message queue) and phase 3 (sequential map step) already
apply to their own scale decisions.

### 2. Reuse decision — `pagerank-link-analysis`, unmodified, as a real package dependency

`pagerank-link-analysis`'s `DocumentIdResolver` (`protocols.py`) is already a
`Protocol`, decoupling `build_adjacency_matrix` from *how* a URL resolves to
a `doc_id` — and its only real implementation, `JsonlDocumentIdResolver`,
already resolves by the explicit `"doc_id"` JSON field on each
`documents.jsonl` line, **not** by line position (`document_resolver.py`).
Phase 3, section 5 of this document already established the consequence:
phase 3's own merged `search-index/documents.jsonl` output satisfies both
invariants that resolver needs (every `doc_id` unique, the ID space dense
from `0`) by construction — so this phase needs **zero** new resolver code,
only `search-index/documents.jsonl` downloaded to a local path and handed to
`JsonlDocumentIdResolver` exactly as `pagerank-link-analysis`'s own
`PageRankPipeline.from_documents_path` already does.

This phase therefore reuses the entire `pagerank_link_analysis.pipeline.PageRankPipeline`
unmodified — resolver, `graph_builder.build_adjacency_matrix`,
`pagerank.compute_pagerank`, `PageRankParams` (damping factor, tolerance,
max iterations) — as a real package dependency (pinned Git URL in
`pyproject.toml`, same pattern as the other four repos this project already
depends on). This phase's own orchestrator is named `DistributedPageRankPipeline`
specifically so it never collides with `pagerank_link_analysis.pipeline.PageRankPipeline`
(imported under the alias `CorpusPageRankPipeline` in `pagerank/pipeline.py`
for extra clarity where both are in scope in the same file), never because
any of its logic is touched. The on-disk
output format (`scores_io.write_pagerank_output`: `manifest.json` +
`pagerank_scores.jsonl` + `convergence.json`) is reused unmodified too — it
is literally `pagerank-link-analysis`'s own declared integration contract
with `learning-to-rank-reranker` (`doc_id -> pagerank_score`), not a format
this phase has any reason to reinvent.

### 3. The one genuinely new piece — materializing `link_graph.jsonl` from phase 1's raw pages, with concurrent fan-out

`build_adjacency_matrix` takes a `Path` to a single local `link_graph.jsonl`
file (its own docstring: entries in `{url, outlinks}` shape, one page per
line) — an unmodified dependency's fixed input shape, the same
materialize-then-call-unmodified-function pattern phase 3 already uses for
`IndexBuilder.build` (`index/partition_indexer.py::materialize_partition_documents`).
Unlike phase 3's map step, though, there is no phase-2-style manifest of a
handful of large, pre-batched part-files to concatenate here: phase 1 writes
**one object per crawled page** (`crawl-pages/date=<YYYY-MM-DD>/shard=<NNN>/<url_hash>.json`,
`crawl/partitioning.py`) — for a few-million-page corpus, that is a few
million individual objects. Reading them with a sequential `for` loop of
`storage.get_object()` calls, the way phase 3's map step reads its handful of
part-files, would make this phase's actual bottleneck **network round-trips**,
not CPU: at even 10 ms/GET, 5,000,000 sequential calls is ~14 hours — far
outside the batch-job time budget section 1 above establishes for the
compute itself.

**Decision:** `pagerank/link_graph_materializer.py::materialize_link_graph`
lists `crawl-pages/` once via `ObjectStorage.list_objects` (already a
streaming `AsyncIterator`, never buffers the bucket into memory — `protocols.py`)
and fans the individual `get_object` calls out concurrently, bounded by an
`asyncio.Semaphore(max_concurrent_reads)` (default `64`) — exactly the
"`asyncio` for network I/O, never block the event loop" rule
`~/Desarrollo/beacon-search-engine/CLAUDE.md` already mandates for this whole
ecosystem, applied here to read fan-out instead of write fan-out. A page
object that fails to parse (missing `final_url`/`outlinks` fields, invalid
JSON — e.g. a page written by a future, incompatible `CrawledPageRecord`
schema) is skipped and counted (`PageRankRunStats.pages_skipped_malformed`),
never aborts the whole materialization — the same "never lose the batch over
one bad record" principle phase 2 applies to a malformed page and phase 3
applies to an unreadable partition.

**The `url`/`final_url` convention already lines up with zero adaptation.**
`pagerank-link-analysis`'s own `url_normalizer.py` docstring documents that
`link_graph.jsonl`'s `url` field is expected to be the crawler's
*post-redirect* URL, while `documents.jsonl`'s `url` field is expected to be
*pre-redirect*. This phase's materializer writes `CrawledPageRecord.final_url`
(post-redirect, phase 1's own field) as `link_graph.jsonl`'s `url`, and
`CrawledPageRecord.outlinks` as `outlinks` — while phase 3's merged
`search-index/documents.jsonl` carries `ExtractedDocument.url`, which
`extract/page_extractor.py::extract_single_page` sets from
`CrawledPageRecord.url` (pre-redirect). Both phase 1 and phase 2 already
carried both fields through unchanged for their own reasons, before this
phase existed — so satisfying `pagerank-link-analysis`'s existing,
independently-implemented convention required no new field anywhere in this
repo, only choosing which existing field maps to which side of the join.

### 4. Output

`pagerank-scores/` (default `output_prefix`) receives exactly
`scores_io.write_pagerank_output`'s three files, uploaded to the phase-0
`ObjectStorage` the same way phase 3 uploads `search-index/`
(`_upload_directory`, `pagerank/pipeline.py`): `manifest.json` (format
version + filenames), `pagerank_scores.jsonl` (`doc_id -> pagerank_score`,
sorted ascending by `doc_id`), and `convergence.json` (damping factor,
tolerance, iterations run, converged flag, and the graph-build stats —
resolved edges, dangling documents, unresolved source/target counts — useful
for auditing how much of a real crawl's raw link graph fell outside the
indexed corpus).

### Known limitations

- **Materialization concurrency is bounded within one process, not
  distributed across worker replicas.** `max_concurrent_reads` controls fan-out
  against a single `ObjectStorage` endpoint from a single `DistributedPageRankPipeline`
  run, not N worker processes each claiming a disjoint slice of `crawl-pages/`
  — the same "not the bottleneck at this phase's target scale, revisit only
  if profiling says otherwise" reasoning phase 3 already applies to its own
  sequential map step (section 1's wall-time numbers above already include
  this phase's actual materialization + compute cost, not a hypothetical
  distributed one).
- **`link_graph.jsonl` is materialized to a local file, not streamed directly
  into `build_adjacency_matrix`,** because that function's own signature (an
  unmodified dependency) takes a `Path`, not an iterator — unavoidable
  without touching `pagerank-link-analysis`.
- **No incremental recomputation.** Every run rebuilds the graph and reruns
  power iteration from scratch over every page phase 1 ever wrote to
  `crawl-pages/`; there is no notion of updating only the rank of documents
  whose link neighborhood changed. Acceptable for the same reason phase 3
  accepts it for indexing (a bounded-domain, occasionally-recomputed corpus,
  not a continuously-updated one) — and section 1's wall-time numbers (single
  minutes, even at this project's largest planned scale) mean a full
  recompute has not been, and is not expected to become, an operational cost
  worth engineering around.
- The dominant memory cost identified in section 1 (the edge-dedup
  `set[tuple[int, int]]`) is a property of the unmodified upstream
  dependency's own implementation choice, not of any code this phase adds —
  see section 1's own explanation of why reducing it is out of scope.

### Non-goals of phase 4

This phase does not: run BM25 scoring or reranking (`bm25-ranking-engine`/
`learning-to-rank-reranker`, consuming this phase's `pagerank_scores.jsonl`
through the contract `pagerank-link-analysis`'s own README already
documents), distribute the power iteration itself across worker processes
(ruled unnecessary by section 1's measured capacity numbers), modify
`pagerank-link-analysis`'s algorithm or on-disk formats, or support
incremental link-graph updates (see "Known limitations" above). It hands a
future query-serving phase a `doc_id -> pagerank_score` mapping in object
storage, ready to combine with a future BM25 signal exactly the way
`learning-to-rank-reranker` already documents doing for the single-machine
portfolio.

## Phase 5 — Distributed query serving

This phase extends the phase-0 substrate with the last domain workload before
the flagship app: answering a search query by fanning it out to every shard of
the index phase 3 built and merging the already-ranked results, over real,
independently restartable infrastructure (containers today, pods later)
instead of local subprocesses.

`distributed-index-sharding` (one of the ten original `beacon-search-engine`
repositories) already implements every piece of this — document-based
partitioning (`partition_index`), the shard HTTP server
(`shard_server.run_shard_server`), the async fan-out with per-shard timeout
(`SearchCoordinator`), and the k-way merge of already-ranked partial results
(`merge_shard_outcomes`) — for shards it discovers as a fixed, hand-typed list
of `ShardTarget(shard_id, host, port)`, running as local OS subprocesses that
`distributed_index_sharding.cluster.LocalShardCluster` launches and tracks
itself. That repository stays untouched (see "Why the ten original repos stay
untouched" above): this phase reuses `partition_index`,
`SearchCoordinator`, `HttpShardTransport`, `run_shard_server` (via its own
`serve-shard` CLI subcommand, launched as a real subprocess exactly like
`LocalShardCluster` already does) and `query_translation` unmodified, as a
real package dependency (pinned Git URL in `pyproject.toml`, same pattern as
phases 1–4), and adds only what running shards as real, independently
restartable, horizontally-scaled infrastructure requires and
`distributed-index-sharding` explicitly does not attempt itself: turning a
fixed list of shard addresses into one that is discovered, kept fresh, and
survives individual replicas dying — see its own README, section "Adding or
removing a shard without downtime", which frames exactly this as the
caller's job, not `LocalShardCluster`'s.

### 0. The nuance this phase has to resolve that `distributed-index-sharding` does not: replicas of the same shard

`SearchCoordinator.search` (`coordinator.py`) fans a query out to *every*
`ShardTarget` in the list it is given, concurrently, with no notion of two
targets being "the same shard": it does not deduplicate by `shard_id`. This is
correct and sufficient for `distributed-index-sharding`'s own scope — one
process per shard, one target per shard, resized by hand — but it means this
phase cannot simply hand `SearchCoordinator` every live replica discovered for
a service name. If shard 0 has two live replicas and both end up in the
`ShardTarget` list, `SearchCoordinator` asks both, and
`merge_shard_outcomes` — which has no idea two `ShardOutcome`s came from
replicas of the same partition, only that they carry the same `shard_id` in
different list entries — folds both into the merged result, double-counting
every document shard 0 holds.

**Decision:** `resolve_shard_targets` (`query/shard_discovery.py`) is the seam
where this gets resolved, entirely on this phase's side, before anything ever
reaches `SearchCoordinator`. It discovers every live instance of a service
name via the phase-0 `ServiceRegistry`, groups them by the `shard_id` each one
carries in its `ServiceInstance.metadata` (the convention this phase adds:
every shard replica registers with `metadata={"shard_id": str(shard_id)}`),
and picks **exactly one** instance per `shard_id` — the lexicographically
smallest `service_id` among the live candidates. The result — one
`ShardTarget` per `shard_id`, never two for the same one — is exactly the
shape `SearchCoordinator` already expects and already handles correctly; nothing
about the coordinator, the transport, or the merge changes.

**Why the smallest `service_id`, not "first in whatever order `discover`
returns"**: `ServiceRegistry.discover` (`protocols.py`) explicitly documents
its result as "in any order — the caller decides the strategy" — relying on
that order would make the chosen replica flap between calls for no reason
whenever the underlying registry (Consul's `/v1/health/service`, or a plain
`dict` in `InMemoryServiceRegistry`) happens to reorder its response, even
though every candidate is still alive and equally valid. A deterministic tie-break
means the chosen replica only ever changes when it actually has to — when the
previously-chosen one stops being among the live instances `discover` returns
— which is real failover, not cosmetic alternation. `tests/test_query_shard_discovery.py`
asserts this directly (`test_choice_only_changes_when_the_chosen_replica_stops_being_alive`).

**Why failover is "discover again", not an active health check of this
phase's own**: this phase deliberately adds no new liveness mechanism. A shard
replica's liveness is already the phase-0 `ServiceRegistry`'s job — TTL
health checks in Consul, TTL-tracked heartbeats in `InMemoryServiceRegistry`
(see phase 0, section 4) — and `resolve_shard_targets` is called fresh before
every single query (`DistributedQueryServingPipeline._current_coordinator`,
`query/pipeline.py`), so the *next* query after a replica's TTL expires
already sees a `ShardTarget` list without it, and picks the next live replica
of that `shard_id` if one exists. Building a second, phase-5-specific health
check on top would duplicate exactly the liveness bookkeeping phase 0 already
solved once, the same reasoning phase 0 itself gives for choosing Consul over
a hand-rolled KV store (see "Service registry" above). A `shard_id` with zero
live replicas at query time simply has no entry in the resolved target list —
that partition is absent from the fan-out for that one query, the identical
degradation shape `SearchCoordinator` already produces for a shard that times
out or errors, just resolved one step earlier and for a different reason.

### 1. The shard-index batch job — partitioning fase 3's global index once, not per replica

**Decision:** `ShardIndexPipeline` (`query/shard_index_pipeline.py`, exposed as
the `shard-index` CLI subcommand) downloads whatever phase 3 left at
`search-index-compressed/` (or `search-index/`, if `build-index` ran with
`--no-compress`) from the phase-0 `ObjectStorage`, calls
`distributed_index_sharding.partitioning.partition_index` **unmodified** to
produce `num_shards` self-contained shard directories plus a
`cluster_manifest.json`, and uploads every file of every shard directory back
to `ObjectStorage` under `shard-index/shard-<id>/`. Like `build-index`
(phase 3) and `compute-pagerank` (phase 4), this is a one-shot batch job, not
a `docker-compose.yml` service: it needs phase 3's global index to already be
finished, and re-running it (to change `num_shards`, or to re-shard after a
new crawl) is an explicit, occasional operator action, not something a
long-running container should retry on a loop — see `README.md`, "Usage
(CLI)" for the exact invocation, the same pattern already established for
`build-index`/`compute-pagerank`.

**Why shard directories round-trip through `ObjectStorage` instead of a
shared volume**: a shard replica (section 2 below) can be scheduled on any
container/host — that is the entire point of moving off
`LocalShardCluster`'s single-parent-process subprocesses — so its shard data
has to be reachable from wherever it starts, which a Docker bind mount or a
Kubernetes hostPath cannot guarantee across machines. `ObjectStorage` is
already the one storage primitive every phase agrees is reachable from any
worker (see phase 0, section 1); reusing it here instead of introducing a
second, container-specific distribution mechanism costs one upload and one
download of a few flat files per shard, not a new architectural concept.

**Why the downloaded/re-uploaded directories are always flat, never
recursive in a meaningful way**: both possible source formats — the
uncompressed `inverted-index-builder` layout (`manifest.json`,
`documents.jsonl`, `postings.jsonl`, `stats.json`) and the compressed
`index-compression-codec` layout (`manifest.json`, `documents.jsonl`,
`terms.jsonl`, `postings.bin`, `stats.json`, `compression_stats.json`) — are
themselves flat directories of sibling files (see each project's own README,
"Data formats"), and `partition_index`'s own output (`shard-<id>/manifest.json`
+ `documents.jsonl` + `postings.jsonl` + `stats.json`, always uncompressed —
see that function's own module docstring, "Por qué cada shard se escribe en
formato sin comprimir") is flat too. `_download_directory`/`_upload_directory`
(`query/shard_index_pipeline.py`) reflect that directly: list one prefix
level, download or upload every file found there, skip anything with a
further `/` in its key. No general-purpose recursive object-tree sync is
built because none of this phase's data shapes ever need one.

### 2. Serving a shard replica — `ShardReplicaService`, one subprocess of `serve-shard` per container

**Decision:** `ShardReplicaService` (`query/shard_replica_service.py`, exposed
as the `shard-replica` CLI subcommand) is this phase's equivalent of
`LocalShardCluster`, narrowed to a single replica and pointed at real
infrastructure instead of local subprocess bookkeeping:

1. downloads its own `shard-index/shard-<id>/` prefix from `ObjectStorage`
   into a local temporary directory (never the whole `shard-index/` tree —
   only the one shard this replica is configured to serve);
2. launches `python -m distributed_index_sharding serve-shard <dir>
   --shard-id <id> --host <host> --port <port>` as a real OS subprocess —
   the exact same invocation `LocalShardCluster.start` already uses, just one
   at a time instead of `num_shards` at once under one parent;
3. polls `GET /health` until it responds (the same polling shape
   `LocalShardCluster._wait_until_healthy` already uses, reimplemented here
   at the scope of one target because that method is not exposed as a
   reusable, standalone helper on `LocalShardCluster`);
4. registers itself in the phase-0 `ServiceRegistry` with
   `metadata={"shard_id": str(shard_id)}` (the convention section 0 depends
   on) and a configurable TTL;
5. renews that TTL on a fixed interval for as long as the process runs.

**Why a wrapper process around `serve-shard`, not a modified
`shard_server.py`**: the shard HTTP server itself (loading a
`BM25RankingPipeline`, answering `/search`/`/health`) is exactly
`distributed-index-sharding`'s job and stays untouched; everything this phase
adds — downloading shard data from a networked store, registering with a
service registry, renewing a heartbeat — is orchestration around an unmodified
binary, the same "materialize input, call the unmodified function/subprocess,
handle only what it cannot do itself" shape phases 3 and 4 already used for
`IndexBuilder.build` and `PageRankPipeline` respectively.

**Graceful shutdown vs. an ungraceful kill — the two paths this phase has to
support, and why only one of them runs any code at all**: `shutdown()`
cancels the heartbeat loop, explicitly deregisters from the `ServiceRegistry`,
then terminates the `serve-shard` subprocess (`SIGTERM`, `SIGKILL` after a
grace period) — the path a `docker stop`/`docker compose down` (`SIGTERM` to
the container's PID 1, handled by an explicit `signal.SIGTERM` handler
installed in the `shard-replica` CLI entrypoint, `__main__.py`) takes. A
`docker kill` (`SIGKILL`) — and the real scenario this phase's Definition of
Done requires testing, a container simply dying — gives this process no
opportunity to run *any* cleanup code, deregister included. That is not a gap
in this design; it is the exact scenario `ServiceRegistry`'s TTL contract
already exists to cover (phase 0, section 4: "a shard that dies without
warning must not keep appearing alive forever"). Once the heartbeat stops
arriving, Consul's TTL check goes critical (or `InMemoryServiceRegistry`'s own
TTL bookkeeping expires it) after `ttl_seconds`, and `discover()` — called
fresh before the very next query, see section 0 — simply stops returning that
replica. `ShardReplicaService.kill_process()` exists specifically to exercise
this path in tests without needing a real `docker kill`
(`tests/test_query_shard_replica_service.py::test_ungraceful_kill_is_only_noticed_after_ttl_expiry`),
the same role `LocalShardCluster.kill(shard_id)` already plays for
`distributed-index-sharding`'s own end-to-end test.

### 3. `docker-compose.yml` topology — one service per `shard_id`, not one scalable `shard` service

**Decision:** unlike `crawl-worker`/`extract-worker` (phases 1–2), where every
replica is interchangeable and a single `docker compose up --scale
crawl-worker=N` is enough, this phase adds three separate services —
`shard-0`, `shard-1`, `shard-2` — each running the same image with a
different `--shard-id`, each independently scalable
(`docker compose up -d --scale shard-0=2`).

**Why not one `shard` service parameterized by an environment variable**:
Compose replicas of the *same* service always share the same service-level
environment — there is no way to hand replica 1 `SHARD_ID=0` and replica 2
`SHARD_ID=1` under one `--scale shard=N`. Shards are not interchangeable the
way crawl/extract workers are (each owns a disjoint partition of `doc_id`s,
see `distributed-index-sharding`'s own partitioning rationale): a query
missing shard 1 entirely because two containers both happened to load shard
0 is a correctness bug, not graceful degradation. One Compose service per
`shard_id`, each independently scaled, is the minimum structure that lets
`--scale shard-0=2` mean "two redundant replicas of the same partition"
unambiguously — the exact requirement this phase's Definition of Done names
("al menos una réplica por shard sin que la caída de una tumbe esa
partición").

**Why no host ports are published**: `crawl-worker`/`extract-worker` never
needed inbound reachability (nothing calls them); shard replicas are the
first service in this repo that another process — the query-serving
coordinator layer itself — has to be able to reach. That reachability is
exactly what `ServiceRegistry` discovery is for: `shard-replica` announces
itself with `socket.gethostname()` (`_default_announce_host`,
`shard_replica_service.py`) as its `host`, the same short, Docker-assigned,
per-container hostname `crawl-worker`/`extract-worker` already use as their
default `--worker-id` (see their own environment comments in
`docker-compose.yml`) — resolvable by any other container on the same
user-defined bridge network. Publishing a fixed host port per shard would
both be redundant with this (nothing outside the Compose network needs to
reach a shard directly) and actively wrong once a service is scaled past one
replica (Compose cannot bind two replicas of the same service to the same
host port).

**Why `shard-index` is a CLI job, not a fourth Compose service**: same
reasoning phase 3/4 already established for `build-index`/`compute-pagerank`
— a one-shot operator action that must run after a prior phase's batch job
finished, not a process a container should hold open or retry on a timer (see
`README.md`, "Usage (CLI)"). `shard-0`/`shard-1`/`shard-2` tolerate
`shard-index` not having run yet: `ShardReplicaService.start` raises
`QueryServingError` when its shard's prefix is empty, `main()` reports it and
exits non-zero, and `restart: on-failure` retries the container — the
container simply keeps retrying until an operator runs `shard-index`, instead
of crash-looping opaquely or silently serving nothing.

### 4. `search` — a CLI demo of dynamic discovery, not a new network-facing service

`DistributedQueryServingPipeline` (`query/pipeline.py`) is this phase's
equivalent of `distributed_index_sharding.pipeline.DistributedSearchPipeline`:
it ties discovery (section 0) to `SearchCoordinator`, exposing the same
`search_text`/`search_parsed_query` calls. It keeps one `aiohttp.ClientSession`
open for its whole lifetime (the same connection-pooling reasoning
`HttpShardTransport` already applies within one search) but re-resolves the
`ShardTarget` list — a cheap, local `ServiceRegistry.discover` call, never a
network round trip to a shard — on every call, rather than fixing it once at
construction time the way `DistributedSearchPipeline` fixes its targets from
`LocalShardCluster.start`. The `search` CLI subcommand is a thin demo of this
programmatic API (mirroring `distributed-index-sharding`'s own `search`
subcommand, but discovering targets instead of taking `--shard` flags) —
this phase does not stand up a persistent, network-facing query API service
of its own; that is `beacon-search-console`'s job, a future consumer of this
same `DistributedQueryServingPipeline` class.

### Testing this phase

- **Discovery logic in isolation** (`tests/test_query_shard_discovery.py`):
  against `InMemoryServiceRegistry` directly, no network — one target per
  `shard_id`, deterministic tie-break among live replicas, failover only
  when the chosen replica stops being alive, and the two error conditions
  (`shard_id` metadata missing or non-numeric).
- **Fan-out, merge and failover over real HTTP, no subprocess**
  (`tests/test_query_pipeline.py`): real `distributed_index_sharding.shard_server`
  apps served by `aiohttp.test_utils.TestServer` (real sockets, not a fake
  transport), registered in `InMemoryServiceRegistry` — a healthy merge
  across two shards, degradation when one shard's server is closed without
  being deregistered (the real window between "a shard dies" and "the
  registry notices" that this phase's design in section 0 explicitly leaves
  to `distributed-index-sharding`'s own tolerance, not to discovery), and
  failover between two live replicas of the same `shard_id` after the chosen
  one is closed and deregistered.
- **A real `serve-shard` subprocess, end-to-end** (`tests/test_query_shard_replica_service.py`):
  `ShardReplicaService` against a real OS subprocess and
  `InMemoryServiceRegistry` — registration with correct `shard_id` metadata,
  a real query answered over a real socket, graceful shutdown deregistering
  immediately, and the ungraceful-kill path (`kill_process()`) only being
  noticed by `discover()` after TTL expiry, never immediately — plus one test
  chaining `ShardIndexPipeline` directly into `ShardReplicaService` with no
  manual step in between, the same handoff `docker-compose.yml`'s `shard-0`/
  `shard-1`/`shard-2` rely on in practice.
- **Real containers, not subprocesses** (`tests/test_query_docker_shard_failover.py`):
  brings up `minio`/`consul` plus `shard-0` (scaled to 2 replicas) and
  `shard-1` (1 replica) from this repo's actual `docker-compose.yml` via the
  `docker compose` CLI, runs `shard-index` against the running MinIO, then
  (a) `docker kill`s one of the two `shard-0` containers and asserts the
  partition keeps answering through its surviving replica, and (b) `docker
  kill`s the only `shard-1` container and asserts the coordinator degrades
  (that partition's documents missing from the merged result) without
  raising — the same criterion
  `distributed-index-sharding/tests/test_cluster_end_to_end.py` already
  applies to a killed local subprocess, here against a container Docker
  itself scheduled and can no longer be reached at all. Skipped automatically
  if the Docker daemon is not reachable, the same guard this ecosystem
  already applies to any test that needs real external infrastructure it
  cannot assume is running.

### Known limitations

- **`num_shards` is fixed by whichever `shard-index` run is currently
  deployed.** Changing it means re-running `shard-index` with a different
  `--num-shards` (which reshuffles every `doc_id % num_shards` assignment,
  see `distributed-index-sharding`'s own partitioning rationale) and
  restarting every shard replica against the new output — there is no live
  re-sharding, the identical caveat that repo's own README already states in
  "Adding or removing a shard without downtime" for changing `num_shards`
  specifically (as opposed to just adding capacity to an existing shard,
  which this phase's per-`shard_id` replica scaling already handles without
  downtime).
- **A `ServiceRegistry` that itself restarts with no persistent state
  (`InMemoryServiceRegistry` in a restarted process, or a fresh Consul agent
  with no data) is not recovered from by a running replica.** `heartbeat`
  renews an existing registration; it does not detect "the registry forgot
  me entirely" and re-`register`. In development, Consul's `-dev` agent
  (`docker-compose.yml`) already loses all state on restart for the same
  reason phase 0 accepted it (a single in-memory agent, no quorum, no
  persistence — see "Service registry" above); a replica surviving a Consul
  restart would need to notice `heartbeat` failing and fall back to
  `register` again, which `_heartbeat_loop` (`shard_replica_service.py`)
  currently logs and retries as if the failure were transient. Not fixed
  here because a development Consul restarting is rare and already a known,
  accepted gap of using `-dev` mode; revisit if a production Consul cluster
  (not `-dev`) is still found to restart often enough for this to matter.
- Same general principle every prior phase applies: nothing in this phase
  aborts a whole query over one shard's failure (`SearchCoordinator`'s own
  guarantee, reused unmodified) or over one replica's registration failing
  at startup (`ShardReplicaService.start` raises with enough context to
  identify which `shard_id`/`bucket` was misconfigured, rather than a bare
  stack trace from `distributed-index-sharding` or the registry SDK).

### Non-goals of phase 5

This phase does not: reimplement partitioning, the shard HTTP server,
fan-out, or merge (all `distributed-index-sharding`, unmodified), rerank
results with `learning-to-rank-reranker` or combine BM25 with the
`pagerank_scores.jsonl` phase 4 produced (a future phase's job, likely inside
`beacon-search-console` once it consumes `DistributedQueryServingPipeline`),
support live re-sharding when `num_shards` changes (see "Known limitations"
above), or stand up a persistent, network-facing search API service of its
own (`search` is a CLI demo of the programmatic pipeline, not that service).
It hands a future `beacon-search-console` integration a
`DistributedQueryServingPipeline` that already discovers shards dynamically,
tolerates individual replica failure, and fails over between redundant
replicas of the same partition, ready to sit behind a real HTTP API.

### Evolution to Kubernetes (documented, not implemented, in this phase)

Following the same "Compose now, Kubernetes as the deliberate next step"
reasoning phase 0 already established (see "Orchestration" above) and phase
1's own Kubernetes section (`Deployment`s registering themselves in Consul on
boot):

- Each `shard-N` Compose service becomes its own Kubernetes `Deployment`
  (`shard-0`, `shard-1`, `shard-2`, ...) with `spec.replicas` set to the
  desired redundancy for that partition — the same one-Deployment-per-`shard_id`
  shape `docker-compose.yml` already uses and for the same reason (section 3
  above): shards are not interchangeable, so there is no single Deployment
  that could scale them symmetrically. A `HorizontalPodAutoscaler` per shard
  Deployment (on query latency or CPU) is a natural later addition once query
  volume, not just redundancy, is the concern — out of scope here, the same
  "revisit only if profiling shows otherwise" bar phase 3/4 already apply to
  their own scaling questions.
- `ShardReplicaService`'s download-then-serve startup sequence maps directly
  onto a Kubernetes **init container**: an init container running
  `beacon-scale-infra shard-index`'s download half (or a dedicated
  "fetch this shard's data" entrypoint) populates an `emptyDir` volume shared
  with the main container, which then runs plain `distributed-index-sharding
  serve-shard` against that already-populated local path — splitting
  "fetch my partition" from "serve my partition" the way Kubernetes expects,
  instead of one process doing both sequentially as `ShardReplicaService`
  does today for Compose's simpler single-process-per-container model.
- **Registration and heartbeating are replaced by Kubernetes' own liveness
  primitives, not carried over as-is**: a `readinessProbe` against `GET
  /health` (already exposed by `shard_server.py`, unmodified) replaces this
  phase's own HTTP polling in `_wait_until_healthy`, and a pod that fails its
  `livenessProbe` is restarted by the kubelet without this phase's code
  needing to run any cleanup at all — the exact same "no code runs, the
  platform notices independently" shape `ServiceRegistry` TTL expiry already
  gives this phase for an ungraceful container kill (section 2 above), just
  provided by the orchestrator instead of by Consul. Whether Consul continues
  to be the discovery mechanism (each pod still running a Consul agent
  sidecar and calling `ServiceRegistry.register`, as phase 0's own Kubernetes
  section already describes for future compute in general) or is replaced by
  a Kubernetes `Service` with a headless `ClusterIP: None` DNS record
  enumerating one A-record per ready pod is a choice this phase does not
  need to make yet — `resolve_shard_targets` only depends on the
  `ServiceRegistry` protocol (phase 0), so swapping what implements discovery
  never touches `query/shard_discovery.py` or `query/pipeline.py`, the same
  protocol-first insulation phase 0 designed this substrate to give every
  later phase.
- **Pod anti-affinity, not this phase's own logic, guarantees redundancy
  survives a node failure**: `resolve_shard_targets`'s failover already
  tolerates *a* replica dying, but two replicas of `shard-0` scheduled on the
  same Kubernetes node both disappear together if that node fails — a
  `podAntiAffinity` rule (prefer, or require, distinct nodes for replicas of
  the same shard Deployment) is how a real cluster deployment closes that
  gap, configured entirely in the Deployment's pod spec, with zero change to
  this phase's code.
- The `shard-index` batch job becomes a Kubernetes `Job` (not a `CronJob`,
  matching phase 3/4's own framing of `build-index`/`compute-pagerank` as
  "run once, or whenever the corpus changes, not on a recurring schedule")
  invoked by an operator (or a future CI/CD pipeline step) after `build-index`'s
  own `Job` completes — the identical one-shot-batch-vs-long-running-service
  distinction this phase already draws for Compose (section 3 above),
  carried over unchanged.

## Phase 6 — The console over the real cluster

This phase puts the flagship application — `beacon-search-console`, the last
of the ten original repositories — in front of the phase-5 shard cluster: a
FastAPI service exposing the exact same versioned `/api/v1/...` contract that
app already defines, runnable as N interchangeable replicas behind a load
balancer, with a shared Redis result cache that is never allowed to serve
stale results silently.

`beacon-search-console` stays untouched (see "Why the ten original repos stay
untouched" above) and is reused as a **real package dependency** (pinned Git
URL, `#subdirectory=backend`): its API response models (`models.py` — reusing
them *is* what guarantees the contract stays identical, field by field), its
snippet construction (`build_snippet` — window + highlight ranges), and its
`stats.json` reader are imported unmodified, and its React frontend runs
against this API without any change. What this phase replaces is exactly the
app's *orchestration layer* (`dependencies.py`/`AppState`), which is built
for a single process on a single machine and breaks with more than one API
replica in two concrete ways:

1. **`AppState.build` starts real shard subprocesses** via
   `DistributedSearchPipeline.start` (`LocalShardCluster`), bound to
   `shard_host:shard_base_port..+N` on the local machine. A second replica on
   the same host collides on those ports; on a different host it spawns a
   redundant private copy of every shard instead of talking to the real
   phase-5 cluster.
2. **Everything else is loaded once into process memory** (snippet table,
   autocomplete tries, spellcheck vocabulary, reranker model and per-shard
   index readers) — independent copies per replica, with no shared cache or
   invalidation between them.

This phase resolves each of those pieces explicitly — either onto the shared
phase-0 substrate, or as state that is *safe to rebuild identically per
replica* — and documents the decision per piece (section 4).

### 0. The serving path, end to end

`ConsoleAppState` (`console/dependencies.py`) is the phase-6 analogue of the
console's `AppState`: built once per replica in the FastAPI lifespan, closed
explicitly on shutdown. A search request flows exactly like the original
console's (parse + spellcheck → fan-out → rerank → snippets), with the
fan-out now going through `ClusterSearchClient` (`console/cluster_search.py`)
— the same composition as phase 5's `DistributedQueryServingPipeline`
(fresh `ServiceRegistry` discovery before every query, one cheap
`SearchCoordinator` per query over a shared `aiohttp` session), reimplemented
at the console layer because the console needs one thing that class does not
expose: *which index version the chosen replica of each shard announces*
(section 2).

### 1. Replacing `DistributedSearchPipeline` — discovery, not subprocesses

The API process never starts, owns, or health-checks a shard. Shard replicas
are phase 5's `shard-replica` containers, discovered per query through the
phase-0 `ServiceRegistry` with the same one-target-per-`shard_id`
deterministic choice `resolve_shard_targets` already implements (refactored
into `choose_shard_instances` so the console can read the chosen instances'
metadata; the phase-5 function delegates to it unchanged). Killing an API
replica affects no shard; killing a shard replica degrades queries exactly as
phase 5 already guarantees. `distributed-index-sharding`'s coordinator,
transport and merge run unmodified, as everywhere else in this repo.

### 2. The index version — a content hash announced by the shards themselves

**The question this phase was required to answer explicitly: what event
invalidates the result cache?** The answer: **a change in the index version
that the live shard replicas announce**, verified per query.

**What the version is.** `build-index` (phase 3) now computes
`index_version = sha256` over the artifacts a query ultimately serves from —
the four merged index files plus the doc_id-aligned corpus file — and
publishes it as an `index_version.json` marker next to both index prefixes
(`search-index/`, `search-index-compressed/`). It is a *content* version, not
a timestamp or build counter: re-running `build-index` over the same phase-2
corpus produces byte-identical artifacts (phase 3's determinism, section 4)
and therefore the same version — correct, because cached results for that
index remain valid. Only a genuinely different corpus produces a different
version.

**How it reaches the serving path.** `shard-index` (phase 5) refuses to run
without the marker (the remedy is printed: re-run `build-index`), and
republishes it at `shard-index/index_version.json`; every `shard-replica`
reads it at startup and includes it in its `ServiceRegistry` registration
metadata (`{"shard_id": ..., "index_version": ...}`). The console therefore
learns, on every query and with zero extra round trips (the metadata rides on
the discovery response it already makes), which build **the live shards are
actually serving right now** — not which build happened to be in the bucket
when something started.

**Why not the alternatives:**

- *A timestamp or monotonic build counter* would invalidate the cache after a
  rebuild that changed nothing, and would need a coordination point to assign
  (exactly what phase 3 rejected for doc_ids: a central counter in a batch
  pipeline that is otherwise a pure function of its input).
- *Verifying against the bucket's marker at startup only* proves nothing
  about live shards: a shard replica started before the last `build-index`
  keeps serving its old partition regardless of what the bucket says now.
  (The startup check still exists — as an early operator warning, see
  `console/artifacts.py` — but the binding check is per query.)
- *A version endpoint on the shard HTTP server* would require modifying
  `distributed-index-sharding` — ruled out for every phase by "Why the ten
  original repos stay untouched"; the registry metadata channel already
  exists (phase 5 established it for `shard_id`) and costs nothing per query.

**Per-query coherence rules** (`ClusterSearchClient.snapshot`): a chosen
replica announcing the *same* version as the API loaded participates
normally and the query is cacheable. One announcing a *different* version is
**excluded from the fan-out** and reported as an explicit `error` in
`shard_statuses` — merging its doc_ids against another build's corpus would
render snippets of the wrong documents, which is precisely the silent
staleness this phase exists to prevent; explicit partial degradation is the
ecosystem's standard failure shape (phase 5, section 0). One announcing *no*
version (a `shard-index/` output predating the marker) participates — the
same unverified behavior the original console always had — but the query is
not cacheable, because without a verified version there is no safe cache
namespace.

### 3. The shared result cache — Redis, namespaced by index version

`CacheStore` joins the phase-0 substrate (`protocols.py`) with the standard
protocol-plus-two-implementations shape: `InMemoryCacheStore` (deterministic,
injectable clock, LRU-bounded — never unbounded growth) and `RedisCacheStore`
(`SET ... PX`/`GET`, every SDK error wrapped as `CacheError`).

**Connection/namespace decision, as required:** the cache reuses the **same
Redis instance** phase 0 already runs (the one hosting the message queue and
phase 1's crawl coordination) under a **dedicated key namespace**
(`beacon:console:cache:v1:...`) — the same call phase 1 already made when it
put the dedup set and rate-limiter keys on the existing instance rather than
operating a second Redis service. The **connection** is the API process's
own (`RedisCacheStore.from_url`): the console consumes no `MessageQueue`, so
there is no existing connection in that process to share — "reuse the
instance, own your connection".

**Invalidation without deletion.** The cache key embeds the verified index
version: `beacon:console:cache:v1:<index_version>:q=<sha256(query)>:limit=N`.
When the index is rebuilt and the shard replicas restart with the new build,
their announced version changes, so the key namespace changes at that exact
moment — no query ever reads an entry produced by the previous build, with no
`SCAN`+`DEL` sweep, no purge race between replicas, and no reliance on
operators remembering to flush anything. Orphaned entries of retired versions
expire on their own TTL (the TTL's only job — freshness within one version is
already guaranteed, because a version's results are immutable by
construction).

**What is never cached:** degraded responses (missing/failed/excluded shards
— transient states must not outlive their cause) and unverified-cluster
responses (section 2). **What a cache failure does:** Redis down or an entry
unreadable degrades to computing the query normally, logged — a cache can
make search cheaper, never make it fail (`CacheError` is caught at the
console layer, not propagated).

### 4. Per-piece decision: shared state vs. state rebuilt per replica

| `beacon-search-console` `AppState` piece | Phase-6 decision | Why |
|---|---|---|
| `pipeline` (shard subprocesses) | **shared infrastructure** — the phase-5 cluster, discovered via phase-0 `ServiceRegistry` | section 1: the one piece that *cannot* be per-replica |
| search results (uncached in the original) | **shared state** — Redis `CacheStore`, version-namespaced | sections 2–3: the one piece that must be shared *and* invalidated |
| `snippet_index` (whole corpus in RAM) | **neither** — resolved on demand from phase-2 partitions in `ObjectStorage`, bounded LRU of hot part files | section 5: gigabytes per replica at target scale; the data already lives in shared storage |
| `spell_checker`, `autocomplete_index` | **rebuilt per replica** at startup from the downloaded phase-3 index | pure deterministic functions of an immutable build artifact — N replicas build bit-identical structures, so sharing buys nothing; serializing the tries to storage would mean pickling another repo's internals across process boundaries, which the ecosystem forbids, and `query-parser-autocomplete` exposes no serialization of its own |
| `reranker_model`, `rerank_context` | **rebuilt per replica** (model downloaded from `ObjectStorage`, readers loaded once) | same determinism argument; see section 6 for the memory boundary this implies |
| `global_stats`, `last_crawled_at` | **rebuilt per replica** (tiny) | `stats.json` is a few hundred bytes; `last_crawled_at` is computed once by `build-index` and carried in the corpus catalog — never rescanned at API startup |

Anything rebuilt per replica is keyed to one `index_version` for the
replica's lifetime; the per-query check (section 2) is what makes a fleet of
replicas on mixed versions fail explicitly instead of subtly.

### 5. Snippets — `doc_id → (partition, part file, line)` against phase-2 partitions

The console's positional contract (`doc_id` = line number in one
`documents.jsonl` loaded whole into RAM) is replaced by an on-demand
resolution against the real phase-2 partitions, using the same construction
phase 3 already used for partitions, pushed one level deeper: `build-index`
now also publishes a **corpus catalog** (`search-index/corpus_catalog.json`)
assigning every *part file* its contiguous global doc_id range
`[start, start + non-blank-line-count)` — computed in the same
materialization pass phase 3 already makes, counting lines by exactly
`IndexBuilder.build`'s rule (blank lines don't consume a doc_id), plus the
corpus-wide `last_crawled_at` (max `fetched_at`, computed here so no replica
ever rescans the corpus). Resolution at query time is a binary search over
part boundaries (`bisect`, the same shape as `DocIdRangeAssignment.
partition_for`), one `get_object` for the single part file (~
`flush_every_pages` documents) on a miss, and a bounded in-process LRU for
hot parts. A doc_id out of range, a vanished part, or an unreadable part
resolves to `None` — that result is dropped and the rest served, the same
degradation the console applies to an inconsistent bootstrap.

**Why not the two simpler options:** loading phase 3's corpus file into every
replica's memory is the original console's design and is exactly what breaks
at target scale (gigabytes of `main_text` duplicated per replica); keeping it
on local disk with a byte-offset index would still download the full
multi-GB corpus on every replica start. The catalog costs one small JSON
object and reuses downloads the part-file granularity already gives for free.
(The corpus file itself is still written — phase 3, section 5's guarantee to
the *original* console deployment is not revoked by this phase.)

The pipeline also gained an integrity check for free: the per-part counts are
compared against the phase-2 manifest during `build-index`, so a manifest
that drifted from the real partitions (an `extract-worker` still writing)
now fails explicitly instead of producing a silently misaligned doc_id space
(the risk phase 3 section 0 describes, now enforced, not just documented).

### 6. Reranking — preloaded global readers, and the honest capacity boundary

`learning_to_rank_reranker.pipeline.rerank` constructs an
`InvertedIndexReader` (which loads `documents.jsonl` + `postings.jsonl`
fully into memory — that reader has no lazy mode) and re-reads the PageRank
scores *on every call*. The original console pays that cost per query per
shard; at this repo's scale that is a multi-gigabyte parse per search.
`PreloadedRerankContext` (`console/reranking.py`) composes the same public
pieces of that unmodified package — `InvertedIndexReader`,
`read_pagerank_scores`, `extract_features`, `LightGBMReranker.predict`, and
the ecosystem's standard deterministic tie-break — with the readers built
once at replica startup. It also dissolves the console's sharding↔reranking
bridge: candidates are reranked against the *global* phase-3 index (the
doc_ids shards return are global, and the global index contains every
candidate's features), so no grouping by owning shard is needed — that
grouping only ever existed because `rerank()` opens one directory per call
and the original console's `data/` layout made per-shard directories the
cheap option.

**The boundary, stated rather than hidden:** this keeps the full uncompressed
index (documents + postings) in each API replica's memory. At the lower end
of this project's target (∼1M documents) that is single-digit gigabytes and
acceptable for a handful of API replicas; toward the 5M upper end it becomes
the dominant per-replica cost. The correct evolution is moving feature
extraction into the shard servers themselves (each already holds its
shard-local index in memory — features could ride back on the existing
search response), but that means extending `distributed-index-sharding`'s
server and response contract, which "Why the ten original repos stay
untouched" rules out for this repo. Documented as the known limitation below
rather than half-solved here.

**The model artifact** has no producer among phases 0–5, so this phase adds
the missing batch job: `train-reranker` runs
`learning_to_rank_reranker.pipeline.train` unmodified (the same
deterministic synthetic-dataset training, same defaults, that the console's
own bootstrap runs) and uploads the saved model directory to `ObjectStorage`
(`ltr-model/`), from which every replica downloads it at startup. The model
is index-independent (synthetic features), so it carries no index version.

### 7. N API replicas behind a load balancer

`docker-compose.yml` adds `console-api` (this repo's image running
`serve-console`; no host ports published — two replicas cannot share one) and
`console-lb`, an nginx that resolves `console-api` against Docker's embedded
DNS with a short re-resolution TTL, so `docker compose up -d --scale
console-api=N` adds replicas to the round-robin without touching any
configuration and a restarted replica's new IP is picked up without
restarting the balancer. Replicas are interchangeable by construction
(sections 1–6): any replica answers any request, a cache entry written
through one is read through another, and killing one loses nothing but its
in-flight requests. The Kubernetes translation is the same one phase 5
already documents — a `Deployment` with `spec.replicas=N` behind a `Service`,
with the artifact download in an init container.

### 8. Serving the frontend from a CDN (documented, not implemented)

What the split looks like, given what the frontend actually is (a static
Vite/React build from `beacon-search-console`, unmodified):

- **Static, CDN-servable:** the build output (`index.html` plus
  content-hashed JS/CSS bundles). It contains no corpus data — results,
  snippets and stats all arrive over `/api/v1` at runtime — so a new index
  build does **not** change these assets and does **not** require a CDN
  purge. Vite's content-hashed filenames get immutable, long-TTL cache
  headers; `index.html` (the only non-hashed asset, it names the current
  bundles) gets a short TTL or `no-cache` with revalidation, which is the
  entire deployment story for a frontend-only release too.
- **Dynamic, never CDN-cached by default:** everything under `/api/v1/...`,
  which must keep hitting the load balancer. The CDN routes `/api/*` to the
  origin (LB) and everything else to the static bucket — the same-origin
  layout also removes the need for the wide-open CORS the development setup
  uses.
- **If API responses were later CDN-cached** (they are cacheable in
  principle: GET, no cookies, no per-user variation): the invalidation event
  is the same one section 2 defines — the index version — so the API would
  emit it (e.g. a `Cache-Control: s-maxage` bounded low, or a surrogate
  key equal to `index_version` purged when shards announce a new one). Not
  implemented because the Redis layer already de-duplicates repeated queries
  for every replica, and an edge cache only earns its invalidation
  complexity with geographically distributed read traffic this project does
  not have.

### Testing this phase

- **Cache backends** (`tests/test_cache_memory.py`, `tests/test_cache_redis.py`):
  the local implementation directly (injected clock for TTL, LRU bound and
  eviction order); the Redis implementation against `fakeredis` (native `PX`
  expiry actually applied, every failure wrapped as `CacheError`, including
  the disconnected-client path).
- **Corpus catalog and index version** (`tests/test_index_corpus_catalog.py`,
  `tests/test_index_version.py`): per-part ranges and blank-line/no-trailing-
  newline handling against the local storage backend, binary-search
  resolution, JSON round-trips, determinism of the content hash (same corpus
  → same version, different corpus → different version), both markers
  published, and the manifest-drift check failing explicitly.
- **Snippet resolution** (`tests/test_console_snippets.py`): across part and
  partition boundaries, hot parts downloaded exactly once (verified by
  counting real reads on a counting subclass of the local backend, not a
  mock), LRU eviction, and every degradation path resolving to `None`.
- **The API end to end** (`tests/test_console_app.py`): real artifacts built
  by the real phase-3/5/6 batch pipelines, real
  `distributed-index-sharding` shard servers over real sockets, the FastAPI
  app driven over ASGI — merged, reranked, snippeted results; a cache hit
  serving identical results after the entire cluster dies; degraded and
  unverified responses never cached; a stale-version replica excluded and
  reported; a replica-less partition reported; autocomplete and stats.
- **The reranker job** (`tests/test_console_reranker_job.py`): the published
  model round-trips through object storage back into `LightGBMReranker.load`.

### Known limitations

- **Picking up a new index build requires restarting the serving fleet**
  (shard replicas, then API replicas): every replica binds its artifacts to
  one `index_version` for its lifetime, and there is no live reload. The
  version check makes the transition explicit — shards already on the new
  build are excluded (with a named reason) by API replicas still on the old
  one, never merged incoherently — but a deployment that forgets to restart
  the APIs serves degraded responses until it does. Live artifact reload is
  deliberate future work, not an oversight: it would add a second lifecycle
  to every piece section 4 currently keeps immutable per process.
- **A shard replica without an announced version disables caching but not
  serving** (section 2): coherence against such a replica is unverifiable by
  construction. The gap closes itself as soon as `shard-index` has run once
  with the marker (all new replicas announce it).
- **Per-replica startup cost**: each API replica downloads the full
  uncompressed index (for the reranker's reader and the query-parser
  vocabulary) — minutes, not seconds, against a multi-GB index. Acceptable
  for a fleet of a few replicas; an artifact volume shared between replicas
  on one node, or the shard-side feature extraction of section 6, are the
  known escape hatches.
- **The reranker's in-memory global index** is the dominant per-replica
  memory cost at the top of the target scale — see section 6 for why the
  real fix lives outside this repo's constraints.
- The LTR model is trained on `learning-to-rank-reranker`'s synthetic
  dataset — inherited from the reference app, unchanged; a real click log
  would be a different project.

### Non-goals of phase 6

This phase does not: modify `beacon-search-console` (its backend package and
frontend are consumed as-is), reimplement parsing/BM25/merging/reranking
feature extraction (all reused unmodified), implement live index reload or
blue/green index swaps (see "Known limitations"), stand up a CDN (section 8
documents the split), add authentication/rate limiting to the API, or feed
autocomplete with a real query log (the synthetic default of
`query-parser-autocomplete` is used, exactly like the reference app).
