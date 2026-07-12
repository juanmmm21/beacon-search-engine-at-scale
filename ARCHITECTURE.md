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

## Non-goals of this phase

This phase does not: run a crawler, build an index, serve a query, decide
the sharding/partitioning scheme for a distributed index (that is the
concern of a later phase, extending `distributed-index-sharding`'s existing
by-document partitioning — see its own README — to run its shards as
Kubernetes-ready replicas instead of local subprocesses), or stand up
Kubernetes. It hands the next phase a tested, documented substrate to build
on.
