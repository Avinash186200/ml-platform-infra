# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-11-25

### Added

- Terraform for AWS (modules `network`, `eks`, `storage`, `redis`, `iam`, `registry` and a `dev` environment): VPC with private subnets, one NAT and an S3 gateway endpoint, EKS with IRSA and IMDSv2-only nodes, a versioned KMS-encrypted artifact bucket, ElastiCache Redis with TLS and an AUTH token in Secrets Manager, ECR with immutable tags, one IAM role per workload scoped to S3 prefixes. Remote state documented, not required. Static checks only (fmt, validate, tflint, checkov); never applied.
- Kubernetes manifests (kustomize base + dev overlay): feature API, model API, job worker, nightly export CronJob, HPAs, PDBs, probes, NetworkPolicies, restricted Pod Security, Secret references only. Validated offline with kubeconform against pinned Kubernetes 1.31 schemas.
- Dagster asset graph: raw data, point-in-time feature snapshot, training set with purge and embargo, candidate model, block-bootstrap evaluation, gated model artifact, deployed-model pointer; nightly and weekly schedules; a promotion sensor; five asset checks (four blocking).
- Feature API with a strict as-of rule (restatement-aware), immutable SQLite snapshots, a Redis read-through cache keyed by snapshot version (fail-open), and a model API that scores a JSON model on those features.
- Async jobs: `POST /jobs` with idempotent submission, Redis queue with leases, crash resume and bounded retries; a worker process and a built-in feature export.
- Synthetic market data with publication delays and restatements, signal and control modes; a bench that writes `results/` and a test that reruns it.
- Dockerfile, optional docker-compose stack, CI (Python tests, Terraform, Kubernetes, checkov, image build, gitleaks).

### Fixed during development

- The AUC interval first resampled single test days. Labels overlap for 5 days and the features are persistent, so that interval was too narrow; it is now a moving-block bootstrap over 10-day blocks.
- Pointer files were first replaced with delete-then-move, which left a moment with no pointer at all; local writes now use an atomic `os.replace`, object stores a single PUT.
- The artifact store created "directories" on every write, which on S3 would have needed bucket-level permissions the workload roles do not have; it now does so only on local disk.
- The serving snapshot loader kept the SQLite files open after shutdown, which broke cleanup on Windows; it now closes old readers (keeping the previous two for instant rollback).
- ElastiCache first used the default parameter group; it now uses its own with `volatile-lru`, so memory pressure can evict cache entries (which have a TTL) but never job records (which do not).
- The Redis password was first passed as an environment variable; it is now a mounted Secret file (`MLPI_REDIS_PASSWORD_FILE`).

### Fixed after an independent review (before release)

- A rollback followed by the next nightly build overwrote a snapshot: the version was `CURRENT + 1`. It is now the highest snapshot file ever written + 1, snapshot writes refuse to replace a key, `CURRENT` records the file's SHA-256, pods verify it, and local files and cache keys include it.
- The blocking point-in-time check ran after `features/CURRENT` had already moved. `feature_snapshot` now only stages the file; a separate `publish_feature_snapshot` asset moves the pointer after the check passes.
- The worker never renewed its lease, so a job longer than the lease was reclaimed and run twice. A background heartbeat now renews it every lease/3.
- Job state changes were several Redis calls each. They are now single Lua scripts (submit, claim, heartbeat, complete, fail, reclaim) with a fencing token, so a crash or a race between reclaim and complete cannot queue a job twice, lose it, or let a stale worker overwrite a result.
- Job keys and parameters are validated (a key becomes part of an artifact path); finished job records expire after 7 days.
- NetworkPolicies had nothing to enforce them and no egress rules: the VPC CNI add-on with network policy is now in the Terraform, egress is denied by default, and only the feature API and worker may reach Redis.
- Second review: `publish_feature_snapshot` published whatever STAGED named, so running it alone after a failed check, or interleaved nightly runs, could publish an unchecked file. The passing check now records the version and hash it verified (`features/CHECKED`) and publish refuses unless STAGED matches; nightly runs are queued one at a time (`deploy/dagster/dagster.yaml`).
- Second review: the heartbeat thread died on the first Redis error, shared the cache's 0.2 s client, and kept a hung handler's lease forever. It now retries until the lease would expire, the queue has its own 5 s client, and a per-job `max_runtime_seconds` stops renewals (and liveness updates). Leases use the Redis server clock. Unknown kinds and features are rejected at submission and fail without retries.
- Second review: the snapshot loader held its lock during the pointer read and download, blocking requests; refresh is now single-flight with I/O outside the lock. The S3 path no longer relies on `exists()` checks: missing objects are detected by reading, immutable writes are conditional PUTs (`If-None-Match`), and the workload roles get plain `s3:ListBucket` so a missing key is a 404, not a 403.
- The EKS cluster role can now use the Secrets KMS key; the flow-logs role trust is limited to this account; the S3 rule that expired snapshots after 90 days (which could delete the one `CURRENT` names) is gone; old local snapshot files are deleted once no request holds them; the worker has a real liveness check; CI verifies the terraform and tflint downloads; the base image is pinned by digest.
