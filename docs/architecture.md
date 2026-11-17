# Architecture and failure analysis

## Why these boundaries exist

A writer can commit successfully while its caller sees failure. Retrying an append can duplicate business records. The experiment separates durable state from acknowledgement, then retries against a reopened database connection.

SQLite is deliberately a reference oracle: orders, immutable version history, and batch identity share one local transaction. It does not represent an external Spark task sink, a Delta transaction log, or a cluster-wide checkpoint protocol. Spark canonicalization is tested separately against a reference function; transaction ingestion retains the original versions so historical conflicts remain detectable.

## Invariants

- One current row per order ID.
- A current version never regresses.
- A key/version payload is immutable, including historical versions.
- Batch identity cannot silently refer to different input.
- Failure before commit leaves no partial order, history, or marker writes.
- Replay after lost acknowledgement returns an already-committed result.

The experiment compares exact rows, not only counts: equal counts can hide wrong values or missing keys. Aggregate cents is useful corroboration, not proof by itself.

## CPU and memory reasoning

Spark deduplication and the per-key window may introduce exchanges and sorts. The report records the executed plan rather than inferring those operators from Python source. JVM task memory and spill differ from driver memory. This lab collects only a bounded 110-row fixture. Its Python validation, sorting, hashing, and SQLite writes are driver-local and must not be scaled to arbitrary production input. No CPU, JVM RSS, GC, latency, or throughput improvement is claimed.

## Failure matrix

| Boundary | Expected state | Recovery |
| --- | --- | --- |
| Invalid contract | No writes | Correct or quarantine source |
| After writes, before commit | Transaction rolled back | Retry same input |
| After commit, before acknowledgement | Durable data and marker | Retry detects committed batch |
| Same batch ID, different content | Existing state preserved | Investigate source identity violation |
| Stale version | History retained; current row unchanged | No rollback of business state |
| Conflicting historical version | Entire batch rejected | Resolve source inconsistency |

## Limits and follow-up experiments

Exceptions exercise transaction rollback and acknowledgement loss; they do not simulate host power loss. Reopening a connection tests persistent state recovery but is not a process-kill experiment. Concurrency, busy timeouts, power loss durability, streaming checkpoints, deletion semantics, unbounded history growth, schema evolution, and multiple writers remain untested.

A distributed Delta implementation is a separate extension: establish merge-key uniqueness, ordering, immutable input snapshots, conflicting version policy, concurrent writers, and how progress relates to table commits. Do not assume a marker in a separate table is atomically committed with the target. Test failure at each boundary. This repository does not claim Databricks deployment or end-to-end exactly-once behavior.

## Evidence-first procedure

Before editing code, write the observation, proposed cause, competing cause, and a prediction that could disprove the hypothesis. Run the baseline; retain its output. Change one mechanism. Check correctness before comparing repeated timings. A smaller row count caused by data loss is not an optimization.
