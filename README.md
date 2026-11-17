# Spark Pipeline Reliability Lab

A reproducible failure-injection lab for order ingestion: duplicate delivery, stale updates, conflicting versions, transaction rollback, and lost acknowledgements after commit.

**Scope:** Spark performs bounded batch canonicalization; SQLite provides a local transactional reference sink. This is a correctness lab, not a distributed production deployment or a throughput benchmark. The Spark result is checked against a Python reference implementation. Synthetic fixtures only.

## Run

Python 3.11 and Java 17 are the CI target. Local execution versions are recorded in the report. Install dependencies in a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m reliability.experiment --spark
```

If local Spark fails to bind the driver address, run with `SPARK_LOCAL_IP=127.0.0.1` (local mode only).

Windows activation: `.venv\Scripts\activate`. Without Spark, `python -m reliability.experiment` runs the transactional experiment using the standard library.

## Experiment and evidence

The experiment writes `reports/latest.json`, including Spark version and the executed physical plan when `--spark` is used. The committed report is an actual local run; a fresh run replaces it. CI generates its own downloadable evidence.

1. Construct 100 synthetic orders. Spark also receives ten duplicate records; canonical output must exactly equal the Python reference.
2. Inject an exception after writes but before commit. Verify the target has no rows.
3. Commit, then raise an exception before acknowledgement.
4. Close and reopen the database, retry the same batch, and verify exact ordered records and aggregate amount.
5. Execute two writes into an append-only baseline and compare row counts.

The baseline has 200 rows; the transactional target has 100. This measures duplicate prevention in this fixture, **not a performance improvement**. Eight unit tests cover rollback, replay, late versions, changed batch content, historical conflicts, schema constraints, and duplicate/permutation invariance.

## Data contract

Each event contains exactly `order_id` (nonempty string), `version` (positive integer), and `amount_cents` (nonnegative integer). A key/version identifies an immutable payload. A higher version wins; arrival order does not determine business recency. Exact duplicates are accepted. Conflicting key/version payloads abort the entire batch. Integer cents avoid floating-point money arithmetic.

Batch fingerprints include all distinct input versions. Reordered or repeated identical input is equivalent; reusing a batch ID for different content fails. The batch marker, event history, and current orders commit in one transaction.

## Repository map

- `reliability/pipeline.py`: input contract, Spark transform, atomic sink, failure points.
- `reliability/experiment.py`: reproducible baseline/recovery experiment.
- `tests/test_reliability.py`: behavioral invariants and negative cases.
- `reports/latest.json`: measured local evidence, including physical plan when available.
- `docs/architecture.md`: assumptions, failure boundaries, and distributed-system limits.
- `.github/workflows/tests.yml`: automated correctness checks and evidence artifacts.

Implementation was developed with AI assistance. Reproduce the results and inspect the transaction boundaries before making personal proficiency or production-experience claims.
