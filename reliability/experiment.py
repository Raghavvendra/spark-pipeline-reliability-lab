"""Run with python -m reliability.experiment [--spark]."""
import argparse
import json
import platform
import tempfile
from pathlib import Path
from reliability.pipeline import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spark', action='store_true')
    args = parser.parse_args()
    events = [{'order_id': f'order-{i}', 'version': 1, 'amount_cents': 100+i} for i in range(100)]
    report = {'python': platform.python_version(), 'scope': 'bounded local correctness experiment', 'input_records': len(events)}
    if args.spark:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master('local[2]').appName('reliability-lab').config('spark.sql.shuffle.partitions','4').getOrCreate()
        try:
            rows, plan = spark_canonicalize(spark, events + events[:10])
            assert rows == canonicalize(events)
            report.update(spark_version=spark.version, physical_plan=plan, spark_matches_reference=True)
        finally: spark.stop()
    with tempfile.TemporaryDirectory() as directory:
        db = connect(str(Path(directory)/'sink.db'))
        try:
            try: apply_batch(db, 'batch-1', events, 'before_commit')
            except InjectedFailure: pass
            assert snapshot(db) == []
            report['precommit_failure_rows'] = 0
            try: apply_batch(db, 'batch-1', events, 'after_commit')
            except InjectedFailure: pass
            db.close()
            db = connect(str(Path(directory)/'sink.db'))
            report['retry_status'] = apply_batch(db, 'batch-1', events)
            actual = snapshot(db)
            assert actual == [(e['order_id'],e['version'],e['amount_cents']) for e in canonicalize(events)]
            report.update(naive_append_replay_rows=len(events)*2, transactional_replay_rows=len(actual), total_amount_cents=sum(r[2] for r in actual), restart_recovery_passed=True)
            # Baseline is an executed append-only sink, deliberately lacking deduplication.
            db.execute('CREATE TABLE naive(order_id TEXT, version INTEGER, amount_cents INTEGER)')
            for _ in range(2): db.executemany('INSERT INTO naive VALUES (?,?,?)', [(e['order_id'],e['version'],e['amount_cents']) for e in events])
            assert db.execute('SELECT count(*) FROM naive').fetchone()[0] == report['naive_append_replay_rows']
        finally: db.close()
    Path('reports').mkdir(exist_ok=True)
    Path('reports/latest.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__ == '__main__': main()
