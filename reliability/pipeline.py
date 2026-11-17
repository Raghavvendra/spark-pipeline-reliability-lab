"""Bounded Spark batches and an atomic SQLite reference sink.

SQLite is a local transaction oracle, not a distributed production sink.
"""
import hashlib
import json
import sqlite3


class InjectedFailure(RuntimeError):
    pass


def canonicalize(events):
    """Reject conflicting versions, collapse duplicates, keep latest per key."""
    seen = {}
    for e in events:
        if set(e) != {'order_id', 'version', 'amount_cents'}:
            raise ValueError('Unexpected schema')
        if not isinstance(e['order_id'], str) or not e['order_id']:
            raise ValueError('Invalid order_id')
        if type(e['version']) is not int or e['version'] < 1:
            raise ValueError('Invalid version')
        if type(e['amount_cents']) is not int or e['amount_cents'] < 0:
            raise ValueError('Invalid amount')
        key = (e['order_id'], e['version'])
        if key in seen and seen[key] != e:
            raise ValueError('Conflicting payload for key/version')
        seen[key] = dict(e)
    latest = {}
    for e in seen.values():
        if e['order_id'] not in latest or e['version'] > latest[e['order_id']]['version']:
            latest[e['order_id']] = e
    return sorted(latest.values(), key=lambda e: e['order_id'])


def spark_canonicalize(spark, events):
    from pyspark.sql import Window, functions as F
    canonicalize(events)  # bounded fixture contract validation
    df = spark.createDataFrame(events, 'order_id string, version long, amount_cents long')
    w = Window.partitionBy('order_id').orderBy(F.col('version').desc())
    result = df.dropDuplicates().withColumn('_rank', F.row_number().over(w)).filter('_rank = 1').drop('_rank')
    rows = sorted([r.asDict() for r in result.collect()], key=lambda e: e['order_id'])
    return rows, result._jdf.queryExecution().executedPlan().toString()


def connect(path):
    db = sqlite3.connect(path)
    db.executescript('''
    CREATE TABLE IF NOT EXISTS orders(order_id TEXT PRIMARY KEY, version INTEGER NOT NULL, amount_cents INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS batches(batch_id TEXT PRIMARY KEY, digest TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS events(order_id TEXT, version INTEGER, amount_cents INTEGER NOT NULL, PRIMARY KEY(order_id, version));
    ''')
    return db


def apply_batch(db, batch_id, events, fail=None):
    canonicalize(events)
    # Include every distinct input version, not just the winning row.
    unique = {json.dumps(e, sort_keys=True) for e in events}
    digest = hashlib.sha256('\n'.join(sorted(unique)).encode()).hexdigest()
    with db:
        old = db.execute('SELECT digest FROM batches WHERE batch_id=?', (batch_id,)).fetchone()
        if old:
            if old[0] != digest:
                raise ValueError('Batch ID reused with different content')
            return 'already_committed'
        for e in events:
            key = (e['order_id'], e['version'])
            prior = db.execute('SELECT amount_cents FROM events WHERE order_id=? AND version=?', key).fetchone()
            if prior and prior[0] != e['amount_cents']:
                raise ValueError('Historical key/version conflict')
            db.execute('INSERT OR IGNORE INTO events VALUES (?,?,?)', (*key, e['amount_cents']))
        for e in canonicalize(events):
            db.execute('''INSERT INTO orders VALUES (?,?,?) ON CONFLICT(order_id) DO UPDATE SET
            version=excluded.version, amount_cents=excluded.amount_cents
            WHERE excluded.version > orders.version''', (e['order_id'], e['version'], e['amount_cents']))
        if fail == 'before_commit':
            raise InjectedFailure('Write occurred but transaction not committed')
        db.execute('INSERT INTO batches VALUES (?,?)', (batch_id, digest))
    if fail == 'after_commit':
        raise InjectedFailure('Commit durable; acknowledgement lost')
    return 'committed'


def snapshot(db):
    return db.execute('SELECT order_id, version, amount_cents FROM orders ORDER BY order_id').fetchall()
