import unittest
from reliability.pipeline import apply_batch, canonicalize, connect, snapshot, InjectedFailure

A = {'order_id': 'A', 'version': 1, 'amount_cents': 100}
B = {'order_id': 'B', 'version': 1, 'amount_cents': 200}

class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')
    def tearDown(self):
        self.db.close()
    def test_before_commit_rolls_back_data_and_marker(self):
        with self.assertRaises(InjectedFailure): apply_batch(self.db, '1', [A,B], 'before_commit')
        self.assertEqual(snapshot(self.db), [])
        self.assertEqual(self.db.execute('SELECT count(*) FROM batches').fetchone()[0], 0)
        apply_batch(self.db, '1', [A,B])
        self.assertEqual(len(snapshot(self.db)), 2)
    def test_ack_loss_retry_is_idempotent(self):
        with self.assertRaises(InjectedFailure): apply_batch(self.db, '1', [A,B], 'after_commit')
        before = snapshot(self.db)
        self.assertEqual(apply_batch(self.db, '1', [B,A,A]), 'already_committed')
        self.assertEqual(before, snapshot(self.db))
    def test_late_version_cannot_regress(self):
        apply_batch(self.db, '2', [dict(A,version=2,amount_cents=150)])
        apply_batch(self.db, '1', [A])
        self.assertEqual(snapshot(self.db), [('A',2,150)])
    def test_reused_batch_id_rejected(self):
        apply_batch(self.db, '1', [A])
        with self.assertRaises(ValueError): apply_batch(self.db, '1', [B])
    def test_conflicting_version_rejected_atomically(self):
        apply_batch(self.db, '1', [A])
        with self.assertRaises(ValueError): apply_batch(self.db, '2', [B,dict(A,amount_cents=999)])
        self.assertEqual(snapshot(self.db), [('A',1,100)])
    def test_historical_conflict_rejected(self):
        apply_batch(self.db, '1', [A,dict(A,version=2)])
        with self.assertRaises(ValueError): apply_batch(self.db, '2', [dict(A,amount_cents=999)])
    def test_invalid_contract(self):
        for e in [dict(A,amount_cents=-1),dict(A,version=True),dict(A,order_id='')]:
            with self.assertRaises(ValueError): canonicalize([e])
    def test_permutation_and_duplicate_invariance(self):
        self.assertEqual(canonicalize([A,B,A]), canonicalize([B,A]))

if __name__ == '__main__': unittest.main()
