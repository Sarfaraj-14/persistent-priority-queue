"""
test_module.py — unit tests for module.py

Run with:
    python -m unittest test_module.py -v
or:
    pytest test_module.py -v
"""

import os
import unittest

from module import PriorityQueue, FileStorage


TEST_FILE = "test_pq_state.json"


class TestPriorityQueue(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEST_FILE):
            os.remove(TEST_FILE)
        self.pq = PriorityQueue(storage=FileStorage(TEST_FILE))

    def tearDown(self):
        if os.path.exists(TEST_FILE):
            os.remove(TEST_FILE)
        tmp = TEST_FILE + ".tmp"
        if os.path.exists(tmp):
            os.remove(tmp)

    def test_is_empty_initially(self):
        self.assertTrue(self.pq.is_empty())

    def test_insert_and_peek(self):
        self.pq.insert("low", priority=5)
        self.pq.insert("high", priority=1)
        self.assertFalse(self.pq.is_empty())
        self.assertEqual(self.pq.peek("min")["value"], "high")

    def test_extract_min_order(self):
        self.pq.insert("c", priority=3)
        self.pq.insert("a", priority=1)
        self.pq.insert("b", priority=2)
        self.assertEqual(self.pq.extract_min()["value"], "a")
        self.assertEqual(self.pq.extract_min()["value"], "b")
        self.assertEqual(self.pq.extract_min()["value"], "c")
        self.assertTrue(self.pq.is_empty())

    def test_extract_max_order(self):
        self.pq.insert("c", priority=3)
        self.pq.insert("a", priority=1)
        self.pq.insert("b", priority=2)
        self.assertEqual(self.pq.extract_max()["value"], "c")
        self.assertEqual(self.pq.extract_max()["value"], "b")
        self.assertEqual(self.pq.extract_max()["value"], "a")

    def test_extract_from_empty_returns_none(self):
        self.assertIsNone(self.pq.extract_min())
        self.assertIsNone(self.pq.extract_max())
        self.assertIsNone(self.pq.peek("min"))
        self.assertIsNone(self.pq.peek("max"))

    def test_update_priority_changes_order(self):
        id_a = self.pq.insert("a", priority=5)
        self.pq.insert("b", priority=1)
        self.assertEqual(self.pq.peek("min")["value"], "b")

        self.pq.update(id_a, priority=0)
        self.assertEqual(self.pq.peek("min")["value"], "a")

    def test_update_value_only(self):
        id_a = self.pq.insert("old_value", priority=1)
        ok = self.pq.update(id_a, value="new_value")
        self.assertTrue(ok)
        self.assertEqual(self.pq.peek("min")["value"], "new_value")
        self.assertEqual(self.pq.peek("min")["priority"], 1)

    def test_update_nonexistent_returns_false(self):
        self.assertFalse(self.pq.update("does-not-exist", priority=1))

    def test_delete_existing(self):
        id_a = self.pq.insert("a", priority=1)
        self.pq.insert("b", priority=2)
        self.assertTrue(self.pq.delete(id_a))
        self.assertEqual(self.pq.peek("min")["value"], "b")

    def test_delete_nonexistent_returns_false(self):
        self.assertFalse(self.pq.delete("nope"))

    def test_lazy_deletion_does_not_resurrect_item(self):
        id_a = self.pq.insert("a", priority=1)
        self.pq.insert("b", priority=2)
        self.pq.delete(id_a)
        # 'a' must not resurface even though its stale heap tuple remains
        self.assertEqual(self.pq.extract_min()["value"], "b")
        self.assertTrue(self.pq.is_empty())

    def test_persistence_across_instances(self):
        self.pq.insert("persisted", priority=1)
        # Simulate a restart by creating a brand-new PriorityQueue
        # pointed at the same storage file.
        pq2 = PriorityQueue(storage=FileStorage(TEST_FILE))
        self.assertFalse(pq2.is_empty())
        self.assertEqual(pq2.peek("min")["value"], "persisted")

    def test_insert_duplicate_id_raises(self):
        self.pq.insert("a", priority=1, id="fixed-id")
        with self.assertRaises(ValueError):
            self.pq.insert("b", priority=2, id="fixed-id")

    def test_update_value_with_falsy_value(self):
        id_a = self.pq.insert("something", priority=1)
        ok = self.pq.update(id_a, value=0)
        self.assertTrue(ok)
        self.assertEqual(self.pq.peek("min")["value"], 0)

    def test_update_value_with_none(self):
        id_a = self.pq.insert("something", priority=1)
        ok = self.pq.update(id_a, value=None)
        self.assertTrue(ok)
        self.assertIsNone(self.pq.peek("min")["value"])

    def test_len(self):
        self.pq.insert("a", priority=1)
        self.pq.insert("b", priority=2)
        self.assertEqual(len(self.pq), 2)


if __name__ == "__main__":
    unittest.main()
