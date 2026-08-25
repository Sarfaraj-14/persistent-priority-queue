"""
module.py — Persistent Priority Queue
======================================

A double-ended priority queue (supports both extract_min and extract_max)
whose state is persisted to disk (JSON file, default) or to PostgreSQL.

DATA STRUCTURE
--------------
Two binary heaps (Python's `heapq`) share one authoritative store:

    self._entries : dict[id] -> {"priority": p, "value": v, "seq": s}
    self._min_heap: list of (priority, seq, id)
    self._max_heap: list of (-priority, seq, id)

`seq` is a monotonically increasing counter assigned every time an item is
inserted or updated. A heap tuple is "stale" if `self._entries` no longer
has a matching id/seq pair (the item was deleted, or updated and now lives
under a new seq). Stale tuples are discarded lazily whenever they surface
at the top of a heap ("lazy deletion") — the standard technique for
supporting update/delete on a heap-based priority queue without a full
O(n) rebuild on every mutation.

Complexities:
    insert        O(log n)
    extract_min   O(log n) amortised
    extract_max   O(log n) amortised
    peek          O(log n) amortised (no removal)
    update        O(log n)
    delete        O(1) amortised (lazy; cleanup deferred to next pop)
    is_empty      O(1)

PERSISTENCE
-----------
Only `self._entries` (+ seq counter) is ground truth; heaps rebuild from it
on load. Two backends:

    FileStorage     -> JSON file on disk (default, zero setup)
    PostgresStorage -> a single JSONB row in a PostgreSQL table

Every mutating call persists the resulting state immediately.

PUBLIC API (class PriorityQueue)
---------------------------------
    insert(value, priority, id=None) -> id
    extract_min() -> dict | None
    extract_max() -> dict | None
    peek(kind="min") -> dict | None
    update(id, priority=None, value=None) -> bool
    delete(id) -> bool
    is_empty() -> bool

MODULE-LEVEL CONVENIENCE FUNCTIONS
-----------------------------------
For callers that just want `import module; module.insert(...)`, this file
also exposes top-level functions of the same names operating on a
lazily-created default PriorityQueue backed by a local JSON file
(`pq_state.json`). Use `configure_file_storage()` or
`configure_postgres_storage()` to change the backend before first use.
"""

from __future__ import annotations

import heapq
import itertools
import json
import os
import threading
import uuid
from typing import Any, Optional, Dict, List, Tuple


# ---------------------------------------------------------------------------
# Storage backends
# ---------------------------------------------------------------------------

class StorageBackend:
    """Abstract persistence backend."""

    def load(self) -> Dict[str, Any]:
        raise NotImplementedError

    def save(self, state: Dict[str, Any]) -> None:
        raise NotImplementedError


class FileStorage(StorageBackend):
    """Persists queue state as a JSON file on disk."""

    def __init__(self, path: str = "pq_state.json"):
        self.path = path

    def load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {"seq": 0, "entries": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
                data.setdefault("seq", 0)
                data.setdefault("entries", {})
                return data
        except (json.JSONDecodeError, OSError):
            return {"seq": 0, "entries": {}}

    def save(self, state: Dict[str, Any]) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp_path, self.path)  # atomic on POSIX & Windows


class PostgresStorage(StorageBackend):
    """
    Persists queue state as a single JSONB row in a PostgreSQL table.

    Requires `psycopg2-binary` and a reachable PostgreSQL server. Run
    init_postgres.sql once to create the table, or leave create_table=True
    (default) to have it created automatically on first use.

    Connection parameters can be passed explicitly or picked up from the
    standard PG* environment variables via psycopg2's own defaults.
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        table_name: str = "priority_queue_state",
        row_id: int = 1,
        create_table: bool = True,
    ):
        try:
            import psycopg2
            import psycopg2.extras  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "PostgresStorage requires psycopg2-binary. "
                "Install it with: pip install psycopg2-binary"
            ) from exc

        self._psycopg2 = psycopg2
        self.dsn = dsn
        conn_kwargs = dict(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        self.conn_kwargs = {k: v for k, v in conn_kwargs.items() if v is not None}
        self.table_name = table_name
        self.row_id = row_id

        if create_table:
            self._ensure_table()

    def _connect(self):
        if self.dsn:
            return self._psycopg2.connect(self.dsn)
        return self._psycopg2.connect(**self.conn_kwargs)

    def _ensure_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id INTEGER PRIMARY KEY,
                        state JSONB NOT NULL,
                        updated_at TIMESTAMP NOT NULL DEFAULT now()
                    );
                    """
                )
            conn.commit()

    def load(self) -> Dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT state FROM {self.table_name} WHERE id = %s;",
                    (self.row_id,),
                )
                row = cur.fetchone()
        if row is None:
            return {"seq": 0, "entries": {}}
        state = row[0]
        state.setdefault("seq", 0)
        state.setdefault("entries", {})
        return state

    def save(self, state: Dict[str, Any]) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.table_name} (id, state, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (id)
                    DO UPDATE SET state = EXCLUDED.state, updated_at = now();
                    """,
                    (self.row_id, self._psycopg2.extras.Json(state)),
                )
            conn.commit()


# ---------------------------------------------------------------------------
# Core Priority Queue
# ---------------------------------------------------------------------------

class PriorityQueue:
    """
    A persistent, double-ended (min & max) priority queue.

    Lower `priority` values are "smaller" (returned first by extract_min);
    higher values are "bigger" (returned first by extract_max). Ties are
    broken by insertion/update order (FIFO) via an internal sequence
    counter.
    """

    def __init__(self, storage: Optional[StorageBackend] = None):
        self._storage = storage or FileStorage()
        self._lock = threading.RLock()

        self._entries: Dict[str, Dict[str, Any]] = {}
        self._min_heap: List[Tuple[float, int, str]] = []
        self._max_heap: List[Tuple[float, int, str]] = []
        self._seq_counter = itertools.count(1)
        self._next_seq_value = 0

        self._load()

    # -- persistence helpers -------------------------------------------------

    def _load(self) -> None:
        state = self._storage.load()
        self._entries = {}
        for id_, info in state.get("entries", {}).items():
            self._entries[id_] = {
                "priority": info["priority"],
                "value": info["value"],
                "seq": info["seq"],
            }
        self._next_seq_value = state.get("seq", 0)
        self._seq_counter = itertools.count(self._next_seq_value + 1)

        self._min_heap = [
            (info["priority"], info["seq"], id_)
            for id_, info in self._entries.items()
        ]
        self._max_heap = [
            (-info["priority"], info["seq"], id_)
            for id_, info in self._entries.items()
        ]
        heapq.heapify(self._min_heap)
        heapq.heapify(self._max_heap)

    def _persist(self) -> None:
        state = {"seq": self._next_seq_value, "entries": self._entries}
        self._storage.save(state)

    def _next_seq(self) -> int:
        val = next(self._seq_counter)
        self._next_seq_value = val
        return val

    # -- validity checks for lazy deletion -----------------------------------

    def _is_valid_min_tuple(self, tup: Tuple[float, int, str]) -> bool:
        priority, seq, id_ = tup
        entry = self._entries.get(id_)
        return entry is not None and entry["seq"] == seq and entry["priority"] == priority

    def _is_valid_max_tuple(self, tup: Tuple[float, int, str]) -> bool:
        neg_priority, seq, id_ = tup
        entry = self._entries.get(id_)
        return (
            entry is not None
            and entry["seq"] == seq
            and entry["priority"] == -neg_priority
        )

    def _clean_min_heap_top(self) -> None:
        while self._min_heap and not self._is_valid_min_tuple(self._min_heap[0]):
            heapq.heappop(self._min_heap)

    def _clean_max_heap_top(self) -> None:
        while self._max_heap and not self._is_valid_max_tuple(self._max_heap[0]):
            heapq.heappop(self._max_heap)

    @staticmethod
    def _to_result(id_: str, entry: Dict[str, Any]) -> Dict[str, Any]:
        return {"id": id_, "value": entry["value"], "priority": entry["priority"]}

    # -- public API -----------------------------------------------------------

    def insert(self, value: Any, priority: float, id: Optional[str] = None) -> str:
        """Insert a new item. Returns the item's id (auto-generated if not given)."""
        with self._lock:
            item_id = id if id is not None else uuid.uuid4().hex
            if item_id in self._entries:
                raise ValueError(f"An item with id={item_id!r} already exists.")

            seq = self._next_seq()
            self._entries[item_id] = {"priority": priority, "value": value, "seq": seq}
            heapq.heappush(self._min_heap, (priority, seq, item_id))
            heapq.heappush(self._max_heap, (-priority, seq, item_id))

            self._persist()
            return item_id

    def extract_min(self) -> Optional[Dict[str, Any]]:
        """Remove and return the item with the smallest priority, or None if empty."""
        with self._lock:
            self._clean_min_heap_top()
            if not self._min_heap:
                return None
            priority, seq, id_ = heapq.heappop(self._min_heap)
            entry = self._entries.pop(id_)
            self._persist()
            return self._to_result(id_, entry)

    def extract_max(self) -> Optional[Dict[str, Any]]:
        """Remove and return the item with the largest priority, or None if empty."""
        with self._lock:
            self._clean_max_heap_top()
            if not self._max_heap:
                return None
            neg_priority, seq, id_ = heapq.heappop(self._max_heap)
            entry = self._entries.pop(id_)
            self._persist()
            return self._to_result(id_, entry)

    def peek(self, kind: str = "min") -> Optional[Dict[str, Any]]:
        """
        Return (without removing) the item at the given end of the queue.
        `kind` is "min" (default) or "max".
        """
        with self._lock:
            if kind == "min":
                self._clean_min_heap_top()
                if not self._min_heap:
                    return None
                _, _, id_ = self._min_heap[0]
                return self._to_result(id_, self._entries[id_])
            elif kind == "max":
                self._clean_max_heap_top()
                if not self._max_heap:
                    return None
                _, _, id_ = self._max_heap[0]
                return self._to_result(id_, self._entries[id_])
            else:
                raise ValueError('kind must be "min" or "max"')

    def update(
        self,
        id: str,
        priority: Optional[float] = None,
        value: Any = ...,
    ) -> bool:
        """
        Update the priority and/or value of an existing item.
        Returns True if the item existed and was updated, False otherwise.

        NOTE: `value=...` (Ellipsis) is the sentinel meaning "leave value
        unchanged" so that legitimate falsy values (0, "", None, False)
        can still be assigned via update(id, value=None) etc.
        """
        with self._lock:
            entry = self._entries.get(id)
            if entry is None:
                return False

            new_priority = entry["priority"] if priority is None else priority
            new_value = entry["value"] if value is ... else value
            seq = self._next_seq()

            self._entries[id] = {"priority": new_priority, "value": new_value, "seq": seq}
            # Push fresh tuples; stale old ones will be skipped lazily on pop.
            heapq.heappush(self._min_heap, (new_priority, seq, id))
            heapq.heappush(self._max_heap, (-new_priority, seq, id))

            self._persist()
            return True

    def delete(self, id: str) -> bool:
        """
        Delete an item by id. Returns True if it existed, False otherwise.
        Uses lazy deletion: the entry is removed from the authoritative
        dict immediately; stale heap tuples are cleaned up on next pop.
        """
        with self._lock:
            if id not in self._entries:
                return False
            del self._entries[id]
            self._persist()
            return True

    def is_empty(self) -> bool:
        """Return True if the queue currently has no items."""
        with self._lock:
            return len(self._entries) == 0

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        return f"PriorityQueue(size={len(self._entries)})"


# ---------------------------------------------------------------------------
# Module-level convenience API (import module; module.insert(...))
# ---------------------------------------------------------------------------

_default_queue: Optional[PriorityQueue] = None
_default_storage: Optional[StorageBackend] = None


def _get_default_queue() -> PriorityQueue:
    global _default_queue, _default_storage
    if _default_queue is None:
        _default_queue = PriorityQueue(storage=_default_storage or FileStorage())
    return _default_queue


def configure_file_storage(path: str = "pq_state.json") -> PriorityQueue:
    """(Re)configure the default queue to use JSON file storage at `path`."""
    global _default_queue, _default_storage
    _default_storage = FileStorage(path)
    _default_queue = PriorityQueue(storage=_default_storage)
    return _default_queue


def configure_postgres_storage(**kwargs) -> PriorityQueue:
    """
    (Re)configure the default queue to use PostgreSQL storage.
    kwargs are forwarded to PostgresStorage(...), e.g.:
        configure_postgres_storage(host="localhost", dbname="pq_db",
                                    user="postgres", password="secret")
    """
    global _default_queue, _default_storage
    _default_storage = PostgresStorage(**kwargs)
    _default_queue = PriorityQueue(storage=_default_storage)
    return _default_queue


def insert(value: Any, priority: float, id: Optional[str] = None) -> str:
    return _get_default_queue().insert(value, priority, id)


def extract_min() -> Optional[Dict[str, Any]]:
    return _get_default_queue().extract_min()


def extract_max() -> Optional[Dict[str, Any]]:
    return _get_default_queue().extract_max()


def peek(kind: str = "min") -> Optional[Dict[str, Any]]:
    return _get_default_queue().peek(kind)


def update(id: str, priority: Optional[float] = None, value: Any = ...) -> bool:
    return _get_default_queue().update(id, priority, value)


def delete(id: str) -> bool:
    return _get_default_queue().delete(id)


def is_empty() -> bool:
    return _get_default_queue().is_empty()


# ---------------------------------------------------------------------------
# Simple manual demo when run directly
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Persistent Priority Queue demo (file storage: pq_state.json)")
    pq = PriorityQueue(storage=FileStorage("pq_state.json"))

    id1 = pq.insert("Pay invoice", priority=5)
    id2 = pq.insert("Fix critical bug", priority=1)
    id3 = pq.insert("Team lunch", priority=10)

    print("is_empty:", pq.is_empty())
    print("peek min:", pq.peek("min"))
    print("peek max:", pq.peek("max"))

    pq.update(id1, priority=0)  # promote invoice to top priority
    print("after update, peek min:", pq.peek("min"))

    pq.delete(id3)  # cancel team lunch
    print("extract_min:", pq.extract_min())
    print("extract_min:", pq.extract_min())
    print("is_empty:", pq.is_empty())
