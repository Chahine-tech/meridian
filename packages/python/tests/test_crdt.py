"""Unit tests for CRDT handles (no network required)."""

import asyncio
import pytest
from unittest.mock import MagicMock

from meridian.crdt import GCounter, PNCounter, LwwRegister, Presence


def make_transport():
    t = MagicMock()
    t.send = MagicMock()
    return t


# ── GCounter ─────────────────────────────────────────────────────────────────

def test_gcounter_starts_at_zero():
    g = GCounter("hits", 1, make_transport())
    assert g.value() == 0


def test_gcounter_increment_local():
    g = GCounter("hits", 1, make_transport())
    g.increment(5)
    assert g.value() == 5


def test_gcounter_increment_sends_op():
    t = make_transport()
    g = GCounter("hits", 42, t)
    g.increment(3)
    t.send.assert_called_once()
    msg = t.send.call_args[0][0]
    assert "Op" in msg
    assert msg["Op"]["crdt_id"] == "hits"


def test_gcounter_apply_delta_merges():
    g = GCounter("hits", 1, make_transport())
    g._apply_delta({"counters": {"1": 10, "2": 5}})
    assert g.value() == 15


def test_gcounter_apply_delta_max_wins():
    g = GCounter("hits", 1, make_transport())
    g._apply_delta({"counters": {"1": 10}})
    g._apply_delta({"counters": {"1": 3}})   # lower — ignored
    assert g.value() == 10


def test_gcounter_increment_rejects_non_positive():
    g = GCounter("hits", 1, make_transport())
    with pytest.raises(ValueError):
        g.increment(0)
    with pytest.raises(ValueError):
        g.increment(-1)


async def test_gcounter_changes_stream():
    g = GCounter("hits", 1, make_transport())

    results = []
    async def collect():
        async for v in g.changes():
            results.append(v)
            if len(results) >= 2:
                break

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    g.increment(1)
    g.increment(4)
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, StopAsyncIteration):
        pass
    assert results == [1, 5]


# ── PNCounter ────────────────────────────────────────────────────────────────

def test_pncounter_increment_and_decrement():
    p = PNCounter("score", 1, make_transport())
    p.increment(10)
    p.decrement(3)
    assert p.value() == 7


def test_pncounter_apply_delta():
    p = PNCounter("score", 1, make_transport())
    p._apply_delta({"pos": {"counters": {"1": 20}}, "neg": {"counters": {"2": 5}}})
    assert p.value() == 15


# ── LwwRegister ──────────────────────────────────────────────────────────────

def test_lww_starts_null():
    r = LwwRegister("cfg", 1, make_transport())
    assert r.value() is None
    assert r.meta() is None


def test_lww_set_local():
    r = LwwRegister("cfg", 1, make_transport())
    r.set({"theme": "dark"})
    assert r.value() == {"theme": "dark"}


def test_lww_apply_delta_wins_later_timestamp():
    r = LwwRegister("cfg", 1, make_transport())
    r._apply_delta_sync({"entry": {"value": "old", "hlc": {"wall_ms": 100, "logical": 0, "node_id": 1}, "author": 1}})
    r._apply_delta_sync({"entry": {"value": "new", "hlc": {"wall_ms": 200, "logical": 0, "node_id": 2}, "author": 2}})
    assert r.value() == "new"


def test_lww_apply_delta_older_loses():
    r = LwwRegister("cfg", 1, make_transport())
    r._apply_delta_sync({"entry": {"value": "first", "hlc": {"wall_ms": 200, "logical": 0, "node_id": 1}, "author": 1}})
    r._apply_delta_sync({"entry": {"value": "stale", "hlc": {"wall_ms": 100, "logical": 0, "node_id": 2}, "author": 2}})
    assert r.value() == "first"


# ── Presence ─────────────────────────────────────────────────────────────────

async def test_presence_online_excludes_expired():
    p = Presence("room", 1, make_transport())
    p._entries["1"] = __import__("meridian.crdt._presence", fromlist=["PresenceEntry"]).PresenceEntry(
        client_id=1, data={"name": "Alice"}, expires_at_ms=0
    )
    assert p.online() == []


async def test_presence_leave_removes_entry():
    p = Presence("room", 1, make_transport())
    import time
    from meridian.crdt._presence import PresenceEntry
    p._entries["1"] = PresenceEntry(client_id=1, data={}, expires_at_ms=int(time.time() * 1000) + 30_000)
    p.leave()
    assert "1" not in p._entries
