"""Unit tests for CRDT handles (no network required)."""

import asyncio
import pytest
from unittest.mock import MagicMock

from meridian.crdt import GCounter, PNCounter, LwwRegister, Presence, UndoManager


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


# ── LwwRegister undo ─────────────────────────────────────────────────────────

def test_lww_can_undo_after_set():
    r = LwwRegister("cfg", 1, make_transport())
    assert not r.can_undo
    r.set("hello")
    assert r.can_undo


def test_lww_undo_sends_undo_lww_message():
    t = make_transport()
    r = LwwRegister("cfg", 1, t)
    r.set("v1")
    t.send.reset_mock()
    r.undo()
    t.send.assert_called_once()
    msg = t.send.call_args[0][0]
    assert "UndoLww" in msg
    assert msg["UndoLww"]["crdt_id"] == "cfg"
    assert isinstance(msg["UndoLww"]["target_hlc"], bytes)
    assert isinstance(msg["UndoLww"]["restore_entry"], bytes)


def test_lww_undo_restores_prev_value_encoded_as_none_initially():
    """When there was no previous value, restore_entry must encode None."""
    import msgpack
    t = make_transport()
    r = LwwRegister("cfg", 1, t)
    r.set("first")
    r.undo()
    msg = t.send.call_args[0][0]
    restore = msgpack.unpackb(msg["UndoLww"]["restore_entry"], raw=False)
    assert restore is None


def test_lww_undo_restores_prev_value():
    """When a previous value existed, restore_entry must encode it."""
    import msgpack
    t = make_transport()
    r = LwwRegister("cfg", 1, t)
    r.set("v1")
    r.set("v2")
    r.undo()  # undo v2, restore v1
    msg = t.send.call_args[0][0]
    restore = msgpack.unpackb(msg["UndoLww"]["restore_entry"], raw=False)
    assert restore == "v1"


def test_lww_undo_pops_stack():
    t = make_transport()
    r = LwwRegister("cfg", 1, t)
    r.set("v1")
    r.set("v2")
    r.undo()
    r.undo()
    assert not r.can_undo


def test_lww_undo_noop_when_empty():
    t = make_transport()
    r = LwwRegister("cfg", 1, t)
    r.undo()  # must not raise
    t.send.assert_not_called()


def test_lww_apply_undo_ack():
    r = LwwRegister("cfg", 1, make_transport())
    r.set("v1")
    # Should not raise
    r._apply_undo_ack()


def test_lww_apply_undo_skipped():
    r = LwwRegister("cfg", 1, make_transport())
    r.set("v1")
    # Should not raise
    r._apply_undo_skipped("entry has been overwritten by a concurrent op")


async def test_lww_undo_results_stream_ack():
    r = LwwRegister("cfg", 1, make_transport())
    r.set("v1")

    results = []
    async def collect():
        async for res in r.undo_results():
            results.append(res)
            break

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    r._apply_undo_ack()
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, StopAsyncIteration):
        pass
    assert results == [{"ok": True}]


async def test_lww_undo_results_stream_skipped():
    r = LwwRegister("cfg", 1, make_transport())
    r.set("v1")

    results = []
    async def collect():
        async for res in r.undo_results():
            results.append(res)
            break

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    r._apply_undo_skipped("concurrent write")
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, StopAsyncIteration):
        pass
    assert results == [{"ok": False, "reason": "concurrent write"}]


# ── UndoManager ──────────────────────────────────────────────────────────────

def test_undo_manager_starts_empty():
    um = UndoManager()
    assert not um.can_undo


def test_undo_manager_pn_increment_and_undo():
    t = make_transport()
    p = PNCounter("score", 1, t)
    um = UndoManager()
    um.pn_increment(p, 5)
    assert p.value() == 5
    t.send.reset_mock()
    um.undo()
    # inverse is decrement(5)
    msg = t.send.call_args[0][0]
    assert "Op" in msg
    import msgpack
    op = msgpack.unpackb(msg["Op"]["op_bytes"], raw=False)
    assert "PNCounter" in op
    assert "Decrement" in op["PNCounter"]
    assert op["PNCounter"]["Decrement"]["amount"] == 5


def test_undo_manager_pn_decrement_and_undo():
    t = make_transport()
    p = PNCounter("score", 1, t)
    um = UndoManager()
    um.pn_decrement(p, 3)
    t.send.reset_mock()
    um.undo()
    import msgpack
    op = msgpack.unpackb(t.send.call_args[0][0]["Op"]["op_bytes"], raw=False)
    assert "Increment" in op["PNCounter"]
    assert op["PNCounter"]["Increment"]["amount"] == 3


def test_undo_manager_lifo_order():
    t = make_transport()
    p = PNCounter("score", 1, t)
    um = UndoManager()
    um.pn_increment(p, 1)
    um.pn_increment(p, 2)
    um.pn_increment(p, 3)
    # undo pops in LIFO order: 3, 2, 1
    import msgpack
    for expected in [3, 2, 1]:
        t.send.reset_mock()
        um.undo()
        op = msgpack.unpackb(t.send.call_args[0][0]["Op"]["op_bytes"], raw=False)
        assert op["PNCounter"]["Decrement"]["amount"] == expected


def test_undo_manager_noop_when_empty():
    um = UndoManager()
    um.undo()  # must not raise
    assert not um.can_undo


def test_undo_manager_respects_max_stack(monkeypatch):
    import meridian.crdt._undo_manager as mod
    monkeypatch.setattr(mod, "_MAX_STACK", 3)
    t = make_transport()
    p = PNCounter("score", 1, t)
    um = UndoManager()
    for i in range(5):
        um.pn_increment(p, i + 1)
    # Only last 3 entries should be kept
    assert len(um._stack) == 3
    # Top of stack is the most recent (amount=5)
    assert um._stack[-1][2] == 5


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
