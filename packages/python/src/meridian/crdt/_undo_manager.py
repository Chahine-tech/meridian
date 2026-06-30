"""Client-side collaborative undo for PNCounter (Kleppmann & Stewen PaPoC 2024).

PNCounter undo works by emitting the exact inverse op — increment undoes a
decrement and vice versa. These inverse ops are naturally CRDT-safe: they
commute with concurrent remote ops just like any other increment/decrement.

LwwRegister undo is handled directly on the register handle (server-validated).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ._pncounter import PNCounter

_MAX_STACK = 50

_PnOp = tuple[Literal["pn_incr", "pn_decr"], "PNCounter", int, int | None]


class UndoManager:
    """Tracks undoable CRDT operations and replays their inverses.

    Usage::

        um = UndoManager()
        um.pn_increment(counter, 5)   # tracked increment
        um.pn_decrement(counter, 2)   # tracked decrement
        um.undo()  # sends decrement(5) to server
        um.undo()  # sends increment(2) to server
    """

    def __init__(self) -> None:
        self._stack: list[_PnOp] = []

    @property
    def can_undo(self) -> bool:
        return bool(self._stack)

    def undo(self) -> None:
        """Apply the inverse of the most recent tracked operation."""
        if not self._stack:
            return
        kind, handle, amount, ttl_ms = self._stack.pop()
        if kind == "pn_incr":
            handle.decrement(amount, ttl_ms=ttl_ms)
        else:
            handle.increment(amount, ttl_ms=ttl_ms)


    def pn_increment(
        self,
        counter: PNCounter,
        amount: int = 1,
        *,
        ttl_ms: int | None = None,
    ) -> None:
        """Increment ``counter`` and record the inverse for ``undo()``."""
        counter.increment(amount, ttl_ms=ttl_ms)
        self._push(("pn_incr", counter, amount, ttl_ms))

    def pn_decrement(
        self,
        counter: PNCounter,
        amount: int = 1,
        *,
        ttl_ms: int | None = None,
    ) -> None:
        """Decrement ``counter`` and record the inverse for ``undo()``."""
        counter.decrement(amount, ttl_ms=ttl_ms)
        self._push(("pn_decr", counter, amount, ttl_ms))


    def _push(self, entry: _PnOp) -> None:
        if len(self._stack) >= _MAX_STACK:
            self._stack.pop(0)
        self._stack.append(entry)
