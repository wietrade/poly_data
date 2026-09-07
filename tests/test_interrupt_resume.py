"""Interrupt/resume safety tests for the HyperSync consume loop (_consume).

The whole point of chunked Parquet + cursor is that an interrupted backfill can
be resumed exactly — no duplicates, no gaps. These tests drive _consume with a
fake receiver that yields synthetic OrderFilled batches, then simulate the
failure modes:

  1. clean end                -> cursor == query end (chain tip + 1)
  2. Ctrl-C (KeyboardInterrupt) mid-stream
                             -> cursor == last consumed batch boundary (NOT the
                                chain tip), so no blocks are silently skipped
  3. generic crash mid-stream -> cursor == last flushed part boundary; a follow-
                                up run resumes without duplicating flushed parts
  4. resume after interrupt  -> produces exactly the same total as a clean run
"""

import asyncio
import json
import types

import pytest
import update_utils.update_chain as uc
from eth_abi import encode as abi_encode
from update_utils.update_chain import (
    _DATA_TYPES,
    _consume,
)

# --- synthetic log builders (mirror test_update_chain._make_log) ---


def _topic_addr(addr_40hex: str) -> str:
    return "0x" + "00" * 12 + addr_40hex


def _make_log(block_number, maker="aa" * 20, taker="bb" * 20):
    data = abi_encode(
        _DATA_TYPES,
        [0, 12345, 3_400_000, 5_000_000, 0, b"\x00" * 32, b"\x00" * 32],
    )
    return types.SimpleNamespace(
        topics=["0xsig", "0xorderhash", _topic_addr(maker), _topic_addr(taker)],
        data="0x" + data.hex(),
        block_number=block_number,
        transaction_hash="0x" + "cd" * 32,
    )


def _batch(from_block: int, to_block_exclusive: int, logs_per_block: int = 1):
    """One receiver payload covering [from_block, to_block_exclusive)."""
    blocks = [
        types.SimpleNamespace(number=b, timestamp=1_700_000_000 + b)
        for b in range(from_block, to_block_exclusive)
    ]
    logs = []
    for b in range(from_block, to_block_exclusive):
        logs.extend(_make_log(block_number=b) for _ in range(logs_per_block))
    return types.SimpleNamespace(
        next_block=to_block_exclusive,
        data=types.SimpleNamespace(blocks=blocks, logs=logs),
    )


class _SeqReceiver:
    """Returns batches in order, then None. Optionally raises at a position."""

    def __init__(self, batches, raise_at=None, exc=KeyboardInterrupt):
        self._batches = list(batches)
        self._raise_at = raise_at  # raise before yielding batch at this index
        self._exc = exc
        self._i = 0

    async def recv(self):
        if self._raise_at is not None and self._i == self._raise_at:
            raise self._exc("simulated interrupt")
        if self._i >= len(self._batches):
            return None
        b = self._batches[self._i]
        self._i += 1
        return b


def _run_consume(tmp_path, monkeypatch, receiver, start_block, end_exclusive):
    """Run _consume with isolated PARTS_DIR/CURSOR_FILE and a tiny part size."""
    parts = tmp_path / "order_filled"
    cursor = tmp_path / "cursor_state.json"
    monkeypatch.setattr(uc, "PARTS_DIR", str(parts))
    monkeypatch.setattr(uc, "CURSOR_FILE", str(cursor))
    monkeypatch.setattr(uc, "ROWS_PER_PART", 2)  # force frequent flushes
    return (
        asyncio.run(
            _consume(
                receiver, start_block, part_index=0, end_block_exclusive=end_exclusive
            )
        ),
        parts,
        cursor,
    )


def _read_cursor(cursor):
    with open(cursor) as f:
        return json.load(f)


class TestCleanEnd:
    def test_completed_sets_cursor_to_query_end(self, tmp_path, monkeypatch):
        # 4 blocks, one log each; ROWS_PER_PART=2 -> 2 parts, tail flushed.
        receiver = _SeqReceiver([_batch(100, 102), _batch(102, 104)])
        (_, completed, total), parts, cursor = _run_consume(
            tmp_path, monkeypatch, receiver, start_block=100, end_exclusive=104
        )
        assert completed is True
        assert total == 4
        # clean end -> cursor lands exactly at the query end (chain tip + 1)
        assert _read_cursor(cursor) == {"last_block": 104, "part_index": 2}
        # two full parts written (2 rows each), no tmp leftovers
        assert sorted(p.name for p in parts.glob("*.parquet")) == [
            "part_00000000.parquet",
            "part_00000001.parquet",
        ]
        assert not list(parts.glob("*.tmp"))


class TestInterrupt:
    def test_ctrl_c_stops_at_last_batch_not_chain_tip(self, tmp_path, monkeypatch):
        # Batches cover 100..106; interrupt arrives before the batch 102..104.
        receiver = _SeqReceiver(
            [_batch(100, 102), _batch(102, 104), _batch(104, 106)],
            raise_at=1,  # raise before the SECOND batch
        )
        with pytest.raises(KeyboardInterrupt):
            _run_consume(
                tmp_path, monkeypatch, receiver, start_block=100, end_exclusive=106
            )
        state = _read_cursor(tmp_path / "cursor_state.json")
        # Must NOT jump to 106 (chain tip). Must resume at 102 (last consumed
        # batch boundary) so blocks 102..105 are NOT skipped.
        assert state["last_block"] == 102
        assert state["part_index"] == 1

    def test_crash_stops_at_last_flush_boundary(self, tmp_path, monkeypatch):
        # First batch fills part 0 fully (2 rows). Crash before batch 2.
        receiver = _SeqReceiver(
            [_batch(100, 102), _batch(102, 104), _batch(104, 106)],
            raise_at=1,
            exc=RuntimeError,
        )
        with pytest.raises(RuntimeError):
            _run_consume(
                tmp_path, monkeypatch, receiver, start_block=100, end_exclusive=106
            )
        state = _read_cursor(tmp_path / "cursor_state.json")
        # part 0 landed (rows for blocks 100,101); cursor = last flush boundary
        assert state["last_block"] == 102
        assert state["part_index"] == 1
        parts = tmp_path / "order_filled"
        assert (parts / "part_00000000.parquet").exists()

    def test_interrupt_then_resume_is_exact(self, tmp_path, monkeypatch):
        """Run 1 interrupted at batch 2; run 2 resumes and equals a clean run."""
        # --- run 1: interrupt before the batch covering 102..104 ---
        r1 = _SeqReceiver(
            [_batch(100, 102), _batch(102, 104), _batch(104, 106)],
            raise_at=1,
        )
        with pytest.raises(KeyboardInterrupt):
            _run_consume(tmp_path, monkeypatch, r1, start_block=100, end_exclusive=106)
        state1 = _read_cursor(tmp_path / "cursor_state.json")
        assert state1["last_block"] == 102

        # --- run 2: resume from cursor, clean end ---
        r2 = _SeqReceiver([_batch(102, 104), _batch(104, 106)])
        # part_index must continue from 1, not restart at 0
        parts2 = tmp_path / "order_filled"
        cursor2 = tmp_path / "cursor_state.json"
        monkeypatch.setattr(uc, "ROWS_PER_PART", 2)
        _, completed, total = asyncio.run(
            _consume(
                r2,
                state1["last_block"],
                part_index=state1["part_index"],
                end_block_exclusive=106,
            )
        )
        assert completed is True
        assert total == 4  # blocks 102..105 -> 4 logs
        state2 = _read_cursor(cursor2)
        assert state2["last_block"] == 106
        assert state2["part_index"] == 3

        # parts written across both runs: run1 wrote part0 (full 2 rows); run2
        # continues at part_index=1 and writes parts 1 & 2 (4 rows, no tail).
        names = sorted(p.name for p in parts2.glob("*.parquet"))
        assert names == [
            "part_00000000.parquet",
            "part_00000001.parquet",
            "part_00000002.parquet",
        ]

        # The key guarantee: every block 100..105 appears exactly once across
        # the concatenation of all parts (no gaps from the interrupt, and no
        # duplicates from re-fetching the flushed range).
        import polars as pl

        concat = pl.concat(
            [pl.read_parquet(str(p)) for p in sorted(parts2.glob("*.parquet"))],
            how="vertical",
        )
        # block -> timestamp 1_700_000_100..1_700_000_105 each appears once
        per_ts = concat.group_by("timestamp").len().sort("timestamp")
        timestamps = per_ts["timestamp"].to_list()
        assert timestamps == [1_700_000_100 + b for b in range(6)]
        assert per_ts["len"].to_list() == [1] * 6
