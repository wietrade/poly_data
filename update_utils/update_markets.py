"""
Fetch the full Polymarket market list from the CLOB API into data/markets.csv.

CLOB's /markets endpoint paginates 1000 rows/page (vs the Gamma keyset's hard
cap of 100) with an offset-based cursor, so the entire ~1.5M-market history —
overwhelmingly closed/resolved markets — can be pulled with concurrent requests
in well under a minute, instead of the ~hour the sequential Gamma keyset took.

Each market is written with the columns process_live expects:

    id            -> CLOB condition_id (stable on-chain market identifier)
    clobTokenIds  -> JSON array of the market's CLOB token_ids (token1, token2)

plus every other field the CLOB market object carries, preserved as-is (nested
values JSON-encoded), mirroring the previous Gamma-based schema.

Resumable: the next offset and discovered column order are saved to
data/markets_state.json so an interrupted run picks up where it left off.
"""

import base64
import csv
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

CLOB_MARKETS = "https://clob.polymarket.com/markets"
PAGE = 1000  # CLOB's fixed page size
WAVE = 20  # concurrent pages per wave (CLOB allows 9000 req / 10s)
END_CURSOR = "LTE="  # base64("-1"): CLOB's end-of-data marker
MAX_RETRIES = 8

MARKETS_CSV = "data/markets.csv"
STATE_FILE = "data/markets_state.json"

# Columns preserved when in "lean" mode (the default). The CLOB market object
# carries ~31 fields; most are useless for downstream analysis and some are
# huge (duplicated image/icon URLs, long description templates, rewards
# params). Lean mode keeps only what the pipeline actually reads plus a few
# cheap fields useful for analysis/filtering:
#
#   id / clobTokenIds / market_slug   -> required by process_live + slug filter
#   condition_id / question_id        -> alternate ids (same value as id for v2)
#   question                          -> short human-readable question
#   closed / active                   -> market state for filtering
#   end_date_iso                      -> when the market resolved/expires
#   tags                              -> compact category list (analysis)
#
# Deliberately DROPPED: image, icon (same URL twice), description (long
# template text, ~45% of raw size, useless for analysis), rewards
# (liquidity-incentive params), fpmm/neg_risk_* (legacy infra),
# maker/taker_base_fee (not used), notifications_enabled, minimum_*,
# seconds_delay, is_50_50_outcome, game_start_time, archived,
# enable_order_book, neg_risk, accepting_orders, accepting_order_timestamp.
#
# Set KEEP_ALL_MARKET_FIELDS=1 to write every field (old behaviour, ~6 GB for
# the full market history).
LEAN_MARKET_COLUMNS = [
    "id",
    "clobTokenIds",
    "condition_id",
    "question_id",
    "question",
    "market_slug",
    "closed",
    "active",
    "end_date_iso",
    "tags",
]
KEEP_ALL = os.environ.get("KEEP_ALL_MARKET_FIELDS", "").strip() in ("1", "true", "yes")

_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        _local.s = s
    return s


def _cursor(offset: int) -> str:
    return base64.b64encode(str(offset).encode()).decode()


def _token_ids(market: dict) -> list[str]:
    toks = market.get("tokens") or []
    return [str(t.get("token_id")) for t in toks if t.get("token_id") is not None]


def _flatten(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


def _row(market: dict, columns: list[str]) -> list:
    ids = _token_ids(market)
    out = []
    for c in columns:
        if c == "id":
            out.append(market.get("condition_id", ""))
        elif c == "clobTokenIds":
            out.append(json.dumps(ids) if ids else "")
        else:
            out.append(_flatten(market.get(c)))
    return out


def _fetch_page(offset: int):
    """Return (offset, markets). Empty list means at/past end of data."""
    s = _session()
    for attempt in range(MAX_RETRIES):
        try:
            r = s.get(CLOB_MARKETS, params={"next_cursor": _cursor(offset)}, timeout=30)
            if r.status_code == 200:
                return offset, r.json().get("data", [])
            if r.status_code in (429, 500, 502, 503):
                time.sleep(min(2**attempt, 10))
                continue
            r.raise_for_status()
        except requests.exceptions.RequestException:
            time.sleep(min(2**attempt, 10))
    raise RuntimeError(
        f"CLOB /markets failed at offset {offset} after {MAX_RETRIES} retries"
    )


def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_state(
    offset: int, fetched: int, columns: list[str] | None, completed: bool = False
):
    with open(STATE_FILE, "w") as f:
        json.dump(
            {
                "offset": offset,
                "fetched": fetched,
                "columns": columns,
                "completed": completed,
            },
            f,
        )


def _read_tail_ids(csv_file: str, n: int) -> set:
    """Return the `id` (first column) of up to the last `n` rows, reading only
    a bounded chunk from the end of the file — avoids scanning a multi-GB CSV
    just to seed the resume dedup set."""
    if not os.path.exists(csv_file):
        return set()
    with open(csv_file, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        chunk = min(size, 16 * 1024 * 1024)
        f.seek(size - chunk)
        data = f.read(chunk)
    lines = [ln for ln in data.split(b"\n") if ln.strip()]
    if chunk < size:
        lines = lines[1:]  # drop the (likely partial) first line
    ids = {ln.split(b",", 1)[0].decode("utf-8", "replace") for ln in lines[-n:]}
    ids.discard("id")  # header, if it landed in the window
    return ids


def update_markets(csv_filename: str = MARKETS_CSV, max_workers: int = WAVE) -> int:
    """Fetch all markets from CLOB into csv_filename. Resumable and concurrent."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)  # live progress when piped
    os.makedirs(os.path.dirname(csv_filename) or ".", exist_ok=True)

    state = _load_state()
    columns = state.get("columns")
    resuming = state.get("offset", 0) > 0 and os.path.exists(csv_filename)

    if resuming:
        fetched = state.get("fetched", 0)
        # Resume from the saved scan offset (the exact page boundary the
        # previous run reached). NOT from (fetched//PAGE)*PAGE: fetched counts
        # only *written* markets — empty / duplicate condition_ids are skipped
        # during a scan, so fetched < offset. Rewinding to a fetched-derived
        # page would re-read markets already appended to the CSV, and because
        # the tail-dedup window only covers the last few rows those get written
        # twice. offset is the precise "scanned up to here" marker, so new
        # markets (CLOB grows at the tail) are always past it.
        offset = state.get("offset", 0)
        seen = _read_tail_ids(csv_filename, PAGE + 500)
        print(f"  Resuming near offset {offset:,} ({fetched:,} markets already saved)")
        f = open(csv_filename, "a", newline="", encoding="utf-8")
        # If a previous run used a different column set than the current mode,
        # force a fresh start so the schema stays consistent.
        expected = None if KEEP_ALL else LEAN_MARKET_COLUMNS
        # Compare as sets: column ORDER in a previously saved file doesn't
        # matter (downstream reads by name), only the SET of columns does.
        # A strict list comparison would force a full rescan just because a
        # resume run once wrote columns in a different order.
        if columns is not None and expected is not None:
            schema_changed = set(columns) != set(expected)
        else:
            schema_changed = columns != expected
        if schema_changed:
            print(
                f"  ⚠ column schema changed (KEEP_ALL_MARKET_FIELDS={KEEP_ALL}); "
                "restarting from scratch"
            )
            f.close()
            f = open(csv_filename, "w", newline="", encoding="utf-8")
            seen = set()
            offset = 0
            fetched = 0
            columns = None
    else:
        seen = set()
        offset = 0
        fetched = 0
        columns = None
        f = open(csv_filename, "w", newline="", encoding="utf-8")

    writer = csv.writer(f)
    done = False
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            while not done:
                offsets = [offset + i * PAGE for i in range(max_workers)]
                for off, markets in ex.map(_fetch_page, offsets):
                    if not markets:
                        done = True
                        continue
                    for m in markets:
                        cid = str(m.get("condition_id", ""))
                        if not cid or cid in seen:
                            continue
                        seen.add(cid)
                        if columns is None:
                            columns = (
                                list(m.keys())
                                if KEEP_ALL
                                else [c for c in LEAN_MARKET_COLUMNS if c in m.keys()]
                            )
                            # Guarantee the required base columns even if a
                            # particular market dict omits them (e.g. first page
                            # sparse), so downstream reads never break.
                            base = ["id", "clobTokenIds", "market_slug", "condition_id"]
                            for c in base:
                                if c not in columns:
                                    columns.insert(0, c)
                            columns = list(dict.fromkeys(columns))  # dedupe, keep order
                            writer.writerow(columns)
                        writer.writerow(_row(m, columns))
                        fetched += 1
                offset += max_workers * PAGE
                f.flush()
                _save_state(offset, fetched, columns)
                print(
                    f"  fetched {fetched:,} markets (scanned through offset {offset:,})"
                )
    finally:
        f.close()

    _save_state(offset, fetched, columns, completed=True)
    print(f"Total markets: {fetched:,}  ->  {csv_filename}")
    return fetched


if __name__ == "__main__":
    update_markets()
    # After a refresh, optionally pull settlement info for any NEW updown
    # markets that appeared since the last run — so the pipeline stays
    # self-sufficient and no manual backfill step is needed for fresh data.
    # Enable with AUTO_SETTLE=1 (or 'auto-settle' as the first CLI arg).
    auto_settle = "auto-settle" in sys.argv[1:] or os.environ.get(
        "AUTO_SETTLE", ""
    ).strip() in ("1", "true", "yes")
    if auto_settle:
        from update_utils import update_settlements  # local import to avoid cycle

        print("\n== Auto-settling new markets ==")
        new_rows, new_absent = update_settlements.update_settlements()
        print(f"Auto-settle done: +{new_rows} rows, +{new_absent} absent")
