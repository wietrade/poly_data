"""Fetch settlement info (outcomePrices + outcomes order) for target markets.

Rationale
---------
process_live / downstream maker-taker analysis need to know, per market, which
outcome token ultimately won (and how token1/token2 map to Up/Down / YES/NO).
The lean `data/markets.csv` deliberately drops `outcomePrices`/`outcomes`, so we
re-fetch just those fields for a *targeted* universe (default: crypto updown
5m/15m) instead of re-pulling the whole 295M-market history.

Data source
-----------
Gamma `/markets?clob_token_ids=...&closed=true|false` — batch 40 token_ids per
request (the same endpoint update_missing_tokens() already uses; a batch round
trip returns `outcomes=["Up","Down"]`, `outcomePrices=["0","1"]`, `clobTokenIds`
and `closed`, so winner + token↔outcome mapping come back in one call).

Verified 2026-09-03: a settled btc-updown-5m from 2026-02-12 resolves via
`closed=true` with outcomePrices=["0","1"], outcomes=["Up","Down"].

Output
------
`data/settlements.parquet` — one row per market condition_id:
    condition_id, clob_token_ids (JSON), outcomes (JSON), outcome_prices (JSON),
    closed, slug, fetched_at
Rows for condition_ids that Gamma no longer returns are NOT written, so absence
in the file means "unknown settlement" (log them to a missing list).

Resumable: `data/settlements_state.json` stores the list of already-fetched
condition_ids (dedup set) so an interrupted run picks up where it left off.

Usage
-----
    # default universe: crypto updown 5m/15m (btc/eth/sol/xrp/...)
    python -m update_utils.update_settlements
    # custom slug universe
    SETTLE_SLUG_RE="^btc-updown-(5m|15m)-[0-9]+$" python -m update_utils.update_settlements
"""

import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import polars as pl
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from poly_utils.utils import _token_exprs  # reuse vectorized clobTokenIds split

GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"

# Default universe: crypto up/down 5m + 15m markets (any coin).
DEFAULT_SLUG_RE = r"^[a-z]+-updown-(5m|15m)-[0-9]+$"
SLUG_RE = os.environ.get("SETTLE_SLUG_RE", "").strip() or DEFAULT_SLUG_RE

MARKETS_CSV = "data/markets.csv"
MISSING_MARKETS_CSV = "data/missing_markets.csv"
OUT_PARQUET = "data/settlements.parquet"
STATE_FILE = "data/settlements_state.json"

_BATCH = 40  # token_ids per Gamma request (URL-safe, same as update_missing_tokens)
_WORKERS = 12  # concurrent batches
_MAX_RETRIES = 6

_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        _local.s = s
    return s


def _load_targets() -> pl.DataFrame:
    """Load target markets (condition_id, market_slug, token1, token2) for the
    slug universe, from markets.csv + missing_markets.csv."""
    frames = []
    for fname, slug_col in (
        (MARKETS_CSV, "market_slug"),
        (MISSING_MARKETS_CSV, "slug"),
    ):
        if not os.path.exists(fname) or os.path.getsize(fname) == 0:
            continue
        df = pl.read_csv(
            fname,
            columns=["id", "clobTokenIds", slug_col],
            schema_overrides={"id": pl.Utf8, "clobTokenIds": pl.Utf8},
            ignore_errors=True,
        )
        frames.append(df)
    if not frames:
        raise FileNotFoundError("markets.csv not found — run update_markets() first")
    df = pl.concat(frames, how="diagonal_relaxed").unique(subset=["id"], keep="first")
    # Normalize slug column to market_slug.
    if "slug" in df.columns:
        df = df.with_columns(
            pl.coalesce(["market_slug", "slug"]).alias("market_slug")
        ).drop("slug")
    df = df.filter(pl.col("market_slug").str.contains(SLUG_RE, literal=False))
    df = df.with_columns(_token_exprs()).drop("clobTokenIds")
    # A market whose clobTokenIds were empty when markets.csv was written can't
    # be queried yet (no tokens). Keep it as a target but mark it so callers can
    # skip it this run; once update_markets refreshes it with real token ids it
    # will become queryable. We must NOT treat it as "absent" (that would
    # permanently skip it). The empty case surfaces as token1=None.
    return df


def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"fetched": [], "absent": []}


def _save_state(fetched: list, absent: list) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump({"fetched": fetched, "absent": absent}, f)


def _fetch_batch(token_ids: list) -> list:
    """Fetch markets for a batch of token ids, closed first then active."""
    s = _session()
    for attempt in range(_MAX_RETRIES):
        try:
            params = [("clob_token_ids", t) for t in token_ids]
            # closed=true first: settled markets are the ones with real prices.
            for closed_flag in ("true", "false"):
                resp = s.get(
                    GAMMA_MARKETS,
                    params=params + [("closed", closed_flag), ("limit", _BATCH)],
                    timeout=25,
                )
                if resp.status_code == 200:
                    payload = resp.json()
                    markets = (
                        payload
                        if isinstance(payload, list)
                        else payload.get("markets") or payload.get("data") or []
                    )
                    if markets:
                        return markets
                elif resp.status_code in (429, 500, 502, 503):
                    time.sleep(min(2**attempt, 10))
                    break  # retry whole batch
            return []
        except requests.exceptions.RequestException:
            time.sleep(min(2**attempt, 10))
    return []


def _cid(m: dict) -> str:
    return str(m.get("conditionId") or m.get("id") or "")


def _to_row(m: dict, slug: str) -> dict:
    return {
        "condition_id": _cid(m),
        "slug": slug,
        "clob_token_ids": json.dumps(m.get("clobTokenIds") or []),
        "outcomes": json.dumps(m.get("outcomes") or []),
        "outcome_prices": json.dumps(m.get("outcomePrices") or []),
        "closed": bool(m.get("closed")),
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def update_settlements() -> tuple[int, int]:
    """Fetch settlement info for target markets not yet recorded.

    Returns (n_new_rows, n_new_absent): rows appended this run and condition_ids
    newly recorded as absent (not returned by Gamma — e.g. too-old markets).

    Incremental: markets whose condition_id is already in the state file (either
    fetched with a settlement row, or previously found absent) are skipped, so
    repeated runs only query genuinely new targets. This lets update_markets()
    call us after every refresh without re-pulling the whole universe.
    """
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    targets = _load_targets()
    print(f"Target universe: {targets.height:,} markets (slug regex: {SLUG_RE!r})")
    if targets.height == 0:
        print("  no target markets")
        return 0, 0

    # Build token->slug lookup for relabelling fetched markets.
    tok_to_slug: dict[str, str] = {}
    tok_to_cid: dict[str, str] = {}
    for r in targets.select(["id", "token1", "token2", "market_slug"]).iter_rows():
        cid, t1, t2 = r[0], r[1], r[2]
        for tok in (t1, t2):
            if tok:
                tok_to_slug[tok] = r[3]
                tok_to_cid[tok] = cid

    state = _load_state()
    done: set = set(state.get("fetched", []))
    absent: set = set(state.get("absent", []))

    # Only query markets we've never resolved (neither fetched nor absent).
    known = done | absent
    fresh = targets.filter(~pl.col("id").is_in(list(known)))
    print(
        f"  {fresh.height:,} new targets (already known: {targets.height - fresh.height:,})"
    )
    if fresh.height == 0:
        print("  nothing new to fetch")
        return 0, 0

    key_tokens = [r for r in fresh.select("token1").to_series().to_list() if r]
    batches = [key_tokens[i : i + _BATCH] for i in range(0, len(key_tokens), _BATCH)]
    print(f"  {len(batches):,} batches of {_BATCH} token_ids")

    rows: list = []
    newly_absent: set = set()
    with ThreadPoolExecutor(max_workers=_WORKERS) as ex:
        # (future -> batch_tokens) so we can mark "asked but not returned" as absent.
        submitted: dict = {}
        for b in batches:
            submitted[ex.submit(_fetch_batch, b)] = b
        n_done_batches = 0
        for fut in submitted:
            markets = fut.result()
            batch_tokens = submitted[fut]
            n_done_batches += 1
            for m in markets:
                c = _cid(m)
                if not c or c in done:
                    continue
                done.add(c)
                slug = tok_to_slug.get(
                    str(m.get("clobTokenIds") or ["", ""])[0].strip('"[] '), ""
                )
                if not slug:
                    # Try to recover slug from the returned market itself.
                    slug = m.get("slug") or m.get("ticker") or ""
                rows.append(_to_row(m, slug))
                if rows and len(rows) % 5000 == 0:
                    _save_state(list(done), list(absent))
                    print(f"    {len(rows):,} rows fetched so far")
            # Tokens asked in this batch that Gamma did not return -> absent
            # (avoids re-querying dead / too-old markets on every run).
            returned_tokens = {
                str(tok) for m in markets for tok in (m.get("clobTokenIds") or [])
            }
            for tok in batch_tokens:
                if tok in returned_tokens:
                    continue
                c = tok_to_cid.get(tok)
                if c and c not in known:
                    absent.add(c)
                    newly_absent.add(c)
            if n_done_batches % 50 == 0:
                _save_state(list(done), list(absent))
                print(f"    {n_done_batches}/{len(batches)} batches done")

    if not rows and not newly_absent:
        print("  no new settlement rows")
        _save_state(list(done), list(absent))
        return 0, len(newly_absent)

    if rows:
        # Append to the parquet output.
        new_df = pl.DataFrame(rows)
        out_exists = os.path.exists(OUT_PARQUET)
        if out_exists:
            old = pl.read_parquet(OUT_PARQUET)
            new_df = pl.concat([old, new_df], how="diagonal_relaxed").unique(
                subset=["condition_id"], keep="last"
            )
        os.makedirs(os.path.dirname(OUT_PARQUET) or ".", exist_ok=True)
        new_df.write_parquet(OUT_PARQUET)
    _save_state(list(done), list(absent))
    print(
        f"Total settlement rows: {len(done):,} fetched (+{len(rows):,} this run); "
        f"absent: {len(absent):,}"
    )
    return len(rows), len(newly_absent)


if __name__ == "__main__":
    update_settlements()
