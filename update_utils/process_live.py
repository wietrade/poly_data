"""
Join raw order-fill events with market metadata to produce labeled trades.

Input:  data/order_filled/part_*.parquet  (written by update_chain.py)
Output: processed/trades/trades_part_*.parquet

Each order part is an atomic unit: it is either fully processed (a matching
trades part exists) or not. Resume therefore means "start from the first order
part that has no corresponding trades part" — no CSV tail-marker parsing, and a
crash mid-part can never duplicate or lose rows.
"""

import glob
import os
import re
import sys

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from poly_utils.utils import get_lean_markets, update_missing_tokens

from update_utils.update_chain import PARTS_DIR  # data/order_filled

TRADES_DIR = "processed/trades"

# Chunk size (in rows) for processing one order part into one trades part.
# Default: process a whole order part in one pass. Not usually needed.
CHUNK_SIZE = int(os.environ.get("PROCESS_CHUNK_SIZE", "0"))

# Optional universe filter: a regex matched against each market's slug. When
# set, only trades belonging to matching markets are written to processed/trades
# (everything else is dropped). The raw order parts in data/order_filled are
# never touched, so you can re-process with a different filter later without
# re-scraping the chain.
#
# Example (crypto 5m/15m updown only):
#   MARKET_SLUG_RE="^(btc|eth|sol)-updown-(5m|15m)-[0-9]+$"
# Empty / unset = keep all markets (default, backward compatible).
MARKET_SLUG_RE = os.environ.get("MARKET_SLUG_RE", "").strip()

# The CTF Exchange V2 contract itself. In every `matchOrders` fill the taker
# order's OrderFilled event hardcodes `taker: address(this)` — the exchange
# custodies the assets (taker → exchange → maker), so it shows up as the taker
# of that leg. Those rows are the redundant "taker side" of the same match; the
# real user↔user fill is already fully recorded in the maker-side leg, so rows
# where either party is the exchange are dropped in _processed_df to avoid
# double-counting every trade.
PROTOCOL_ADDRESS = "0xe111180000d2663c0091e4f400237545b87b996b"

_ORDER_PART_RE = re.compile(r"^part_(\d+)\.parquet$")
_TRADES_PART_RE = re.compile(r"^trades_part_(\d+)\.parquet$")


def _universe_tokens(markets_df: pl.DataFrame) -> set[str] | None:
    """Set of outcome-token ids belonging to the filtered universe.

    Returns None when no slug filter is active (keep everything). When active,
    returns the union of token1/token2 across the filtered markets so order
    rows can be pre-filtered before the join — massively shrinking each part
    when the universe is a small slice (e.g. crypto updown).
    """
    if not MARKET_SLUG_RE:
        return None
    toks = set()
    for col in ("token1", "token2"):
        if col in markets_df.columns:
            toks.update(markets_df[col].drop_nulls().to_list())
    return toks


def _order_parts() -> list[tuple[int, str]]:
    """Sorted list of (part_index, path) for every order part file."""
    out = []
    for p in glob.glob(os.path.join(PARTS_DIR, "part_*.parquet")):
        m = _ORDER_PART_RE.search(os.path.basename(p))
        if m:
            out.append((int(m.group(1)), p))
    return sorted(out)


def _processed_parts() -> set[int]:
    """Indexes of order parts already processed (trades part exists)."""
    done = set()
    for p in glob.glob(os.path.join(TRADES_DIR, "trades_part_*.parquet")):
        m = _TRADES_PART_RE.search(os.path.basename(p))
        if m:
            done.add(int(m.group(1)))
    return done


def _trades_part_path(part_index: int) -> str:
    return os.path.join(TRADES_DIR, f"trades_part_{part_index:08d}.parquet")


def _processed_df(
    df: pl.DataFrame, markets_df: pl.DataFrame, universe_tokens: set[str] | None = None
) -> pl.DataFrame:
    # Drop the exchange-as-counterparty legs before anything else: in every
    # matchOrders fill the taker order's OrderFilled event hardcodes
    # `taker: address(this)` (the exchange custodies assets taker → exchange →
    # maker). Those rows are the redundant "taker side" ledger leg of the same
    # match — the real user↔user fill is already fully recorded in the
    # maker-side leg — so keeping them would double-count every trade.
    # See PROTOCOL_ADDRESS above.
    df = df.filter(
        (pl.col("maker") != PROTOCOL_ADDRESS) & (pl.col("taker") != PROTOCOL_ADDRESS)
    )

    markets_df = markets_df.rename({"id": "market_id"})

    markets_long = markets_df.select(["market_id", "token1", "token2"]).unpivot(
        index="market_id",
        on=["token1", "token2"],
        variable_name="side",
        value_name="asset_id",
    )

    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # Optional universe filter: drop any order whose non-USDC token is not one
    # of the target markets' tokens BEFORE the join. Keeps downstream small and
    # the join cheap. None = keep everything (backward compatible).
    if universe_tokens is not None:
        df = df.filter(pl.col("nonusdc_asset_id").is_in(list(universe_tokens)))

    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    df = df.with_columns(
        [
            pl.when(pl.col("makerAssetId") == "0")
            .then(pl.lit("USDC"))
            .otherwise(pl.col("side"))
            .alias("makerAsset"),
            pl.when(pl.col("takerAssetId") == "0")
            .then(pl.lit("USDC"))
            .otherwise(pl.col("side"))
            .alias("takerAsset"),
            pl.col("market_id"),
        ]
    )

    df = df[
        [
            "timestamp",
            "market_id",
            "maker",
            "makerAsset",
            "makerAssetId",
            "makerAmountFilled",
            "taker",
            "takerAsset",
            "takerAmountFilled",
            "transactionHash",
            "fee",
        ]
    ]

    # USDC has 6 decimals. Outcome tokens also have 6 (CTF wraps to 6 for parity).
    df = df.with_columns(
        [
            (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
            (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
            (pl.col("fee") / 10**6).alias("fee"),
        ]
    )

    df = df.with_columns(
        [
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.lit("BUY"))
            .otherwise(pl.lit("SELL"))
            .alias("taker_direction"),
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("BUY"))
            .alias("maker_direction"),
        ]
    )

    df = df.with_columns(
        [
            # Derive from the raw assetId (never null) so unknown markets stay null
            # instead of leaking the literal "USDC" through polars three-valued logic.
            pl.when(pl.col("makerAssetId") == "0")
            .then(pl.col("takerAsset"))
            .otherwise(pl.col("makerAsset"))
            .alias("nonusdc_side"),
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.col("takerAmountFilled"))
            .otherwise(pl.col("makerAmountFilled"))
            .alias("usd_amount"),
            pl.when(pl.col("takerAsset") != "USDC")
            .then(pl.col("takerAmountFilled"))
            .otherwise(pl.col("makerAmountFilled"))
            .alias("token_amount"),
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
            .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
            .cast(pl.Float64)
            .alias("price"),
        ]
    )

    return df[
        [
            "timestamp",
            "market_id",
            "maker",
            "taker",
            "nonusdc_side",
            "maker_direction",
            "taker_direction",
            "price",
            "usd_amount",
            "token_amount",
            "fee",
            "transactionHash",
        ]
    ]


def _read_last_line(path: str) -> str:
    """Return the last non-empty line of a (possibly huge) text file without
    loading it — seek backwards from EOF in chunks. Cross-platform replacement
    for shelling out to `tail` (which doesn't exist on Windows)."""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        if pos == 0:
            return ""
        buf = b""
        while pos > 0:
            step = min(4096, pos)
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
            stripped = buf.rstrip(b"\r\n")
            nl = stripped.rfind(b"\n")
            if nl != -1:
                return stripped[nl + 1 :].decode("utf-8", errors="replace")
        return buf.rstrip(b"\r\n").decode("utf-8", errors="replace")


def _discover_missing_tokens(markets_df: pl.DataFrame) -> None:
    """Scan the order parquet parts for asset IDs not present in markets and
    fetch them. Uses a lazy polars scan (two columns only) across all parts so
    finding the distinct traded tokens across hundreds of millions of rows
    takes seconds, not a Python row loop.
    """
    paths = [p for _, p in _order_parts()]
    if not paths:
        return
    lf = pl.scan_parquet(paths)
    maker = (
        lf.select("makerAssetId")
        .filter(pl.col("makerAssetId") != "0")
        .unique()
        .collect()
    )
    taker = (
        lf.select("takerAssetId")
        .filter(pl.col("takerAssetId") != "0")
        .unique()
        .collect()
    )
    trade_asset_ids = set(maker["makerAssetId"].to_list()) | set(
        taker["takerAssetId"].to_list()
    )

    existing = set()
    for col in ("token1", "token2"):
        if col in markets_df.columns:
            existing.update(markets_df[col].drop_nulls().to_list())

    missing = sorted(trade_asset_ids - existing)
    if missing:
        print(
            f"🔍 {len(missing)} tokens not in markets.csv — fetching (batched) from Gamma"
        )
        update_missing_tokens(missing)
    else:
        print("✅ All markets present")


def process_live() -> None:
    # UTF-8: Windows 默认 GBK 无法编码 emoji/箭头(🔄✅→), 重定向到文件会崩
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(
            encoding="utf-8", errors="replace", line_buffering=True
        )  # live progress when piped
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    print("=" * 60)
    print("🔄 Processing trades (Parquet parts)")
    print("=" * 60)

    order_parts = _order_parts()
    if not order_parts:
        print(
            f"⚠ No {os.path.join(PARTS_DIR, 'part_*.parquet')} found — run update_chain() first"
        )
        return

    universe_tokens = None
    if MARKET_SLUG_RE:
        print(f"🎯 Universe filter: MARKET_SLUG_RE={MARKET_SLUG_RE}")
        # markets.csv is the full market list; filter it down to the universe.
        # _discover_missing_tokens is skipped in filter mode: scanning order
        # parts would flag every out-of-universe token as "missing" and trigger
        # pointless Gamma backfills. Universe markets come from the full
        # markets.csv, which is fetched to completion by update_markets first.
        markets_df = get_lean_markets(MARKET_SLUG_RE)
        universe_tokens = _universe_tokens(markets_df)
        print(
            f"  → {markets_df.height:,} matching markets, {len(universe_tokens):,} tokens"
        )
        if markets_df.height == 0 or not universe_tokens:
            print("⚠ Filter matched no markets — nothing to process.")
            return
    else:
        # Streaming discovery pass (lazy scan over all parts). Run first so
        # update_missing_tokens populates missing_markets.csv before we load
        # markets.
        markets_df = get_lean_markets()
        _discover_missing_tokens(markets_df)
        markets_df = get_lean_markets()  # reload if backfilled

    done = _processed_parts()
    pending = [(i, p) for i, p in order_parts if i not in done]
    if done:
        print(
            f"📍 {len(done)} parts already processed — resuming from part {pending[0][0] if pending else 'none'}"
        )
    else:
        print("⚠ No trades parts yet — processing from beginning")

    os.makedirs(TRADES_DIR, exist_ok=True)

    total_written = 0
    for part_index, path in pending:
        df = pl.read_parquet(path)
        # update_chain stores unix seconds; trades parts keep real datetimes.
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp")
        )
        trades_chunk = _processed_df(df, markets_df, universe_tokens)
        # A filtered part may legitimately contain zero universe trades; skip
        # writing empty parts so resume bookkeeping stays simple.
        if trades_chunk.height == 0:
            print(f"  · part {part_index:,} → 0 universe trades (skipped)")
            continue
        out = _trades_part_path(part_index)
        tmp = out + ".tmp"
        trades_chunk.write_parquet(tmp, compression="zstd")
        os.replace(tmp, out)
        total_written += len(trades_chunk)
        print(
            f"  ✓ part {part_index:,} → {out}  (+{len(trades_chunk):,} rows  total: {total_written:,})"
        )

    print(
        f"✓ Done. Processed {len(pending):,} parts, wrote {total_written:,} rows → {TRADES_DIR}/"
    )

    print("=" * 60)
    print("✅ Done")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
