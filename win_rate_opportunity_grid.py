"""Parameter grid for the +EV "buy-the-leader" cell (market-level, first-hit per market).

For each (leader-price band, gap threshold, earliest entry minute) combo:
  rule = buy the LEADING side at the FIRST qualifying 5s bucket of each market
         (qualify: minute>=min_minute, gap>=gap_min, fav_px in band), hold to settlement.
Outputs n trades, trades/day, win%, avg entry px, net ROI/trade (fee+slippage), and a
daily-throughput proxy (n/day x ROI) to balance opportunity count vs edge.

One full scan builds per-(market, 5s-bucket) leader state; combos are cheap passes after.

Usage:
  python win_rate_opportunity_grid.py [--universe ...] [--parts N] [--fee 0.010] [--slip 0.005]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import time

import polars as pl

TRADES_DIR = "processed/trades"
SETTLE = "data/settlements.parquet"
DEFAULT_UNIVERSE = r"^(btc)-updown-15m-[0-9]+$"
NEED_COLS = ["market_id", "timestamp", "nonusdc_side", "price"]
TRADES_RE = re.compile(r"^trades_part_(\d+)\.parquet$")
WIN_SECS = 900


def _dload(s):
    try:
        return json.loads(json.loads(s))
    except Exception:
        try:
            return json.loads(s)
        except Exception:
            return []


def _slug_start(slug: str) -> int | None:
    m = re.search(r"-(\d+)$", slug)
    return int(m.group(1)) if m else None


def load_settle(universe: str) -> pl.DataFrame:
    st = pl.read_parquet(SETTLE)
    st = st.filter(pl.col("closed"))
    st = st.with_columns(
        pl.col("outcomes")
        .map_elements(_dload, return_dtype=pl.List(pl.Utf8))
        .alias("ou"),
        pl.col("outcome_prices")
        .map_elements(_dload, return_dtype=pl.List(pl.Utf8))
        .alias("pr"),
    )
    st = st.with_columns(
        [
            pl.col("ou").list.get(0).alias("o1"),
            pl.col("pr").list.get(0).alias("p1"),
            pl.col("slug")
            .map_elements(_slug_start, return_dtype=pl.Int64)
            .alias("start_epoch"),
        ]
    )
    st = st.filter(pl.col("slug").str.contains(universe, literal=False))
    st = st.filter(pl.col("start_epoch").is_not_null())
    st = st.with_columns(
        pl.when(pl.col("p1") == "1")
        .then(pl.col("o1"))
        .otherwise(pl.lit("Down"))
        .alias("winner")
    )
    return st.select(["condition_id", "winner", "start_epoch"])


def _trades_parts():
    out = []
    for p in glob.glob(os.path.join(TRADES_DIR, "trades_part_*.parquet")):
        m = TRADES_RE.search(os.path.basename(p))
        if m:
            out.append((int(m.group(1)), p))
    return sorted(out)


def run(universe, parts_limit, fee, slip, win):
    t0 = time.time()
    min_p = max(0, win // 60 - 1)  # last minute index for this window
    settle = load_settle(universe)
    print(f"settled markets: {settle.height:,}")
    settle_df = pl.DataFrame(
        {
            "market_id": settle["condition_id"].to_list(),
            "winner": settle["winner"].to_list(),
            "start_epoch": settle["start_epoch"].to_list(),
        }
    )
    parts = _trades_parts()
    if parts_limit:
        parts = parts[:parts_limit]
    print(f"parts: {len(parts)} (smoke={bool(parts_limit)})")

    bucket_parts = []
    total = kept = 0
    for idx, path in parts:
        df = pl.read_parquet(path, columns=NEED_COLS)
        total += df.height
        df = df.filter(
            pl.col("price").is_finite() & (pl.col("price") > 0) & (pl.col("price") < 1)
        )
        df = df.join(settle_df, on="market_id", how="inner")
        if df.height == 0:
            continue
        df = df.with_columns(
            [
                (pl.col("timestamp").dt.epoch("s") - pl.col("start_epoch")).alias(
                    "elapsed"
                ),
                pl.when(pl.col("nonusdc_side") == "token1")
                .then(pl.lit("Up"))
                .otherwise(pl.lit("Down"))
                .alias("side"),
            ]
        )
        df = df.filter((pl.col("elapsed") >= 0) & (pl.col("elapsed") <= win))
        if df.height == 0:
            continue
        kept += df.height
        df = df.with_columns((pl.col("elapsed") // 5).cast(pl.Int32).alias("bucket5"))
        lp = df.sort(["bucket5", "side", "timestamp"])
        lp = lp.group_by(["market_id", "bucket5", "side"]).agg(
            pl.col("price").last().alias("px")
        )
        up = lp.filter(pl.col("side") == "Up").drop("side").rename({"px": "up_px"})
        dn = lp.filter(pl.col("side") == "Down").drop("side").rename({"px": "dn_px"})
        sides = up.join(dn, on=["market_id", "bucket5"], how="inner")
        if sides.height:
            bucket_parts.append(sides)
        if (idx + 1) % 200 == 0:
            print(
                f"  part {idx + 1}/{len(parts)} kept {kept:,} ({time.time() - t0:.0f}s)",
                flush=True,
            )

    if not bucket_parts:
        print("no bucket rows")
        return
    B = pl.concat(bucket_parts).with_columns(
        [
            (pl.col("up_px") - pl.col("dn_px")).abs().alias("gap"),
            pl.when(pl.col("up_px") >= pl.col("dn_px"))
            .then(pl.lit("Up"))
            .otherwise(pl.lit("Down"))
            .alias("fav_side"),
            pl.max_horizontal("up_px", "dn_px").alias("fav_px"),
            (pl.col("bucket5") // 12).cast(pl.Int32).clip(0, min_p).alias("minute"),
        ]
    )
    # dedupe (cross-part repeats are impossible: market window lives in one part roughly,
    # but guard anyway)
    B = (
        B.sort(["market_id", "bucket5"])
        .group_by(["market_id", "bucket5"], maintain_order=True)
        .first()
    )
    B = B.join(settle_df, on="market_id", how="inner")
    B = B.with_columns((pl.col("start_epoch") // 86400).alias("day"))
    B = B.drop(["up_px", "dn_px", "start_epoch"])
    print(
        f"bucket rows: {B.height:,} ; markets: {B['market_id'].n_unique():,} ; days: {B['day'].n_unique()} ({time.time() - t0:.0f}s)"
    )
    # memory pressure: free
    del bucket_parts

    cost_mult = 1 + fee + slip
    bands = {".60-.70": (0.60, 0.70), ".65-.70": (0.65, 0.70)}
    gaps = [0.30, 0.35, 0.40, 0.50]
    mins = [0, 1, 2, 3] if win <= 300 else [0, 6, 8, 10]
    n_days = B["day"].n_unique()

    rows = []
    for bname, (blo, bhi) in bands.items():
        for gap_min in gaps:
            for mm in mins:
                q = B.filter(
                    (pl.col("minute") >= mm)
                    & (pl.col("gap") >= gap_min)
                    & (pl.col("fav_px") >= blo)
                    & (pl.col("fav_px") < bhi)
                )
                if q.height == 0:
                    continue
                h = (
                    q.sort(["bucket5"])
                    .group_by("market_id", maintain_order=True)
                    .first()
                )
                n = h.height
                w = int(h["winner"].eq(h["fav_side"]).sum())
                wr = w / n
                px = float(h["fav_px"].mean())
                sub = h.with_columns(
                    pl.when(h["winner"].eq(h["fav_side"]))
                    .then((1.0 / (px * cost_mult)) - 1.0)
                    .otherwise(pl.lit(-1.0))
                    .alias("ret")
                )
                roi = float(sub["ret"].mean())
                rows.append(
                    {
                        "band": bname,
                        "gap_min": gap_min,
                        "min_min": mm,
                        "n": n,
                        "per_day": n / n_days,
                        "win%": wr * 100,
                        "avg_px": px,
                        "roi%": roi * 100,
                        "daily_proxy": n / n_days * roi * 100,
                    }
                )

    out = pl.DataFrame(rows).sort(["roi%"], descending=True)
    print("\n" + "=" * 118)
    print(
        "GRID: buy leader at first qualifying bucket (market-level, one entry/market)"
    )
    print(
        "  roi% = net ROI/trade (fee+slippage buffered);  daily_proxy = trades/day x roi%"
    )
    print("=" * 118)
    print(
        f"{'band':<9}{'gap>=':<7}{'min_min':<9}{'n':>8}{'n/day':>8}{'win%':>7}{'avg_px':>8}{'roi%':>8}{'daily_proxy':>11}"
    )
    for r in out.iter_rows(named=True):
        print(
            f"{r['band']:<9}{r['gap_min']:<7.2f}{r['min_min']:<9}{r['n']:>8,}{r['per_day']:>8.1f}"
            f"{r['win%']:>7.1f}{r['avg_px']:>8.3f}{r['roi%']:>+8.2f}{r['daily_proxy']:>+10.2f}"
        )

    os.makedirs("processed", exist_ok=True)
    coin = re.sub(r"[^a-zA-Z0-9]+", "_", universe).strip("_")[:24]
    p = f"processed/win_rate_grid_{coin}_15m.csv"
    out.write_csv(p)
    print(f"[OK] wrote {p}  rows={out.height} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--universe", default=os.environ.get("MATRIX_UNIVERSE", DEFAULT_UNIVERSE)
    )
    ap.add_argument("--parts", type=int, default=None)
    ap.add_argument("--fee", type=float, default=0.010)
    ap.add_argument("--slip", type=float, default=0.005)
    ap.add_argument("--win", type=int, default=900)
    a = ap.parse_args()
    run(a.universe, a.parts, a.fee, a.slip, a.win)
