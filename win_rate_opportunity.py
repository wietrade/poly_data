"""Opportunity count + simple market-level backtest for the +EV cell.

Cell found by win_rate_matrix_cond.py: buy the LEADING side (gap>=0.30) when its
price is in 0.60-0.69. This script counts how often that cell occurs and what a
simple "buy the first qualifying moment of each market, hold to settlement" rule
would make.

Rule variants:
  ANY  : first moment in the whole 15m window where leader price in [0.60,0.70) & gap>=0.30
  LATE : same but entry must be at minute >= 10 (only markets that hit the cell late)
Costs: taker fee ~ 1.0% of notional + 0.5% slippage buffered (configurable), applied
on the buy price (entry_price*(1+fee+slippage)). Payout 1.0/share at settlement.

Market-level (one entry per market, first qualifying bucket) -> no fill-level bias.
Usage: python win_rate_opportunity.py [--universe ...] [--parts N] [--fee 0.010] [--slip 0.005]
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
PX_LO, PX_HI = 0.60, 0.70  # leader price cell
GAP_MIN = 0.30


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


def run(universe, parts_limit, fee, slip):
    t0 = time.time()
    settle = load_settle(universe)
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
    print(f"settled markets: {settle.height:,} ; parts: {len(parts)}")

    hit_rows = []  # (market_id, first_bucket5, fav_px, fav_side)
    market_count = 0
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
        df = df.filter((pl.col("elapsed") >= 0) & (pl.col("elapsed") <= WIN_SECS))
        if df.height == 0:
            continue
        kept += df.height
        df = df.with_columns((pl.col("elapsed") // 5).cast(pl.Int32).alias("bucket5"))
        # both-side last price per (market, bucket5)
        lp = df.sort(["bucket5", "side", "timestamp"])
        lp = lp.group_by(["market_id", "bucket5", "side"]).agg(
            pl.col("price").last().alias("px")
        )
        up = lp.filter(pl.col("side") == "Up").drop("side").rename({"px": "up_px"})
        dn = lp.filter(pl.col("side") == "Down").drop("side").rename({"px": "dn_px"})
        sides = up.join(
            dn, on=["market_id", "bucket5"], how="inner"
        )  # both sides present
        if sides.height == 0:
            continue
        sides = sides.with_columns(
            [
                (pl.col("up_px") - pl.col("dn_px")).abs().alias("gap"),
                pl.when(pl.col("up_px") >= pl.col("dn_px"))
                .then(pl.lit("Up"))
                .otherwise(pl.lit("Down"))
                .alias("fav_side"),
                pl.max_horizontal("up_px", "dn_px").alias("fav_px"),
            ]
        )
        market_count += sides["market_id"].n_unique()
        hit = sides.filter(
            (pl.col("gap") >= GAP_MIN)
            & (pl.col("fav_px") >= PX_LO)
            & (pl.col("fav_px") < PX_HI)
        )
        if hit.height == 0:
            continue
        hit = hit.sort("bucket5").group_by("market_id", maintain_order=True).first()
        hit_rows.append(hit.select(["market_id", "bucket5", "fav_px", "fav_side"]))
        if (idx + 1) % 200 == 0:
            print(
                f"  part {idx + 1}/{len(parts)} kept {kept:,} ({time.time() - t0:.0f}s)",
                flush=True,
            )

    if not hit_rows:
        print("no hits found")
        return
    allhits = pl.concat(hit_rows)
    allhits = (
        allhits.sort(["bucket5"]).group_by("market_id", maintain_order=True).first()
    )
    # market first-hit across parts (already min bucket per part; take global min)
    allhits = allhits.sort("bucket5").group_by("market_id", maintain_order=True).first()
    h = allhits.join(settle_df, on="market_id", how="inner")
    h = h.with_columns(
        [
            (pl.col("start_epoch") // 86400).alias("day"),
            pl.col("bucket5")
            .map_elements(lambda b: min(b // 12, 14), return_dtype=pl.Int32)
            .alias("minute"),
            (pl.col("winner") == pl.col("fav_side")).alias("won"),
        ]
    )
    n_mk = h.height
    n_day = h["day"].n_unique()
    print(
        f"\n[OK] scanned {total:,} fills kept {kept:,} ; bucket rows {market_count:,} ({time.time() - t0:.0f}s)"
    )
    print(f"markets with both-side buckets: {market_count:,}")

    def report(tag, sub):
        if sub.height == 0:
            print(f"  {tag}: no trades")
            return
        n = sub.height
        w = int(sub["won"].sum())
        wr = w / n
        px = sub["fav_px"].mean()
        # per-trade ROI on $1 of cost basis (buy shares = 1/px_eff)
        cost_mult = 1 + fee + slip
        px_eff = px * cost_mult
        sub2 = sub.with_columns(
            pl.when(sub["won"])
            .then((1.0 / px_eff) - 1.0)
            .otherwise(pl.lit(-1.0))
            .alias("ret")
        )
        mean_ret = float(sub2["ret"].mean())
        days = sub["day"].n_unique()
        print(
            f"  [{tag}] trades={n:,}  win={wr * 100:.1f}%  avg_entry_px={px:.3f}  "
            f"cost_mult={cost_mult:.3f}  ROI/trade={mean_ret * 100:+.2f}%  "
            f"days={days}  trades/day={n / days:.1f}"
        )
        # per entry-minute distribution
        g = sub.group_by("minute").agg(
            pl.len().alias("n"), pl.col("won").cast(pl.Int32).sum().alias("w")
        )
        g = g.sort("minute")
        print(
            "     entry-minute: "
            + " ".join(f"m{m}:{int(a['n'][0]) if False else 0}" for m in range(0))
            + "".join(
                f"[m{int(r[0]):>2} n={int(r[1]):,} w={100 * int(r[2]) / int(r[1]):.0f}%]"
                for r in g.iter_rows()
            )
        )

    print("\n" + "=" * 100)
    print(
        "MARKET-LEVEL backtest: buy leader @ first qualifying moment (px in [0.60,0.70) & gap>=0.30)"
    )
    print("=" * 100)
    report("ANY (first hit whole window)", h)
    late = h.filter(pl.col("minute") >= 10)
    report("LATE only (entry>=m10)", late)

    print("\n" + "=" * 100)
    print("Coverage: of markets that traded, how many ever hit the cell?")
    print("=" * 100)
    # traded markets = distinct market in sides rows per part summed approx -> use hit vs market_count coarse
    print(
        f"  markets hitting cell (this run): {n_mk:,}  vs bucket-markets {market_count:,}"
    )

    # daily series
    dg = h.group_by("day").agg(
        pl.len().alias("n"), pl.col("won").cast(pl.Int32).sum().alias("w")
    )
    dg = dg.sort("day")
    print(
        "\n  trades/day: mean %.1f  min %d  max %d"
        % (dg["n"].mean(), dg["n"].min(), dg["n"].max())
    )
    print("  winrate/day mean %.1f%%" % (100 * dg["w"].sum() / dg["n"].sum()))

    # CSV
    os.makedirs("processed", exist_ok=True)
    coin = re.sub(r"[^a-zA-Z0-9]+", "_", universe).strip("_")[:24]
    out = f"processed/win_rate_opp_{coin}_15m.csv"
    h.drop("day").write_csv(out)
    print(f"[OK] wrote {out}  ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--universe", default=os.environ.get("MATRIX_UNIVERSE", DEFAULT_UNIVERSE)
    )
    ap.add_argument("--parts", type=int, default=None)
    ap.add_argument("--fee", type=float, default=0.010)
    ap.add_argument("--slip", type=float, default=0.005)
    a = ap.parse_args()
    run(a.universe, a.parts, a.fee, a.slip)
