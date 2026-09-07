"""Conditional win-rate analysis on the reconstructed favorite matrix (win_rate_matrix.py).

Goal: test whether "buy-the-leader" conditions that 0x3615 / 4coinsbot / btc-15m-live rely on
actually create conditional alpha on top of the (near-calibrated) unconditional price grid.

Two conditional views, both on BTC 15m updown (clean fills, settled winners):

  A1. HOUR OF DAY (UTC): favorite buys by (UTC hour x merged price band): win% vs implied.
      Motivates/checks the `--hours` filters in the 43 paper experiments (v4..v7).

  A2. LEADERSHIP GAP (dominance): reconstruct both-side last price per 5s bucket per market,
      so every "buy" observation can be tagged with (a) which side was higher then,
      (b) the price gap |up-down|. We then restrict to TRUE-favorite buys
      (bought the side that is currently higher) at price 0.60-0.85 and measure win% as a
      function of gap tier and late-window (minute>=10). If "buy the leader" carries real
      alpha, win% should rise with gap / in the late window well above the unconditional ~62-77%.

Usage:
  python win_rate_matrix_cond.py [--universe ...] [--parts N]
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


def _band60(fb: int) -> int:  # merged favorite bands
    if fb <= 1:
        return 1  # 0.50-0.59
    if fb <= 3:
        return 2  # 0.60-0.69
    if fb <= 5:
        return 3  # 0.70-0.79
    if fb <= 7:
        return 4  # 0.80-0.89
    return 5  # 0.90+


def run(universe: str, parts_limit: int | None):
    t0 = time.time()
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

    def acc(d, key, n, w):
        a = d.get(key)
        if a is None:
            d[key] = [n, w]
        else:
            a[0] += n
            a[1] += w

    hour: dict = {}  # (hour, band60) -> [n,w]
    lead: dict = {}  # (th_tier, late) -> [n,w]  for true-fav buys @ 0.60-0.85
    pb_lead: dict = {}  # (price_band, tight01, late) -> [n,w]
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
        df = df.with_columns(
            [
                (pl.col("elapsed") // 60).cast(pl.Int32).clip(0, 14).alias("minute"),
                (pl.col("elapsed") // 5).cast(pl.Int32).alias("bucket5"),
                ((pl.col("start_epoch") + pl.col("elapsed")) // 3600 % 24)
                .cast(pl.Int32)
                .alias("hour_utc"),
                (pl.col("winner") == pl.col("side")).alias("won"),
                (pl.col("price") * 100).cast(pl.Int32).alias("pc"),
            ]
        )

        # ---- A1 hour aggregation (favorite buys) ----
        fv = df.filter(pl.col("pc") >= 50).with_columns(
            [((pl.col("pc") - 50) // 5).cast(pl.Int32).clip(0, 9).alias("fb")]
        )
        fv = fv.with_columns(
            pl.col("fb").map_elements(_band60, return_dtype=pl.Int32).alias("band")
        )
        g = fv.group_by(["hour_utc", "band"]).agg(
            pl.len().alias("n"), pl.col("won").cast(pl.Int32).sum().alias("w")
        )
        for h, b, n, w in zip(
            g["hour_utc"].to_list(),
            g["band"].to_list(),
            g["n"].to_list(),
            g["w"].to_list(),
        ):
            acc(hour, (int(h), int(b)), int(n), int(w))

        # ---- A2 leadership gap: rebuild both-side last price per (market, bucket5) ----
        last_pr = (
            df.sort(["bucket5", "side", "timestamp"])
            .group_by(["market_id", "bucket5", "side"])
            .agg(pl.col("price").last().alias("last_px"))
        )
        up = (
            last_pr.filter(pl.col("side") == "Up")
            .drop("side")
            .rename({"last_px": "up_px"})
        )
        dn = (
            last_pr.filter(pl.col("side") == "Down")
            .drop("side")
            .rename({"last_px": "dn_px"})
        )
        sides = up.join(dn, on=["market_id", "bucket5"], how="outer")

        obs = df.join(sides, on=["market_id", "bucket5"], how="left")
        # true favorite = bought side is currently the higher side
        obs = obs.with_columns(
            [
                (pl.col("up_px") >= pl.col("dn_px")).alias("up_is_fav"),
                (pl.col("up_px") - pl.col("dn_px")).abs().alias("gap"),
                (pl.col("up_px").is_not_null() & pl.col("dn_px").is_not_null()).alias(
                    "gap_ok"
                ),
            ]
        )
        obs = obs.with_columns(
            pl.when(pl.col("side") == "Up")
            .then(pl.col("up_is_fav"))
            .otherwise(~pl.col("up_is_fav"))
            .alias("bought_fav_now")
        )
        # focus: true-favorite buys at price 0.60-0.85
        tf = obs.filter(
            pl.col("bought_fav_now")
            & pl.col("gap_ok")
            & (pl.col("pc") >= 60)
            & (pl.col("pc") <= 85)
        )
        tf = tf.with_columns(
            [
                pl.when(pl.col("minute") >= 10)
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
                .alias("late"),
                pl.when(pl.col("gap") >= 0.30)
                .then(pl.lit(3))
                .when(pl.col("gap") >= 0.20)
                .then(pl.lit(2))
                .when(pl.col("gap") >= 0.10)
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
                .alias("th"),
            ]
        )
        # tiers: 0=any gap (<0.10),1=0.10,2=0.20,3=0.30 ; key th>=0 is all
        for thval in range(4):
            sub = tf.filter(pl.col("th") >= thval) if thval else tf
            for late in (0, 1):
                s2 = sub.filter(pl.col("late") == late)
                g2 = s2.group_by([]).agg(
                    pl.len().alias("n"), pl.col("won").cast(pl.Int32).sum().alias("w")
                )
                if g2.height:
                    acc(lead, (thval, late), int(g2["n"][0]), int(g2["w"][0]))
        # price-band x gap control: does gap add beyond the price level itself?
        tf2 = tf.with_columns((pl.col("pc") // 10).alias("pband"))
        for pb in (6, 7, 8):
            sb = tf2.filter(pl.col("pband") == pb)
            for tight, thv in ((0, 0), (1, 3)):
                ss = sb.filter(pl.col("th") >= thv)
                for late in (0, 1):
                    s2 = ss.filter(pl.col("late") == late)
                    g2 = s2.group_by([]).agg(
                        pl.len().alias("n"),
                        pl.col("won").cast(pl.Int32).sum().alias("w"),
                    )
                    if g2.height:
                        acc(
                            pb_lead, (pb, tight, late), int(g2["n"][0]), int(g2["w"][0])
                        )

        if (idx + 1) % 200 == 0:
            print(
                f"  part {idx + 1}/{len(parts)} kept {kept:,} ({time.time() - t0:.0f}s)",
                flush=True,
            )

    print(
        f"\n[OK] scanned {total:,} fills, kept-in-window {kept:,} ({time.time() - t0:.0f}s)"
    )

    print("\n" + "=" * 96)
    print("A1) FAVORITE buy win% by UTC hour x merged price band (win% ; implied mid)")
    print("=" * 96)
    bands = {
        1: (0.50, 0.59),
        2: (0.60, 0.69),
        3: (0.70, 0.79),
        4: (0.80, 0.89),
        5: (0.90, 1.00),
    }
    hdr = "hour | " + " | ".join(f"{a:.2f}-{b:.2f}" for a, b in bands.values())
    print(hdr)
    print("-" * 96)
    hour_tot = {}
    for h in range(24):
        cells = []
        for b, (a, z) in bands.items():
            n, w = hour.get((h, b), [0, 0])
            hour_tot[b] = hour_tot.get(b, [0, 0])
            hour_tot[b][0] += n
            hour_tot[b][1] += w
            cells.append(f"{100 * w / n:5.1f}" if n else "   . ")
        print(f"{h:>3}  | " + " | ".join(cells))
    print("-" * 96)
    cells = []
    for b, (a, z) in bands.items():
        n, w = hour_tot.get(b, [0, 0])
        cells.append(f"{100 * w / n:5.1f}" if n else "   . ")
    print(" TOT  | " + " | ".join(cells))

    print("\n" + "=" * 96)
    print("A2) TRUE-FAVORITE buy @0.60-0.85: win% by LEADERSHIP GAP tier x window part")
    print("    (gap = |up_px - down_px| at ~5s before the buy; late = minute>=10)")
    print("=" * 96)
    print(
        f"{'tier':<14}{'window':<8}{'n':>12}{'win%':>9}{'vs_implied':>12}{'EV_pp(0.25fee)':>14}"
    )
    tier_lo = {0: 0.00, 1: 0.10, 2: 0.20, 3: 0.30}
    for th in (0, 1, 2, 3):
        for late in (0, 1):
            n, w = lead.get((th, late), [0, 0])
            if not n:
                continue
            wr = 100 * w / n
            # implied at mid of 0.60-0.85 range approx 0.725 (we bucket by actual price below)
            print(
                f"gap>={tier_lo[th]:<5.2f}{'late' if late else 'early':<8}{n:>12,}{wr:>8.2f}%"
            )
    print()
    print("-- row keys: gap>=0.00 = unconditional (all gaps) --")

    print("\nA2b) price-band x gap control (does gap>0.30 add beyond the price level?)")
    for pb, (lo, hi) in ((6, (0.60, 0.69)), (7, (0.70, 0.79)), (8, (0.80, 0.85))):
        for tight, gl in ((0, "0.00"), (1, "0.30")):
            cells = []
            for late, wl in ((0, "early"), (1, "late")):
                n, w = pb_lead.get((pb, tight, late), [0, 0])
                cells.append(f"{wl}={100 * w / n:.1f}% (n={n:,})" if n else f"{wl}=.")
            print(f"price {lo:.2f}-{hi:.2f} gap>={gl}: " + "  ".join(cells))

    # write summary CSV
    os.makedirs("processed", exist_ok=True)
    coin = re.sub(r"[^a-zA-Z0-9]+", "_", universe).strip("_")[:24]
    rows = []
    tier_lo = {0: 0.00, 1: 0.10, 2: 0.20, 3: 0.30}
    for th in (0, 1, 2, 3):
        for late in (0, 1):
            n, w = lead.get((th, late), [0, 0])
            if n:
                rows.append(
                    {
                        "tier": f"gap>={tier_lo[th]:.2f}",
                        "window": "late" if late else "early",
                        "n": n,
                        "wins": w,
                        "win_rate": w / n,
                    }
                )
    out = f"processed/win_rate_cond_lead_{coin}_15m.csv"
    pl.DataFrame(rows).write_csv(out)
    print(f"[OK] wrote {out} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--universe", default=os.environ.get("MATRIX_UNIVERSE", DEFAULT_UNIVERSE)
    )
    ap.add_argument("--parts", type=int, default=None)
    a = ap.parse_args()
    run(a.universe, a.parts)
