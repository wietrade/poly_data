"""Win-rate matrix reconstruction — independent replication of btc-15m-live/data/win_rate.csv.

Answers, on OUR clean poly_data updown universe:
  For "buy the favorite (higher-priced side) at price p, at minute m of the 15m window,
  and hold to settlement" — what is the realized win rate?

Grid: price 5c buckets (0.50..1.00 -> 10 cols) x minute-in-window (0..14 -> 15 rows),
      plus an underdog (<0.50) companion view.
Method = updown_calibration.py style: stream trades_part_*.parquet (clean fills,
0xe111 mirror legs removed), join settled updown winners (data/settlements.parquet),
per-fill "buy side" observation (the BUY side of each fill buys nonusdc_side @ price),
won = (side == winner).

KEY facts reused (verified in updown_calibration.py):
  * token1 == "Up", token2 == "Down" in every updown market
  * settlements.parquet winner Up/Down per condition_id; slug tail = window start epoch (s, %900==0)
  * trades.price = USDC per outcome token (0..1), timestamp = block time (us)

Time binning: elapsed = trade_epoch_s - window_start_epoch ; keep 0 <= elapsed <= 900;
minute = elapsed // 60 (0..14). Fills after window end are excluded (they'd be
settlement-chasing, not "buy during window").

Usage:
  python win_rate_matrix.py                      # default: btc 15m, all parts
  python win_rate_matrix.py --universe "^(btc|eth)-updown-15m-[0-9]+$"
  python win_rate_matrix.py --parts 60           # smoke test on first 60 parts
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
# trades cols needed per part (column pruning -> fast IO)
NEED_COLS = ["market_id", "timestamp", "nonusdc_side", "price"]
TRADES_RE = re.compile(r"^trades_part_(\d+)\.parquet$")

WIN_SECS = 900  # 15m window


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


def load_settle(universe: str):
    """condition_id -> (winner 'Up'/'Down', start_epoch_s) for closed 15m markets."""
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


def run(universe: str, parts_limit: int | None):
    t0 = time.time()
    print(f"loading settled winners ({universe}) ...")
    settle = load_settle(universe)
    print(f"  settled universe markets: {settle.height:,}  ({time.time() - t0:.0f}s)")
    win_map = dict(zip(settle["condition_id"].to_list(), settle["winner"].to_list()))
    start_map = dict(
        zip(settle["condition_id"].to_list(), settle["start_epoch"].to_list())
    )
    print(f"  with start_epoch: {len(start_map):,}")

    settle_df = pl.DataFrame(
        {"market_id": list(win_map.keys()), "winner": list(win_map.values())}
    ).with_columns(pl.Series("start_epoch", [start_map[k] for k in win_map]))

    parts = _trades_parts()
    if parts_limit:
        parts = parts[:parts_limit]
    print(f"trades parts: {len(parts)}  (smoke={bool(parts_limit)})")

    # fav[(fb, minute)] = [n, w];  dog[(db, minute)] = [n, w]
    fav: dict = {}
    dog: dict = {}
    market_seen: dict = {}  # (coin-independent) per market minute bucket seen -> dedupe alt view later optional

    def acc(d: dict, key, n, w):
        a = d.get(key)
        if a is None:
            d[key] = [n, w]
        else:
            a[0] += n
            a[1] += w

    total = kept = 0
    for idx, path in parts:
        df = pl.read_parquet(path, columns=NEED_COLS)
        total += df.height
        df = df.filter(
            pl.col("price").is_finite() & (pl.col("price") > 0) & (pl.col("price") < 1)
        )
        df = df.join(settle_df, on="market_id", how="inner")
        n_rows = df.height
        kept += n_rows
        if n_rows == 0:
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
        # keep only fills inside the 15m trading window
        df = df.filter((pl.col("elapsed") >= 0) & (pl.col("elapsed") <= WIN_SECS))
        if df.height == 0:
            continue
        df = df.with_columns(
            [
                (pl.col("elapsed") // 60).cast(pl.Int32).clip(0, 14).alias("minute"),
                (pl.col("winner") == pl.col("side")).alias("won"),
                (pl.col("price") * 100).cast(pl.Int32).alias("pc"),
            ]
        )
        # favorite (buy higher side, price >= 0.50) : 5c buckets 0.50..1.00 -> 10
        fv = df.filter(pl.col("pc") >= 50).with_columns(
            (((pl.col("pc") - 50) // 5).cast(pl.Int32).clip(0, 9)).alias("fb")
        )
        # underdog (buy lower side, price < 0.50) : 5c buckets 0.00..0.50 -> 10
        dg = df.filter(pl.col("pc") < 50).with_columns(
            ((pl.col("pc") // 5).cast(pl.Int32).clip(0, 9)).alias("db")
        )

        g = fv.group_by(["fb", "minute"]).agg(
            pl.len().alias("n"), pl.col("won").cast(pl.Int32).sum().alias("w")
        )
        for fb, mn, n, w in zip(
            g["fb"].to_list(), g["minute"].to_list(), g["n"].to_list(), g["w"].to_list()
        ):
            acc(fav, (int(fb), int(mn)), int(n), int(w))

        g = dg.group_by(["db", "minute"]).agg(
            pl.len().alias("n"), pl.col("won").cast(pl.Int32).sum().alias("w")
        )
        for db, mn, n, w in zip(
            g["db"].to_list(), g["minute"].to_list(), g["n"].to_list(), g["w"].to_list()
        ):
            acc(dog, (int(db), int(mn)), int(n), int(w))

        if (idx + 1) % 200 == 0:
            print(
                f"  part {idx + 1}/{len(parts)}  rows {total:,} kept-in-window {kept:,} ({time.time() - t0:.0f}s)",
                flush=True,
            )

    print(
        f"\n[OK] scanned {total:,} fills, {kept:,} matched settled inside window ({time.time() - t0:.0f}s)"
    )

    def table(title, accmap, lo, hi, lo_cent):
        print("\n" + "=" * 118)
        print(title)
        print("=" * 118)
        cols = list(range(lo, hi))
        hdr = "minute | " + " | ".join(
            f"{lo_cent + c * 5 / 100:.2f}-{lo_cent + c * 5 / 100 + 0.04:.2f}"
            for c in cols
        )
        print(hdr)
        print("-" * 118)
        tot_buckets = {c: [0, 0] for c in cols}
        for mn in range(15):
            cells = []
            for c in cols:
                n, w = accmap.get((c, mn), [0, 0])
                tot_buckets[c][0] += n
                tot_buckets[c][1] += w
                cells.append(f"{100 * w / n:5.1f}" if n else "   . ")
            s = f"{mn:>2}   | " + " | ".join(cells)
            print(s + "  |")
        # per-bucket row
        print("-" * 118)
        cells = []
        for c in cols:
            n, w = tot_buckets[c]
            cells.append(f"{100 * w / n:5.1f}" if n else "   . ")
        print(" TOT   | " + " | ".join(cells) + "  |")
        return tot_buckets

    tb_fav = table(
        "FAVORITE buy (price>=0.50, 5c buckets) x minute - win% (replication of win_rate.csv)",
        fav,
        0,
        10,
        0.50,
    )
    tb_dog = table(
        "UNDERDOG buy (price<0.50, 5c buckets) x minute - win%",
        dog,
        0,
        10,
        0.00,
    )

    # implied-vs-real bias (favorite, all minutes pooled per bucket)
    print("\n" + "=" * 78)
    print("FAVORITE bias per 5c bucket (all minutes): win% vs implied mid")
    print("=" * 78)
    print(f"{'bucket':<12}{'n':>14}{'win%':>9}{'implied%':>9}{'bias_pp':>9}")
    for c in range(10):
        n, w = tb_fav[c]
        if n:
            wr = 100 * w / n
            impl = 100 * (0.50 + c * 0.05 + 0.025)
            lo = 0.50 + c * 0.05
            print(
                f"{lo:.2f}-{lo + 0.04:.2f}:{'':<3}{n:>14,}{wr:>8.2f}%{impl:>8.1f}%{wr - impl:>+9.1f}"
            )

    # write CSVs
    os.makedirs("processed", exist_ok=True)
    coin = re.sub(r"[^a-zA-Z0-9]+", "_", universe).strip("_")[:24]
    rows = []
    for c in range(10):
        for mn in range(15):
            n, w = fav.get((c, mn), [0, 0])
            if n:
                rows.append(
                    {
                        "bucket": f"0.{50 + c * 5}-0.{54 + c * 5}",
                        "bucket_lo": 0.50 + c * 0.05,
                        "minute": mn,
                        "n": n,
                        "wins": w,
                        "win_rate": w / n,
                    }
                )
    out = f"processed/win_rate_matrix_{coin}_15m.csv"
    pl.DataFrame(rows).write_csv(out)
    print(f"\n[OK] wrote {out}  ({len(rows)} cells with n>0)")
    print(f"  (total time {time.time() - t0:.0f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--universe", default=os.environ.get("MATRIX_UNIVERSE", DEFAULT_UNIVERSE)
    )
    ap.add_argument(
        "--parts", type=int, default=None, help="only first N parts (smoke)"
    )
    a = ap.parse_args()
    run(a.universe, a.parts)
