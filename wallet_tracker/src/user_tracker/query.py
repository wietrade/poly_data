"""查询: 从某钱包的 activity.parquet(或 _mt) 读."""
import os

import pandas as pd

from . import config


def load_df(wallet, mt=True):
    base = os.path.join(config.DATA, wallet.lower())
    for name in (("activity_mt.parquet" if mt else None), "activity.parquet"):
        if not name:
            continue
        p = os.path.join(base, name)
        if os.path.exists(p):
            return pd.read_parquet(p)
    return None


def market(df, slug, quiet=False):
    m = df[df["slug"] == slug]
    tr = m[m["type"] == "TRADE"]
    rd = m[m["type"] == "REDEEM"]
    up = tr[tr.get("outcome_norm") == "UP"]
    dn = tr[tr.get("outcome_norm") == "DOWN"]
    upc = up["usdcSize"].sum() if len(up) else 0.0
    dnc = dn["usdcSize"].sum() if len(dn) else 0.0
    rdv = rd["size"].sum() if len(rd) else 0.0
    print(f"{slug}: {len(m)} 条 (TRADE {len(tr)}/REDEEM {len(rd)})")
    print(f"  UP 买入 {len(up):4d}笔  ${upc:,.2f} | DOWN {len(dn):4d}笔  ${dnc:,.2f}")
    print(f"  redeem ${rdv:,.2f} → 净 ${rdv-upc-dnc:,.2f}")
    if "role" in df.columns and len(tr):
        print("  role:", tr["role"].value_counts().to_dict())
    if not quiet and len(tr) <= 120:
        for _, r in tr.sort_values("timestamp").iterrows():
            h = str(r["transactionHash"])[:12]
            print(f"    {str(r.get('utc_time',''))[:19]} {str(r.get('outcome_norm','')):4s} "
                  f"@{r['price']:.3f} {r['size']:7.2f}股 ${r['usdcSize']:.2f}  {h}..")


def by_hash(df, h):
    r = df[df["transactionHash"] == h]
    if not len(r):
        print("未命中"); return
    for _, x in r.iterrows():
        print(f"  {x['utc_time']} {x['type']:12s} {str(x.get('slug',''))} "
              f"{str(x.get('outcome','')):5s} px={x.get('price','')} size={x.get('size','')} "
              f"usdc={x.get('usdcSize','')} role={x.get('role','')}")


def day(df, date):
    s = pd.Timestamp(date, tz="UTC")
    d = df[(df["utc_time"] >= s) & (df["utc_time"] < s + pd.Timedelta(days=1))]
    tr = d[d["type"] == "TRADE"]
    rd = d[d["type"] == "REDEEM"]
    print(f"{date}: {len(d)} 条 | TRADE买入 {len(tr)}笔 ${tr['usdcSize'].sum():,.2f} "
          f"| REDEEM {len(rd)}次 ${rd['size'].sum():,.2f}")
    if "role" in df.columns and len(tr):
        print("  role:", tr["role"].value_counts().to_dict())


def daily_pnl(df):
    tr = df[df["type"] == "TRADE"]
    rd = df[df["type"] == "REDEEM"]
    buy = tr.set_index("utc_time")["usdcSize"].resample("D").sum()
    rec = rd.set_index("utc_time")["size"].resample("D").sum()
    idx = buy.index.union(rec.index).sort_values()
    cum = 0.0
    print(f"{'日期':12s}{'买入$':>13s}{'赎回$':>13s}{'当日净':>12s}{'累计':>12s}")
    for d in idx:
        b = float(buy.get(d, 0.0)); r = float(rec.get(d, 0.0))
        cum += r - b
        print(f"{str(d.date()):12s}{b:13,.0f}{r:13,.0f}{r-b:12,.0f}{cum:12,.0f}")
