"""M/T 标注: 读 poly_data order_filled 的链上 OrderFilled, 判定每笔 TRADE 角色.

规则:
  - 只取目标钱包参与、对手不是撮合系统合约(0xe111)的行
  - 钱包在 maker 列 → M; 在 taker 列 → T
  - v1→v2 迁移(2026-04-28)前的交易**不做 M/T 判定**, 统一标 V1:
    v1 期主撮合在 v1 合约(0x4bfb41d5...), 本库 order_filled 只采 v2(0xe111...),
    查不到 → 永久无法标. 截止时间由 config.toml [annotation].v1_cutoff 定
    (默认 2026-04-29T00:00:00Z), 扫描直接跳过该区间(省时省内存).
  - v1 截止之后仍无链上记录的 tx → NA(缺口/缝隙, 补链上数据后重跑可回填)
"""

import os

import pandas as pd
import pyarrow.dataset as ds

from . import config
from .store import WalletStore


def build_role_map(wallet, poly_path, match_contract, min_ts=None, progress=True):
    """扫 order_filled(可加 timestamp>=min_ts 过滤), 返回 {transactionHash: role}.

    min_ts: v1 截止(unix 秒). 给出时只扫该时刻之后的行 —— v1 期交易直接跳过,
    不在标注范围内.
    """
    w = wallet.lower()
    mc = match_contract.lower()
    if progress:
        lim = f" (>= {min_ts})" if min_ts else ""
        print(f"扫描 {poly_path}{lim} (找 {wallet[:10]}.. 参与行)...")
    dat = ds.dataset(poly_path)
    f = (ds.field("maker") == w) | (ds.field("taker") == w)
    if min_ts:
        f = f & (ds.field("timestamp") >= min_ts)
    tbl = dat.to_table(filter=f, columns=["transactionHash", "maker", "taker"])
    df = tbl.to_pandas()
    if df.empty:
        return {}
    for c in ("transactionHash", "maker", "taker"):
        df[c] = df[c].astype(str).str.lower()
    # 排除系统撮合合约对手
    real = df[~((df["maker"] == mc) | (df["taker"] == mc))]
    role = {}
    for h, g in real.groupby("transactionHash"):
        is_m = int((g["maker"] == w).sum())
        is_t = int((g["taker"] == w).sum())
        role[h] = "M" if is_m > is_t else ("T" if is_t else "?")
    if progress:
        from collections import Counter

        print("  角色分布:", dict(Counter(role.values())), f"(tx {len(role)})")
    return role


def annotate_wallet(wallet, poly_path=None, match_contract=None, out_suffix="_mt"):
    """给某钱包 activity.parquet 加 role 列, 存 activity{out_suffix}.parquet.

    role: M/T(正常) | V1(v1 期, 永久不标) | NA(v1 后缺口/缝隙, 可回填).
    """
    cfg = config.load()
    poly_path = poly_path or config.ORDER_FILLED_DIR
    mc = match_contract or cfg["poly_data"].get("match_contract")
    cutoff = config.v1_cutoff_unix()
    store = WalletStore(wallet)
    df = store.read_parquet()
    if df is None:
        print("无 activity.parquet, 先 add/update")
        return
    role = build_role_map(wallet, poly_path, mc, min_ts=cutoff or None)
    tr = df["type"] == "TRADE"
    h = df.loc[tr, "transactionHash"].astype(str).str.lower()
    r = h.map(role)
    if cutoff:
        v1 = pd.to_datetime(df.loc[tr, "utc_time"], utc=True) < pd.Timestamp(
            cutoff, unit="s", tz="UTC"
        )
        r = r.where(~v1, "V1")  # v1 期(扫描已跳过)统一 V1, 不判 M/T
    r = r.fillna("NA")  # 其余未命中 → NA(可回填)
    df.loc[tr, "role"] = r
    df.loc[~tr, "role"] = ""
    out = os.path.join(store.dir, f"activity{out_suffix}.parquet")
    df.to_parquet(out, index=False, compression="snappy")
    n = int(tr.sum())
    vc = df.loc[tr, "role"].value_counts()
    m, t = int(vc.get("M", 0)), int(vc.get("T", 0))
    v1n = int(vc.get("V1", 0))
    na = int(vc.get("NA", 0))
    marked = m + t
    print(f"写出 {out}: TRADE {n}, M/T 标到 {marked} ({100 * marked / n:.1f}%)")
    if cutoff:
        cut_txt = pd.Timestamp(cutoff, unit="s", tz="UTC").strftime(
            "%Y-%m-%d %H:%M UTC"
        )
    else:
        cut_txt = "未启用"
    print(
        f"  分类: M {m} | T {t} | V1 {v1n}(v1 期, 截止 {cut_txt} 前, 永久不标)"
        f" | NA {na}(v1 后缺口/缝隙, 可回填)"
    )
