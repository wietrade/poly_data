"""链上新鲜度检查: 读 poly_data 每次 update 写的 order_filled_state.json.

比较链上(order_filled 覆盖窗口) vs 钱包活动时间:
  - 钱包 TRADE 早于 v1 截止(config [annotation].v1_cutoff, 默认 2026-04-29)
    → v1/v1 合约期成交, order_filled(v2) 无记录, annotate 标 V1, 永久不判 M/T
  - 钱包 TRADE 晚于链上覆盖点(last_time) → 缝隙 NA, 先 `uv run poly-data` 追平
状态文件由 update_utils/update_chain.py 在每次链上更新结束时写:
  data/order_filled_state.json  {start_time_unix, start_time_utc, last_time_unix,
                                  last_time_utc, last_block, updated_at_utc}
"""

import datetime
import json
import os

import pandas as pd

from . import config
from .store import WalletStore


def load_chain_state():
    """读 order_filled_state.json; 无文件/损坏返回 None."""
    if not os.path.exists(config.FRESH_FILE):
        return None
    try:
        with open(config.FRESH_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def chain_cutoff_datetime():
    """链上 order_filled 覆盖点(aware datetime); 无状态文件返回 None.

    钱包 add/update 的拉取终点用它 —— 钱包数据最多拉到链上覆盖点, 不会比
    链上新 → annotate 无缝隙 NA.
    """
    cs = load_chain_state()
    t = (cs or {}).get("last_time_unix")
    if not t:
        return None
    return datetime.datetime.fromtimestamp(int(t), tz=datetime.timezone.utc)


def _fmt(unix_ts):
    return datetime.datetime.fromtimestamp(unix_ts, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def last_wallet_unix(wallet):
    """钱包最新一条 TRADE 活动的 unix 时间; 无数据/损坏返回 None."""
    p = os.path.join(WalletStore(wallet).dir, "activity.parquet")
    if not os.path.exists(p):
        return None
    try:
        df = pd.read_parquet(p, columns=["type", "utc_time"])
        tr = df[df["type"] == "TRADE"]
        if tr.empty:
            return None
        return int(pd.to_datetime(tr["utc_time"], utc=True).max().timestamp())
    except Exception:
        return None


def wallet_trades_before(wallet, cut_unix):
    """钱包 TRADE 早于 cut_unix 的笔数; 无 parquet/异常返回 0."""
    if cut_unix <= 0:
        return 0
    p = os.path.join(WalletStore(wallet).dir, "activity.parquet")
    if not os.path.exists(p):
        return 0
    try:
        df = pd.read_parquet(p, columns=["type", "utc_time"])
        tr = df[df["type"] == "TRADE"]
        if tr.empty:
            return 0
        t = pd.to_datetime(tr["utc_time"], utc=True)
        cut = pd.Timestamp(cut_unix, unit="s", tz="UTC")
        return int((t < cut).sum())
    except Exception:
        return 0


def check_fresh(wallet, verbose=True):
    """链上新鲜度检查(钱包采集已截止到链上覆盖点). 返回 (ok, info).

    语义: 钱包数据拉取截止 = 链上 order_filled last_time(见 chain_cutoff_datetime),
    所以钱包不会比链上新 —— 真正要防的是**链上停更**(很久没跑 uv run poly-data,
    钱包也被卡在旧点拉不动):
      - ok=False → 链上停更超过 stale_threshold_hours(默认 6h), 先 uv run poly-data
      - ok=True 但 info['early_trades']>0 → v1 期交易(annotate 标 V1, 永久)
    """
    cs = load_chain_state()
    if cs is None:
        if verbose:
            print(
                "[freshness] 未找到 order_filled_state.json —— 请先跑 "
                "`uv run poly-data`, 让 update_chain 在结束时生成链上新鲜度状态"
            )
        return True, {"missing_state": True}
    start_t = int(cs.get("start_time_unix") or 0)
    chain_t = int(cs.get("last_time_unix") or 0)
    wal_t = last_wallet_unix(wallet)
    info = {"start": start_t, "chain": chain_t, "wallet": wal_t}
    # 早于 v1 截止(默认 2026-04-29)的交易 → annotate 标 V1, 永久不标 M/T
    v1_cut = config.v1_cutoff_unix() or start_t
    early = wallet_trades_before(wallet, v1_cut)
    info["early_trades"] = early
    if early and verbose:
        print(
            f"[freshness] [NOTICE] 钱包有 {early} 笔 TRADE 早于 v1 截止 "
            f"{_fmt(v1_cut)} (v1 期/v1 合约成交, order_filled(v2) 无记录)"
            "\n            这些交易 annotate 将标 V1, 不做 M/T 判定(永久)"
        )
    # 链上停更检测(覆盖点距墙钟)
    cfg = config.load()
    thr_h = float(cfg.get("tracker", {}).get("stale_threshold_hours", 6))
    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    stale = now - chain_t
    if stale > thr_h * 3600:
        if verbose:
            print(
                f"[freshness] [WARN] 链上 order_filled 已停更 {stale / 3600:.1f}h "
                f"(覆盖至 {_fmt(chain_t)})"
                "\n            建议: 先 `uv run poly-data` 追平, 否则钱包只能拉到该点"
            )
        info["stale_h"] = round(stale / 3600, 1)
        return False, info
    if verbose:
        print(
            f"[freshness] [OK] 链上覆盖至 {_fmt(chain_t)} (滞后 {stale / 3600:.1f}h, "
            "正常); 钱包将截止到链上点, 放心 update + annotate"
        )
    return True, info
