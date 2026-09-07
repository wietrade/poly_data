#!/usr/bin/env python3
"""user-trade-tracker CLI.

用法:
  python cli.py add     <wallet> [start_date]   登记并全量采集
  python cli.py update  [wallet ...]            增量更新(默认全部)
  python cli.py annotate <wallet>               用 poly_data 标 M/T
  python cli.py list                            已登记钱包
  python cli.py query <market|hash|day|daily> ... <wallet>
"""

import argparse
import datetime
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from user_tracker import annotate, api, config, freshness
from user_tracker import query as q
from user_tracker.store import WalletStore


def _cfg():
    return config.load()


def _proxy(cfg):
    p = cfg.get("proxy", {})
    return p["url"] if p.get("enabled") else None


def _range(cfg):
    base = cfg["api"]["base"]
    return base, int(cfg["api"].get("page", 500)), _proxy(cfg)


def _end_dt():
    """钱包拉取截止 = 链上 order_filled 覆盖点; 无链上状态则用当前墙钟.

    钱包数据最多拉到链上覆盖点, 不比链上新 → annotate 无缝隙 NA.
    """
    edt = freshness.chain_cutoff_datetime()
    return edt or datetime.datetime.now(datetime.timezone.utc)


def _v1_cutoff_date():
    """v1 截止日期(v1_cutoff 配置); 未配置返回 None."""
    u = config.v1_cutoff_unix()
    if not u:
        return None
    return datetime.datetime.fromtimestamp(u, tz=datetime.timezone.utc).date()


def cmd_add(args):
    cfg = _cfg()
    base, page, proxy = _range(cfg)
    wallet = args.wallet.lower()
    reg = config.load_wallets()
    start = (
        args.start
        or reg.get(wallet, {}).get("start")
        or cfg["tracker"]["default_start"]
    )
    start_d = datetime.date.fromisoformat(start)
    # 起点 clamp 到 v1 截止(默认 2026-04-29): 更早的交易必标 V1(不判 M/T), 拉了白拉
    v1_d = _v1_cutoff_date()
    if v1_d and start_d < v1_d:
        print(
            f"  [提示] 起点 {start_d} < v1 截止 {v1_d}(v1 期只能标 V1); 起点设为 {v1_d}"
        )
        start_d = v1_d
    # 终点 = 链上 order_filled 覆盖点(钱包不比链上新 → annotate 无缝隙 NA)
    end_dt = _end_dt()
    end_d = end_dt.date()
    st = WalletStore(wallet)
    keys = st.load_keys()
    print(
        f"采集 {wallet}  {start_d} ~ {end_dt:%Y-%m-%d %H:%M} UTC"
        f"  (已有 {len(keys)} key)"
    )
    rows, calls = api.fetch_range(
        wallet, start_d, end_d, base, page, proxy, keys, end_dt=end_dt
    )
    added = st.append(rows)
    print(f"新增 {added} 行 (调用 {calls}); 重建 parquet...")
    n = st.rebuild_parquet()
    print(f"activity.parquet: {n} 行")
    reg[wallet] = {"start": start_d.isoformat(), "added": str(datetime.date.today())}
    config.save_wallets(reg)


def cmd_update(args):
    cfg = _cfg()
    base, page, proxy = _range(cfg)
    reg = config.load_wallets()
    wallets = [w.lower() for w in args.wallets] or list(reg)
    lookback = int(cfg["tracker"].get("lookback_days", 2))
    # 终点 = 链上 order_filled 覆盖点(钱包不比链上新 → 无缝隙 NA)
    end_dt = _end_dt()
    end_d = end_dt.date()
    # 链上新鲜度: 若链上停更超阈值 → 提示先 uv run poly-data
    fresh_req = bool(cfg["tracker"].get("require_chain_fresh", True))
    for w in wallets:
        if w not in reg:
            print(f"未登记: {w} (先 add)")
            continue
        if fresh_req and not args.force:
            ok, _info = freshness.check_fresh(w)
            if not ok:
                print(
                    f"  [跳过 {w[:10]}..] 链上数据较旧, 请先 `uv run poly-data`"
                    " 追平; 或 --force 强制更新钱包"
                )
                continue
        st = WalletStore(w)
        keys = st.load_keys()
        # 起点: 已有数据最大时间往前回看 lookback 天
        maxts = 0
        for k in keys:
            if isinstance(k[1], int):
                maxts = max(maxts, k[1])
        if maxts:
            last = datetime.datetime.fromtimestamp(
                maxts, tz=datetime.timezone.utc
            ).date()
            start_d = last - datetime.timedelta(days=lookback)
        else:
            start_d = datetime.date.fromisoformat(reg[w]["start"])
        print(f"[{w}] 增量 {start_d} ~ {end_dt:%m-%d %H:%M} UTC ...")
        rows, calls = api.fetch_range(
            w, start_d, end_d, base, page, proxy, keys, end_dt=end_dt
        )
        added = st.append(rows)
        if added or args.rebuild or not os.path.exists(st.parquet):
            n = st.rebuild_parquet()
            print(f"  +{added} 行 → parquet {n} 行")
        else:
            print("  +0 行(已最新)")


def cmd_annotate(args):
    annotate.annotate_wallet(args.wallet.lower())


def cmd_list(args):
    reg = config.load_wallets()
    for w, info in reg.items():
        p = os.path.join(config.DATA, w, "activity.parquet")
        n = "?"
        if os.path.exists(p):
            try:
                import pandas as pd

                n = len(pd.read_parquet(p, columns=["type"]))
            except Exception:
                pass
        print(f"  {w}  start={info.get('start')} rows={n}")


def cmd_query(args):
    df = q.load_df(args.wallet.lower())
    if df is None:
        print("无数据, 先 add/update")
        return
    if args.kind == "market":
        q.market(df, args.slug, quiet=args.quiet)
    elif args.kind == "hash":
        q.by_hash(df, args.hash)
    elif args.kind == "day":
        q.day(df, args.date)
    elif args.kind == "daily":
        q.daily_pnl(df)
    elif args.kind == "summary":
        tr = df[df["type"] == "TRADE"]
        rd = df[df["type"] == "REDEEM"]
        print(
            f"TRADE {len(tr)} 买入 ${tr['usdcSize'].sum():,.0f} | REDEEM {len(rd)} ${rd['size'].sum():,.0f}"
        )
        if "role" in df.columns and len(tr):
            print("role:", tr["role"].value_counts().to_dict())


def main():
    # Windows 默认 stdout 为 GBK, 中文/符号会乱码或 UnicodeEncodeError
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    p = argparse.ArgumentParser(description="user-trade-tracker")
    sub = p.add_subparsers(dest="cmd", required=True)

    pa = sub.add_parser("add")
    pa.add_argument("wallet")
    pa.add_argument("start", nargs="?")
    pa.set_defaults(fn=cmd_add)

    pu = sub.add_parser("update")
    pu.add_argument("wallets", nargs="*")
    pu.add_argument("--rebuild", action="store_true")
    pu.add_argument(
        "--force", action="store_true", help="跳过链上新鲜度检查, 强制更新钱包"
    )
    pu.set_defaults(fn=cmd_update)

    pn = sub.add_parser("annotate")
    pn.add_argument("wallet")
    pn.set_defaults(fn=cmd_annotate)
    pl = sub.add_parser("list")
    pl.set_defaults(fn=cmd_list)

    pq = sub.add_parser("query")
    pq.add_argument("kind", choices=["market", "hash", "day", "daily", "summary"])
    pq.add_argument("args", nargs="+")
    pq.add_argument("--quiet", action="store_true")
    pq.set_defaults(fn=cmd_query)

    a = p.parse_args()
    if a.cmd == "query":
        qa = a.args
        if a.kind == "market":
            a.slug, a.wallet = qa[0], qa[-1]
            a.quiet = a.quiet or (len(qa) > 2 and qa[1] == "--quiet")
        elif a.kind == "hash":
            a.hash, a.wallet = qa[0], qa[-1]
        elif a.kind == "day":
            a.date, a.wallet = qa[0], qa[-1]
        elif a.kind in ("daily", "summary"):
            a.wallet = qa[0]
    a.fn(a)


if __name__ == "__main__":
    main()
