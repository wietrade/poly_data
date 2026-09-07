"""官方 data-api /activity 抓取(按天分窗, 幂等去重).

注意: /activity 每条 TRADE 是"撮合碎片", transactionHash 每行唯一(真链上 tx).
"""

import datetime
import json
import time
import urllib.request

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _opener(proxy_url=None):
    if proxy_url:
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        )
    return urllib.request.build_opener()


def fetch_day(wallet, s_epoch, e_epoch, base, page, opener):
    """拉一天所有页, 返回该日原始行列表(未去重)."""
    rows, off = [], 0
    while True:
        q = (
            f"{base}?user={wallet}&sortDirection=ASC&start={s_epoch}&end={e_epoch}"
            f"&offset={off}&limit={page}&excludeDepositsWithdrawals=false"
        )
        data = None
        for attempt in range(5):
            try:
                req = urllib.request.Request(q, headers=UA)
                with opener.open(req, timeout=30) as r:
                    data = json.loads(r.read().decode())
                break
            except Exception as ex:
                if attempt == 4:
                    print("   ERR", repr(ex)[:100], flush=True)
                    return None
                time.sleep(1.3 * (attempt + 1))
        if data is None:
            return None
        rows.extend(data)
        if len(data) < page:
            return rows
        off += page


def key_of(row):
    return (row.get("type"), row.get("timestamp"), row.get("transactionHash"))


def fetch_range(
    wallet,
    start_date,
    end_date,
    base,
    page,
    proxy_url,
    existing_keys,
    progress=True,
    end_dt=None,
):
    """逐天抓取 start_date..end_date(含), 返回 (新行, 调用数). 幂等.

    end_dt: 可选精确截止(aware datetime, 通常 = 链上 order_filled 覆盖点).
    给定后最后一天只拉到该时刻 —— 钱包数据不比链上新, annotate 无缝隙 NA.
    """
    opener = _opener(proxy_url)
    new_rows, calls = [], 0
    d = start_date
    end_day = end_dt.date() if end_dt is not None else end_date
    while d <= end_day:
        s = int(
            datetime.datetime(
                d.year, d.month, d.day, tzinfo=datetime.timezone.utc
            ).timestamp()
        )
        if end_dt is not None and d == end_day:
            e = int(end_dt.timestamp())
        else:
            e = s + 86400
        rows = fetch_day(wallet, s, e, base, page, opener)
        if rows is None:
            break
        calls += 1
        added = 0
        for r in rows:
            k = key_of(r)
            if k in existing_keys:
                continue
            existing_keys.add(k)
            new_rows.append(r)
            added += 1
        if progress and added:
            print(f"  {d}: +{added}", flush=True)
        d += datetime.timedelta(days=1)
    return new_rows, calls
