"""按钱包落盘: jsonl(增量 append) + parquet(重建, 供查询)."""
import json
import os

import pandas as pd

from . import config

NUM = ("size", "usdcSize", "price", "outcomeIndex")


class WalletStore:
    def __init__(self, wallet):
        self.wallet = wallet.lower()
        self.dir = os.path.join(config.DATA, self.wallet)
        self.jsonl = os.path.join(self.dir, "activity.jsonl")
        self.parquet = os.path.join(self.dir, "activity.parquet")
        os.makedirs(self.dir, exist_ok=True)

    def load_keys(self):
        keys = set()
        if os.path.exists(self.jsonl):
            with open(self.jsonl, encoding="utf-8") as f:
                for line in f:
                    try:
                        o = json.loads(line)
                    except Exception:
                        continue
                    keys.add((o.get("type"), o.get("timestamp"), o.get("transactionHash")))
        return keys

    def append(self, rows):
        if not rows:
            return 0
        with open(self.jsonl, "a", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        return len(rows)

    def read_all(self):
        rows = []
        with open(self.jsonl, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def rebuild_parquet(self):
        rows = self.read_all()
        df = pd.DataFrame(rows)
        for c in NUM:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "timestamp" in df.columns:
            df["utc_time"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        if "outcome" in df.columns:
            df["outcome_norm"] = df["outcome"].str.upper()
        df.to_parquet(self.parquet, index=False, compression="snappy")
        return len(df)

    def read_parquet(self):
        if not os.path.exists(self.parquet):
            return None
        return pd.read_parquet(self.parquet)
