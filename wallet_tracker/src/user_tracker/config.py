"""配置加载.

布局(v0.2, 并入 poly_data 后统一在链上主项目里管理):

  poly_data/                     <- POLY_ROOT (链上主项目, git 仓库)
    update_utils/update_chain.py <- 每次 update 结束写 order_filled_state.json
    data/
      order_filled/              <- 链上 OrderFilled (poly-data 写, M/T 标注源)
      order_filled_state.json    <- 链上新鲜度状态 (读它判断是否先更链上)
      tracked_wallets/           <- 本应用钱包数据 {addr}/activity.*
    wallet_tracker/              <- 本应用 HERE
      config.toml
      src/user_tracker/
"""

import datetime
import os

import tomllib

HERE = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)  # .../wallet_tracker
POLY_ROOT = os.path.dirname(HERE)  # poly_data 根(链上主项目)
if not os.path.isdir(os.path.join(POLY_ROOT, "update_utils")):
    raise RuntimeError(
        "wallet_tracker 必须位于 poly_data 项目内(作为其子目录); 当前 HERE=" + HERE
    )
CONF = os.path.join(HERE, "config.toml")

POLY_DATA_DIR = os.path.join(POLY_ROOT, "data")
DATA = os.path.join(POLY_DATA_DIR, "tracked_wallets")  # 钱包 activity 数据
WALLETS_FILE = os.path.join(DATA, "wallets.json")
ORDER_FILLED_DIR = os.path.join(POLY_DATA_DIR, "order_filled")  # 链上 M/T 源
FRESH_FILE = os.path.join(POLY_DATA_DIR, "order_filled_state.json")  # 链上新鲜度


def load(path=CONF):
    with open(path, "rb") as f:
        return tomllib.load(f)


def v1_cutoff_unix():
    """v1→v2 迁移截止(unix 秒). 此时间前的交易不做 M/T, 统一标 V1(v1 期).

    默认 config.toml [annotation].v1_cutoff = 2026-04-29T00:00:00Z.
    未配置/解析失败返回 0(即不启用 V1 分类, 全部尝试标注).
    """
    cfg = load()
    s = cfg.get("annotation", {}).get("v1_cutoff")
    if not s:
        return 0
    try:
        iso = s[:-1] + "+00:00" if s.endswith("Z") else s
        return int(datetime.datetime.fromisoformat(iso).timestamp())
    except Exception:
        return 0


def poly_venv_python():
    """本应用与 poly_data 共用 .venv(有 pandas/pyarrow/polars) — 供外部提示."""
    cand = os.path.join(POLY_ROOT, ".venv", "Scripts", "python.exe")
    return cand if os.path.exists(cand) else "python"


def ensure_data():
    os.makedirs(DATA, exist_ok=True)


def load_wallets():
    ensure_data()
    if os.path.exists(WALLETS_FILE):
        import json

        with open(WALLETS_FILE, encoding="utf-8") as f:
            return json.load(f)  # {addr: {start:..., added:...}}
    return {}


def save_wallets(w):
    import json

    ensure_data()
    tmp = WALLETS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(w, f, indent=2, ensure_ascii=False)
    os.replace(tmp, WALLETS_FILE)
