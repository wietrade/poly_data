# Polymarket Data (v2)

[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://opensource.org/licenses/GPL-3.0)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)

A pipeline for fetching, processing, and analyzing Polymarket v2 trading data — plus a wallet tracker that profiles any address as Maker/Taker. Streams order events directly from the Polymarket **CTF Exchange V2** contract on Polygon via [Envio HyperSync](https://docs.envio.dev/docs/HyperSync/overview), joins them with market metadata from the Polymarket CLOB API, and writes structured trades to Parquet.

中文全景说明（三块东西关系、数据边界、实操命令）见 [`链上数据与钱包跟踪-使用说明.md`](链上数据与钱包跟踪-使用说明.md)。

## ⚠️ v1 → v2 migration

Polymarket migrated to a new set of CTF Exchange contracts on **2026-04-28** and stopped supporting their old subgraph indexer. The old pipeline in this repo (Goldsky subgraph + GraphQL polling) **no longer returns complete data**, so it has been removed.

The previous version is preserved at the [`v1-final`](https://github.com/warproxxx/poly_data/tree/v1-final) tag in the original repository if you need it for historical analysis. **For any new work, use this v2 version.**

The V1 retriever used goldsky, but now goldsky only gives data through a turbo pipeline that is expensive and complex. 

V2 reads directly from on-chain events via HyperSync, which streams logs (with block timestamps inline) across the full chain in a single connection without RPC throttling. A single request scans hundreds of thousands of blocks, so the full backfill is only a handful of requests. HyperSync requires a free API token (mandatory since 2025-11-03); generate one at [envio.dev/app/api-tokens](https://envio.dev/app/api-tokens) and set it as `HYPERSYNC_API`.


## Configuration

All tuning is via environment variables.

| Variable | Default | What it does |
|---|---|---|
| `HYPERSYNC_API` | _(unset)_ | **Required.** Envio HyperSync bearer token. Generate a free one at [envio.dev/app/api-tokens](https://envio.dev/app/api-tokens) — make sure it has **HyperSync** product access |
| `ORDER_PART_ROWS` | `500000` | Rows buffered in memory before the chain stage flushes one `data/order_filled/part_*.parquet` file. Lower it (e.g. `200000`) if the chain backfill OOMs on a RAM-constrained machine. |
| `PROCESS_CHUNK_SIZE` | `0` | Reserved (formerly CSV chunking). Processing now operates per whole Parquet part, so this is no longer needed. |
| `MARKET_SLUG_RE` | _(unset)_ | Optional **universe filter**: a regex matched against each market's slug. When set, only trades in matching markets are written to `processed/trades/` (everything else is dropped). The raw order parts are never touched, so re-processing with a different filter later needs no chain re-scrape. Example for crypto 5m/15m updown: `MARKET_SLUG_RE="^(btc|eth|sol)-updown-(5m|15m)-[0-9]+$"`. Empty/unset = keep all markets (default). |
| `KEEP_ALL_MARKET_FIELDS` | _(unset)_ | `update_markets` writes a **lean column set** by default (id/clobTokenIds/condition_id/question_id/question/market_slug/closed/active/end_date_iso/tags), dropping the ~20 useless or huge fields (image, icon, description, rewards, neg_risk_*, …). This shrinks the full-history markets.csv from ~6 GB to ~1.6 GB. Set `KEEP_ALL_MARKET_FIELDS=1` to write every CLOB field (old behaviour). |

Set them in `.env` file:

```bash
export HYPERSYNC_API="your-token-here"     # required — get one at envio.dev/app/api-tokens
export ORDER_PART_ROWS=500000              # only if the chain backfill is RAM-tight
```

## Table of Contents

- [Overview](#overview)
- [Protocol Contracts & Data Coverage](#protocol-contracts--data-coverage)
- [Installation](#installation)
- [HyperSync API token](#hypersync-api-token)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Files](#data-files)
- [Pipeline Stages](#pipeline-stages)
- [Resumable & Incremental](#resumable--incremental)
- [Troubleshooting](#troubleshooting)
- [Tests](#tests)
- [Analysis](#analysis)
- [License](#license)

## Overview

`update.py` runs three stages:

1. **Markets** — fetches all markets from the Polymarket **CLOB** API (`/markets`, 1000/page, concurrent) into `data/markets.csv`. Runs to completion first so the full list exists before any trade is processed.
2. **Chain** — streams `OrderFilled` events from the CTF Exchange V2 contract (`0xE111180000d2663C0091e4f400237545B87B996B`) on Polygon via Envio HyperSync. Resumable from the last scanned block.
3. **Process** — joins order events with market metadata to produce labeled trades with price, USD amount, and BUY/SELL direction.

The stages run **sequentially** (markets → chain → process): the complete market list is built first, so every scraped trade can be labeled against it.

## Protocol Contracts & Data Coverage

### Official contract set (Polygon, v2)

From the official deployment table in `Polymarket/ctf-exchange-v2` and the official SDK configs (`py-clob-client-v2`, `rs-clob-client-v2`, `clob-client-v2`, `py-sdk`, `ts-sdk`):

| Contract | Address | Role |
|---|---|---|
| **CTFExchangeV2** | `0xE111180000d2663C0091e4f400237545B87B996B` | Standard-market matching engine — **the contract this pipeline streams from** |
| NegRiskCtfExchangeV2 | `0xe2222d279d744050d28e00520010520000310F59` | Neg-risk (multi-outcome) market matching engine |
| exchangeV3 | `0xe3333700cA9d93003F00f0F71f8515005F6c00Aa` | Newer matching engine (some SDK envs list `0x9fE6e61422AdB6F610d8597F9684b16912D50C3D`) |
| CtfCollateralAdapter | `0xADa100874d00e3331D00F2007a9c336a65009718` | USDC ↔ CTF collateral adapter |
| NegRiskCtfCollateralAdapter | `0xAdA200001000ef00D07553cEE7006808F895c6F1` | Neg-risk collateral adapter |
| ConditionalTokens | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` | CTF conditional-token contract (mint/burn) |
| NegRiskAdapter | `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` | Neg-risk adapter |
| auto_redeem_operator | `0xa1200000d0002264C9a1698e001292D00E1b00af` | Auto-redemption operator |
| safe_multisend | `0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761` | Safe multi-send helper |
| relay_hub | `0xD216153c06E857cD7f72665E0aF1d7D82172F494` | Relay hub |

### Coverage scope

The chain stage is scoped to **CTFExchangeV2 (`0xe111...`)** only, so the `OrderFilled` events in this dataset are exclusively from standard-market matching:

- ✅ **Standard markets** (binary up/down, crypto 5m/15m, etc.) are fully captured.
- ❌ **Neg-risk markets** (multi-outcome, e.g. "who will win the election") match on `NegRiskCtfExchangeV2` (`0xe222...`), which emits its own `OrderFilled` on its own contract — **not in this dataset**.
- ❌ **exchangeV3 markets** (`0xe333...`) — also **not in this dataset**.

These other exchanges are a **coverage gap, not a contamination**: they never appear as `maker`/`taker` inside `0xe111...` events (verified across the full dataset — see the order_filled section). To capture neg-risk or V3 markets, open a separate HyperSync stream scoped to `0xe222...` / `0xe333...` and run the same process stage.

## Installation

This project uses [UV](https://docs.astral.sh/uv/) for fast package management.

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or with pip
pip install uv
```

Then install dependencies:

```bash
uv sync
```

## HyperSync API token

The chain stage streams from Envio HyperSync, which **requires a bearer token** (mandatory since 2025-11-03). The free tier is enough for this pipeline. Get one:

1. Sign in at **[envio.dev/app/api-tokens](https://envio.dev/app/api-tokens)**.
2. Create a new token. **Make sure it has _HyperSync_ product access** — a token scoped only to HyperRPC/HyperIndex authenticates but returns `403 "Your token does not have access to this product"` on data queries.
3. Add it to a `.env` file in the project root:

   ```bash
   HYPERSYNC_API="your-token-here"
   ```

Without it the run stops immediately with a message telling you to set `HYPERSYNC_API`.

### Free vs paid: backfill speed

The free tier is rate-limited, so the first full backfill takes **several hours to a few days**. A **paid [Envio token](https://envio.dev/pricing) is much faster** (no throttling). Just set `HYPERSYNC_API` to it, no code change. The backfill is resumable, so the free tier can also just run unattended.

## Quick Start

```bash
uv run poly-data        # or: uv run python update.py
```

That's it. Runs markets → chain → process in order. **First run is the long one** — the full market list from CLOB takes a couple of minutes, then the initial chain backfill from v2 genesis (~April 2026) over HyperSync runs after it (timing depends on your tier — see [Free vs paid](#free-vs-paid-backfill-speed)). Subsequent runs only pull deltas and finish in seconds. The backfill is resumable: stop with Ctrl-C and rerun anytime.

To run any stage individually:

```bash
uv run python -m update_utils.update_markets
uv run python -m update_utils.update_chain
uv run python -m update_utils.process_live
```

## Project Structure

```
poly_data/
├── update.py                  # thin shim → update_utils.pipeline.main
├── update_utils/
│   ├── pipeline.py            # orchestrator: markets → chain → process (poly-data entrypoint)
│   ├── update_markets.py      # Polymarket CLOB /markets → markets.csv
│   ├── update_chain.py        # HyperSync OrderFilled events → data/order_filled/part_*.parquet
│   └── process_live.py        # join order parts ↔ markets → processed/trades/trades_part_*.parquet
├── poly_utils/
│   └── utils.py               # market loader, missing-token backfill
├── wallet_tracker/            # per-address Maker/Taker profiling (see below)
│   ├── cli.py
│   └── src/user_tracker/      # annotate / query / store / freshness
├── tests/                     # pytest unit tests (pure, offline)
├── data/                      # all generated data + resume state (gitignored)
│   ├── markets.csv            # all markets (id = condition_id, clobTokenIds, …)
│   ├── missing_markets.csv    # markets backfilled per-token from trades
│   ├── markets_state.json     # CLOB pagination cursor for incremental resume
│   ├── order_filled/          # raw order events from chain (chunked Parquet)
│   │   └── part_00000000.parquet
│   └── cursor_state.json      # last scanned block + next part index
└── processed/                 # user-facing output (gitignored)
    └── trades/                # labeled trades for analysis (chunked Parquet)
        └── trades_part_00000000.parquet
```

## Data Files

### `data/markets.csv`
All markets from the Polymarket CLOB API. By default a **lean column set** is written (see `KEEP_ALL_MARKET_FIELDS`): `id`, `clobTokenIds`, `condition_id`, `question_id`, `question`, `market_slug`, `closed`, `active`, `end_date_iso`, `tags`. Nested fields (`tokens`, `tags`, …) are stored as JSON strings. Two derived columns are prepended for the downstream join: `id` (= the on-chain `condition_id`) and `clobTokenIds` (JSON array of the market's two token IDs). Set `KEEP_ALL_MARKET_FIELDS=1` to preserve every CLOB field as-is (old behaviour — the full ~289万-market history is ~6 GB that way).

Key fields used downstream: `id` (= `condition_id`), `clobTokenIds` (JSON array — first element = `token1`, second = `token2`), `question`, `market_slug`, `closed`.

### `data/order_filled/part_*.parquet`
Raw `OrderFilled` events decoded from the chain, written as **chunked Parquet** (`ROWS_PER_PART` rows per file, zstd-compressed). Each part file is written atomically (`.tmp` + `os.replace`) so an interrupted run never leaves a half-written part. Schema:

| Column | Type | Notes |
|---|---|---|
| `timestamp` | Int64 | Unix seconds (from block timestamp) |
| `maker` | String | Maker address, lowercase |
| `makerAssetId` | String | `"0"` if maker is paying USDC; otherwise the CTF token ID (kept as string — token IDs can exceed Int64) |
| `makerAmountFilled` | Int64 | Raw integer (6 decimals — USDC and CTF tokens both use 6) |
| `taker` | String | Taker address, lowercase |
| `takerAssetId` | String | `"0"` if taker is paying USDC; otherwise the CTF token ID |
| `takerAmountFilled` | Int64 | Raw integer (6 decimals) |
| `transactionHash` | String | Polygon transaction hash |
| `orderHash` | String | Order hash (topic 1) |
| `fee` | Int64 | Trading fee paid by taker, raw (6 decimals) |

The v2 `OrderFilled` event natively carries a single `tokenId` + `side` (BUY=0, SELL=1) referring to the maker's order. The reader maps that back to the v1-compatible maker/taker/asset schema above so downstream code stays simple.

**Why the exchange appears as `taker` (`0xe111...`).** Every `matchOrders` fill emits two `OrderFilled` legs:

1. **maker leg** — `maker` = the resting order's owner, `taker` = the aggressive taker user. This is the real user↔user trade.
2. **taker leg** — `maker` = the aggressive taker user, `taker` = `0xe111...`. This is the exchange's own custody leg: assets flow taker → exchange → maker, and the exchange contract source code hardcodes `taker: address(this)` in this event (`Trading.sol::_settleComplementaryTaker` / `_matchBuyOrders`).

The `0xe111180000d2663c0091e4f400237545b87b996b` address is therefore the **CTFExchangeV2 contract itself** (confirmed by the official deployment table in `Polymarket/ctf-exchange-v2`), not a real counterparty, and its rows are a **redundant duplicate** of the maker leg — the real user↔user fill is already fully recorded in the maker leg. `process_live` drops these rows (where `maker` or `taker` equals the exchange address) so `processed/trades` contains each real trade exactly once.

**It is the only one.** Verified across the full dataset: `0xe111...` is the *only* protocol contract that ever appears as `maker` or `taker` in these events (2.1B rows as taker, 0 as maker). The other official contracts — NegRiskCtfExchangeV2 (`0xe222...`), CtfCollateralAdapter, NegRiskCtfCollateralAdapter, ConditionalTokens, NegRiskAdapter, and exchangeV3 (`0xe333...`) — never appear, because they emit their own `OrderFilled` on their own contracts, which this stream (scoped to `0xe111...`) does not capture. So filtering `0xe111...` alone is both necessary and sufficient.

### `processed/trades/trades_part_*.parquet`
Labeled trades for analysis. One trades part per source order part (same index), so resume simply means "process any order part with no matching trades part". Rows where `maker` or `taker` is the CTF Exchange V2 contract (`0xe111...`) are filtered out — see the order_filled section above — so every row is a real user↔user trade. Schema:

| Column | Type | Notes |
|---|---|---|
| `timestamp` | Datetime | from block unix seconds |
| `market_id` | String | from markets.csv (null if market wasn't found) |
| `maker`, `taker` | String | addresses |
| `nonusdc_side` | String | `"token1"` or `"token2"` |
| `maker_direction`, `taker_direction` | String | `"BUY"` / `"SELL"` |
| `price` | Float64 | USDC per outcome token (0–1) |
| `usd_amount` | Float64 | trade size in USD |
| `token_amount` | Float64 | outcome tokens transferred |
| `fee` | Float64 | trading fee in USD (raw 6-dec ÷ 1e6) |
| `transactionHash` | String | |

## Pipeline Stages

### 1. `update_markets` — Polymarket CLOB `/markets`

Pages through the CLOB `/markets` endpoint (1000 markets/page, offset cursor) with concurrent request waves, covering the full ~1.5M-market history — closed and active — in a couple of minutes (vs. the old Gamma keyset's ~hour at 100/page). Each market is written to `data/markets.csv` with `id` = `condition_id` and `clobTokenIds` derived from its `tokens`. The next offset is saved to `data/markets_state.json` so an interrupted run resumes.

### 2. `update_chain` — HyperSync `OrderFilled` stream

Opens a HyperSync stream filtered to the CTF Exchange V2 contract and the `OrderFilled` topic. HyperSync returns logs in bulk along with their block timestamps in the same response, so there's no separate `eth_getBlock` pass. Each batch is ABI-decoded and buffered; every `ROWS_PER_PART` rows are flushed as an atomic chunked-Parquet part file into `data/order_filled/`. The cursor — persisted to `data/cursor_state.json` — records both `last_block` (advanced only after a part lands on disk) and the next `part_index`. Because a crash never leaves a partial part and the stream restarts at `last_block`, resumes are exact (no duplicates, no gaps). A 20-block reorg buffer is applied to the chain tip.

### 3. `process_live`

Reads the raw order parts (`data/order_filled/part_*.parquet`), skips any part index that already has a `processed/trades/trades_part_*.parquet`, joins the rest against `get_markets()` (which parses `clobTokenIds` into `token1`/`token2`), computes price/USD/direction, and writes one trades part per order part. Whole-part granularity means resume is trivial and a crash mid-part can't corrupt output.

If any trade references a token ID not in `markets.csv`, it's backfilled into `missing_markets.csv` via batched, parallel Gamma API requests before the join (the CLOB list isn't queryable by token ID, so Gamma's `clob_token_ids` lookup is used here), with `id` = `conditionId` to stay consistent with `markets.csv`.

## Wallet Tracker

`wallet_tracker/` tracks a configured set of addresses and labels each of their fills as **Maker** or **Taker**, on top of the same `processed/trades` output — useful for wallet intelligence / copy-trading research.

- Tracked wallets: `data/tracked_wallets/wallets.json` (config knobs in `wallet_tracker/config.toml`)
- CLI: `python -m wallet_tracker.cli` — annotate / rebuild / status (see `wallet_tracker/README.md`)
- Output: `data/tracked_wallets/{address}/...` per-wallet M/T profiles

Known boundary: fills that happened on the **v1** contract (before the 2026-04-28 migration) cannot be M/T-labeled by this v2-only dataset.

## Tests

Unit tests cover the data-correctness logic — the `OrderFilled` decode and side→asset mapping, the trade-labeling transform (price, BUY/SELL direction, USD/token amounts), the CLOB market row mapping, and the parsing helpers. They're pure and offline (no network, no API token needed).

```bash
uv run pytest
```

Tests live in `tests/`. They exercise the pure functions directly, so a regression like a flipped trade side or inverted price fails fast.

## Analysis

```python
import polars as pl
from poly_utils.utils import get_markets

markets_df = get_markets()           # parses clobTokenIds → token1/token2
trades_df = pl.read_csv("processed/trades.csv", try_parse_dates=True)

# Filter trades for a specific user (filter on `maker` — see note below)
USERS = {
    'domah': '0x9d84ce0306f8551e02efef1680475fc0f1dc1344',
    '50pence': '0x3cf3e8d5427aed066a7a5926980600f6c3cf87b3',
}
trader_df = trades_df.filter(pl.col("maker") == USERS['domah'])
```

**Note on user filtering**: After the exchange-as-taker legs are filtered out, both `maker` and `taker` are real users. A user who rests an order appears as `maker`; a user who aggressively takes appears as `taker`. To capture a user's *entire* trading activity, filter on `maker == addr OR taker == addr`.

## License

GPL-3.0 — see [LICENSE](LICENSE).
