"""
Polymarket CTF Exchange V2 OrderFilled event reader (HyperSync).

Streams OrderFilled logs from the CTF Exchange V2 contract on Polygon via
Envio's HyperSync and writes them to data/order_filled/part_*.parquet with
v1-compatible columns so process_live.py can stay close to its original form:

    timestamp, maker, makerAssetId, makerAmountFilled,
    taker, takerAssetId, takerAmountFilled, transactionHash, orderHash, fee

Rows are buffered and flushed as chunked Parquet files (ROWS_PER_PART per
file), which keeps memory bounded on multi-hundred-million-row backfills and
makes each file an atomic unit: a flush is written to a .tmp then os.replace'd
into place, so an interrupted run never leaves a half-written part. The cursor
records the block up to which all flushed parts are complete, plus the index of
the next part file to write.

HyperSync returns block timestamps inline with logs, so there's no separate
eth_getBlock pass. Cursor (last block scanned + part index) is persisted in
data/cursor_state.json.
"""

import asyncio
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

import hypersync
import polars as pl
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from eth_utils import keccak
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    LogField,
    LogSelection,
    Query,
    StreamConfig,
)

load_dotenv()

# Polymarket CTF Exchange V2 on Polygon (deployed 2026-03-31).
# Migration from v1 occurred on 2026-04-28.
CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"
V2_GENESIS_BLOCK = 84_902_353

# OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)
# 3 indexed params (orderHash, maker, taker) + 7 in data.
ORDERFILLED_TOPIC = (
    "0x"
    + keccak(
        text=(
            "OrderFilled(bytes32,address,address,uint8,uint256,"
            "uint256,uint256,uint256,bytes32,bytes32)"
        )
    ).hex()
)
_DATA_TYPES = [
    "uint8",
    "uint256",
    "uint256",
    "uint256",
    "uint256",
    "bytes32",
    "bytes32",
]

OUTPUT_DIR = "data"
PARTS_DIR = os.path.join(OUTPUT_DIR, "order_filled")
CURSOR_FILE = os.path.join(OUTPUT_DIR, "cursor_state.json")
# 链上新鲜度状态: 每次 update 结束写 order_filled 实际覆盖的最后时间,
# 供 user-trade-tracker 在更新钱包前判断"链上是否比钱包早"(避免把未覆盖
# 的新交易标成 NA 而不知情).
FRESH_FILE = os.path.join(OUTPUT_DIR, "order_filled_state.json")

# Reorg-safety buffer for Polygon.
CONFIRMATIONS = 20

DEFAULT_URL = "https://polygon.hypersync.xyz"

COLUMNS = [
    "timestamp",
    "maker",
    "makerAssetId",
    "makerAmountFilled",
    "taker",
    "takerAssetId",
    "takerAmountFilled",
    "transactionHash",
    "orderHash",
    "fee",
]

# Rows buffered in memory before one part parquet file is flushed.
# Set e.g. ORDER_PART_ROWS=200000 to lower peak RAM on constrained machines.
ROWS_PER_PART = int(os.environ.get("ORDER_PART_ROWS", "500000"))

_PART_RE = re.compile(r"^part_(\d+)\.parquet$")


def _part_path(part_index: int) -> str:
    """Path of the part file for a zero-based part index."""
    return os.path.join(PARTS_DIR, f"part_{part_index:08d}.parquet")


def _next_part_index() -> int:
    """Highest existing part index + 1 (used when cursor lacks part_index)."""
    best = 0
    for p in glob.glob(os.path.join(PARTS_DIR, "part_*.parquet")):
        m = _PART_RE.search(os.path.basename(p))
        if m:
            best = max(best, int(m.group(1)) + 1)
    return best


def _flush_part(rows: list, part_index: int) -> int:
    """Atomically write rows as one Parquet part file. Returns the file path.

    Written to <path>.tmp then os.replace'd so an interrupted run never leaves
    a partial part that the cursor could later believe is complete.
    """
    os.makedirs(PARTS_DIR, exist_ok=True)
    path = _part_path(part_index)
    tmp = path + ".tmp"
    schema = {
        c: pl.Utf8
        if c
        in (
            "maker",
            "makerAssetId",
            "taker",
            "takerAssetId",
            "transactionHash",
            "orderHash",
        )
        else pl.Int64
        for c in COLUMNS
    }
    df = pl.DataFrame(rows, schema=schema, orient="row")
    df.write_parquet(tmp, compression="zstd")
    os.replace(tmp, path)
    return path


def _load_cursor():
    """Return (last_block, part_index). part_index is the next part file index
    to write (0 if unknown). The cursor is only advanced after a part is fully
    flushed, so on resume the stream restarts at last_block and any rows in the
    un-flushed buffer are re-fetched exactly once (no duplicates)."""
    if os.path.isfile(CURSOR_FILE):
        try:
            with open(CURSOR_FILE) as f:
                state = json.load(f)
            last = state.get("last_block")
            if isinstance(last, int) and last >= V2_GENESIS_BLOCK:
                pi = state.get("part_index")
                return last, (pi if isinstance(pi, int) else _next_part_index())
        except Exception:
            pass
    return V2_GENESIS_BLOCK, _next_part_index()


def _save_cursor(next_block: int, part_index: int) -> None:
    """Persist the cursor atomically (write-temp + os.replace) so an interrupt
    mid-write can't corrupt it into a genesis re-backfill."""
    tmp = CURSOR_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_block": next_block, "part_index": part_index}, f)
    os.replace(tmp, CURSOR_FILE)


def write_freshness() -> None:
    """Write data/order_filled_state.json after an update run.

    Records the actual coverage window of order_filled:
      - start_time: 最老 part 的 min timestamp (= 链上数据绝对起点, 实测
        2026-04-03; 注意 04-03~04-22 基本空转(v1 期, 仅 ~1300 行), 实质数据
        自 ~2026-04-23 起)
      - last_time:  最新 part 的 max timestamp (= 已覆盖到哪, 链上新鲜度)
      - last_block: cursor
      - updated_at: 本次运行墙钟
    wallet_tracker 据此判断: 钱包活动早于 start_time 的交易 → 本地无任何覆盖
    可能(多为 v1/部署前成交), annotate 必为 NA; 晚于 last_time → 缝隙 NA, 需
    先 uv run poly-data 追平.
    """
    try:
        best = None
        for p in glob.glob(os.path.join(PARTS_DIR, "part_*.parquet")):
            m = _PART_RE.search(os.path.basename(p))
            if m:
                best = max(best if best is not None else 0, int(m.group(1)))
        if best is None:
            print("[freshness] 无 part 文件, 跳过写 order_filled_state.json")
            return
        path = _part_path(best)
        ts = pl.read_parquet(path, columns=["timestamp"])["timestamp"].max()
        last_ts = int(ts) if ts is not None else 0
        # 覆盖起点: 最早 part(part_00000000) 的 min timestamp (单文件, 秒级)
        first_ts = 0
        p0 = _part_path(0)
        if os.path.isfile(p0):
            try:
                t0 = pl.read_parquet(p0, columns=["timestamp"])["timestamp"].min()
                first_ts = int(t0) if t0 is not None else 0
            except Exception:
                first_ts = 0
        last_block = 0
        try:
            with open(CURSOR_FILE) as f:
                last_block = json.load(f).get("last_block", 0)
        except Exception:
            pass
        state = {
            "start_time_unix": first_ts,
            "start_time_utc": _fmt_ts(first_ts) if first_ts else "",
            "last_time_unix": last_ts,
            "last_time_utc": _fmt_ts(last_ts),
            "last_block": last_block,
            "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        tmp = FRESH_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, FRESH_FILE)
        print(
            f"[freshness] order_filled 覆盖 {state['start_time_utc']} → "
            f"{state['last_time_utc']} UTC (block {last_block:,}) → {FRESH_FILE}"
        )
    except Exception as e:  # freshness 是增强信息, 失败不阻断更新
        print(f"[freshness] 写入失败(忽略): {e}")


def _now() -> str:
    """Wall-clock timestamp prefix for progress logs (HH:MM:SS)."""
    return datetime.now().strftime("%H:%M:%S")


def _fmt_ts(unix_ts: int) -> str:
    """Format an on-chain block unix timestamp as a UTC datetime."""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _as_int(v):
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        return int(v, 16) if v.startswith("0x") else int(v)
    raise TypeError(f"unexpected numeric value: {v!r}")


def _hex_to_bytes(s: str) -> bytes:
    return bytes.fromhex(s.removeprefix("0x"))


def _topic_to_hex(v) -> str:
    """Normalize a HyperSync topic value to a lowercased 0x hex string.

    Topics may arrive as bytes or as (possibly 0x-prefixed) hex strings;
    addresses come left-padded to 32 bytes, which the caller trims.
    """
    if isinstance(v, (bytes, bytearray)):
        s = v.hex()
    else:
        s = str(v)
    if not s.startswith("0x"):
        s = "0x" + s
    return s.lower()


def _decode_log(log, ts_by_block: dict) -> list:
    """Decode one HyperSync Log into a v1-shaped CSV row."""
    # topics: [event_sig, orderHash, maker, taker]
    topics = log.topics
    order_hash = _topic_to_hex(topics[1])
    maker = "0x" + _topic_to_hex(topics[2])[-40:]
    taker = "0x" + _topic_to_hex(topics[3])[-40:]

    data_bytes = _hex_to_bytes(log.data)
    side, token_id, maker_amt, taker_amt, fee, _builder, _metadata = abi_decode(
        _DATA_TYPES, data_bytes
    )

    # V2 `side` reflects the MAKER order's side. BUY=0, SELL=1.
    # process_live treats "0" as USDC and any other id as an outcome token.
    if side == 0:
        maker_asset_id = "0"
        taker_asset_id = str(token_id)
    else:
        maker_asset_id = str(token_id)
        taker_asset_id = "0"

    bn = _as_int(log.block_number)
    tx_hash = log.transaction_hash
    if not tx_hash.startswith("0x"):
        tx_hash = "0x" + tx_hash

    return [
        ts_by_block[bn],
        maker,
        maker_asset_id,
        maker_amt,
        taker,
        taker_asset_id,
        taker_amt,
        tx_hash,
        order_hash,
        fee,
    ]


def _build_query(from_block: int, to_block: int) -> Query:
    return Query(
        from_block=from_block,
        to_block=to_block + 1,  # HyperSync to_block is exclusive
        logs=[
            LogSelection(
                address=[CTF_EXCHANGE_V2],
                topics=[[ORDERFILLED_TOPIC]],
            )
        ],
        field_selection=FieldSelection(
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
            log=[
                LogField.BLOCK_NUMBER,
                LogField.TRANSACTION_HASH,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.TOPIC3,
                LogField.DATA,
            ],
        ),
    )


async def _consume(
    receiver,
    start_block: int,
    part_index: int,
    end_block_exclusive: int,
) -> tuple[int, bool, int]:
    """Consume a HyperSync receiver, flushing Parquet parts and advancing the
    cursor. Safe on every exit path (clean end, Ctrl-C, exception).

    Returns ``(part_index, completed, total)`` where ``completed`` is True only
    if the stream ended normally (recv() returned None, i.e. we reached the end
    of the query range). On any other exit the cursor is persisted at
    ``last_next_block`` — the furthest batch boundary actually consumed — so a
    resume re-fetches at most the un-flushed tail. It never jumps to the chain
    tip on an interrupt, which would silently skip every remaining block.
    """
    total = 0
    first_ts = last_ts = None
    buffer: list = []
    last_next_block = start_block
    completed = False
    try:
        while True:
            res = await receiver.recv()
            if res is None:
                completed = True
                break

            last_next_block = _as_int(res.next_block)

            blocks = res.data.blocks or []
            logs = res.data.logs or []

            ts_by_block = {_as_int(b.number): _as_int(b.timestamp) for b in blocks}

            if logs:
                buffer.extend(_decode_log(log, ts_by_block) for log in logs)
                total += len(logs)

            # Flush in atomic whole-part units. The cursor is advanced only
            # after a part lands on disk, so a crash mid-buffer never duplicates
            # rows: on resume we restart at last_block and re-fetch the buffer.
            while len(buffer) >= ROWS_PER_PART:
                batch = buffer[:ROWS_PER_PART]
                del buffer[:ROWS_PER_PART]
                _flush_part(batch, part_index)
                part_index += 1
                _save_cursor(last_next_block, part_index)

            # Report the on-chain time we've reached (from block timestamps),
            # not the opaque block number.
            if ts_by_block:
                if first_ts is None:
                    first_ts = min(ts_by_block.values())
                last_ts = max(ts_by_block.values())
                reached = _fmt_ts(last_ts)
            else:
                reached = "       —             "
            print(
                f"[{_now()}]   reached {reached} UTC  block {last_next_block - 1:>10,}  "
                f"events: {len(logs):>5}  total: {total:,}  buffered: {len(buffer):,}"
            )
    finally:
        # Flush the tail (fewer than ROWS_PER_PART rows) if any.
        if buffer:
            _flush_part(buffer, part_index)
            part_index += 1
            print(
                f"[{_now()}] Flushed trailing {len(buffer):,} rows to part {part_index - 1:,}"
            )

        # Advance the cursor to the furthest block whose parts are durable.
        # On a clean run that is the query end (chain tip). On an interrupt it
        # is the last batch boundary we actually consumed — never the chain tip,
        # so the next run resumes exactly where we stopped.
        commit_block = end_block_exclusive if completed else last_next_block
        _save_cursor(commit_block, part_index)

    return part_index, completed, total


async def _run() -> None:
    if not os.path.isdir(PARTS_DIR):
        os.makedirs(PARTS_DIR)

    url = os.environ.get("POLYGON_HYPERSYNC_URL", DEFAULT_URL)
    token = os.environ.get("HYPERSYNC_API") or None
    if not token:
        raise RuntimeError(
            "HYPERSYNC_API is not set. HyperSync requires a bearer token "
            "(mandatory since 2025-11-03). Generate a free one with HyperSync "
            "product access at https://envio.dev/app/api-tokens and add it to "
            ".env as HYPERSYNC_API."
        )
    # 调大单请求超时: 默认超时太短时, 大响应(arrow 数据流)读到一半会被判
    # operation timed out(曾 0 logs 全失败); 调大后能持续拉。重试次数也可配。
    timeout_ms = int(os.environ.get("HYPERSYNC_TIMEOUT_MS", "300000"))
    n_retries = int(os.environ.get("HYPERSYNC_NUM_RETRIES", "10"))
    client = hypersync.HypersyncClient(
        ClientConfig(
            url=url,
            bearer_token=token,
            http_req_timeout_millis=timeout_ms,
            max_num_retries=n_retries,
        )
    )

    print(f"[{_now()}] HyperSync: {url} (with token)")

    height = await client.get_height()
    safe_height = height - CONFIRMATIONS
    start_block, part_index = _load_cursor()

    print(
        f"[{_now()}] Archive height: {height:,}  (safe: {safe_height:,} after {CONFIRMATIONS} confs)"
    )
    print(
        f"[{_now()}] Resuming from block {start_block:,}  (next part index {part_index:,})"
    )

    if start_block > safe_height:
        print(f"[{_now()}] Already up to date.")
        return

    # 分小段拉取: 单个 HyperSync query 若跨超大块范围, 本地网络读大响应会
    # operation timed out(曾把 ~32 万块做成单 query, 拉几千行就超时重试 15 次崩)。
    # 每段独立 query+stream, 段内失败按 cursor 续传重试, 段间自动续。
    # 可用 CHAIN_SEGMENT_BLOCKS / CHAIN_SEGMENT_RETRIES 调段大小与重试次数。
    # 段默认 3000 块: 每段约 19 万 logs / 5 分钟级, 单连接可完成且断点粒度小;
    # 段太大(如 2 万块)单段长时间拉取中途仍易超时。
    seg_blocks = int(os.environ.get("CHAIN_SEGMENT_BLOCKS", "3000"))
    retries = int(os.environ.get("CHAIN_SEGMENT_RETRIES", "6"))

    while True:
        cur_block, part_index = _load_cursor()
        if cur_block > safe_height:
            break
        end_block = min(cur_block + seg_blocks - 1, safe_height)
        print(
            f"[{_now()}] Segment blocks {cur_block:,} → {end_block:,}  "
            f"(next part index {part_index:,})"
        )
        completed = False
        total = 0
        for attempt in range(1, retries + 1):
            query = _build_query(cur_block, end_block)
            try:
                receiver = await client.stream(query, StreamConfig())
                part_index, completed, total = await _consume(
                    receiver, cur_block, part_index, end_block + 1
                )
                break
            except KeyboardInterrupt:
                # _consume 已在中断时落盘尾部并保存 cursor, 干净退出。
                print(f"\n[{_now()}] Interrupted. Cursor saved; resume will continue.")
                return
            except Exception as e:
                print(
                    f"[{_now()}] segment {cur_block:,}-{end_block:,} "
                    f"attempt {attempt}/{retries} failed: {type(e).__name__}: {e}"
                )
                if attempt >= retries:
                    print(
                        f"[{_now()}] segment failed after {retries} attempts; "
                        f"cursor saved at last consumed block — rerun to continue."
                    )
                    return
                # _consume 中断时已把 cursor 存到实际 consumed 的 batch 边界;
                # 重试从最新 cursor 续拉该段剩余部分。
                cur_block, part_index = _load_cursor()
                if cur_block > end_block:  # 该段其实已推完
                    completed = True
                    break
        # 安全阀: 成功返回但 cursor 无推进(理论不发生)时避免死循环。
        new_block, _ = _load_cursor()
        if not completed and new_block <= cur_block:
            print(f"[{_now()}] no progress on segment; aborting to avoid loop")
            return
    print(f"[{_now()}] Done. All segments processed into {PARTS_DIR}/")


def update_chain() -> None:
    """Sync entrypoint so the pipeline can call it directly."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(
            encoding="utf-8", errors="replace", line_buffering=True
        )  # live progress when piped; UTF-8 avoids GBK crashes
    try:
        asyncio.run(_run())
    finally:
        # 无论 完成/中断/已最新, 都刷新链上新鲜度状态文件(读新 part 单文件,
        # 秒级, 不扫全库).
        write_freshness()


if __name__ == "__main__":
    update_chain()
