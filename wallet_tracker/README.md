# wallet_tracker (原 user-trade-tracker, 已并入 poly_data 作为子应用)

通用 Polymarket 用户交易记录采集器：通过官方 data-api 采集**任意指定地址**的
完整活动历史，利用 **poly_data** 主库（`data/order_filled`）做 M/T 标注，支持
增量更新与查询。数据按钱包分目录存储，**统一收在 poly_data 主项目的 data/ 下**。

自 v0.2 (2026-09-04) 起本项目迁入 `I:/plot/poly_data/wallet_tracker/`，与链上
主数据同仓库管理；链上新鲜度状态由 `uv run poly-data` 在每次链上更新结束时
写入 `data/order_filled_state.json`，钱包更新前据此判断"是否要先更链上"。

## 快速开始

```bash
cd /i/plot/poly_data/wallet_tracker
export PY=/i/plot/poly_data/.venv/Scripts/python.exe   # 与 poly_data 共用 venv

# 1) 登记并全量采集一个钱包
#    起点自动 clamp 到 v1 截止 2026-04-29(更早只能标 V1); 终点=链上覆盖点
$PY cli.py add 0x361528e242bc6cc789ac8da6fd5cb98046178fdf 2026-04-26

# 2) 增量更新所有已登记钱包(幂等, 自动补漏+重建parquet)
#    更新前自动检查链上新鲜度: 若 order_filled 比钱包旧 → 提示先跑 uv run poly-data
$PY cli.py update
$PY cli.py update --force        # 跳过新鲜度检查强制拉取(不推荐: 缝隙会标 NA)

# 3) M/T 标注(读 poly_data/data/order_filled, 过滤系统合约 0xe111)
$PY cli.py annotate 0x3615...

# 4) 查询
$PY cli.py query market --slug btc-updown-5m-1777247700 0x3615...
$PY cli.py query hash 0x8c0acc... 0x3615...
$PY cli.py query day 2026-04-26 0x3615...
$PY cli.py query daily 0x3615...
$PY cli.py list
```

## 数据布局 (统一在 poly_data 主项目 data/ 下)

```
poly_data/
  data/
    order_filled/                 # 链上 OrderFilled (poly-data 写, M/T 源)
    order_filled_state.json       # 链上新鲜度状态 (uv run poly-data 结束自动写)
    tracked_wallets/
      wallets.json                # 已登记钱包 {addr: {start, added}}
      0x3615...78fdf/
        activity.jsonl            # 原始全量(逐行活动, 增量 append)
        activity.parquet          # 查询用(含 utc_time/outcome_norm)
        activity_mt.parquet       # annotate 后(含 role: M/T/NA)
  wallet_tracker/
    cli.py / config.toml / src/   # 本应用
```

## 钱包采集窗口规则 (v0.3, 关键)

- **起点**: 自动 clamp 到 v1 截止(config `[annotation].v1_cutoff`, 默认
  `2026-04-29`) —— 更早的交易只能标 V1, add 直接跳过不拉(避免白拉废数据)
- **终点**: 对齐链上 order_filled 覆盖点(`order_filled_state.json.last_time`) ——
  钱包数据最多拉到链上点, **不比链上新 → annotate 无缝隙 NA**
  (api 支持精确到秒的截止, 不会把链上之后的活动拉进来)
- 链上推进后(order_filled_state 更新), 下次 update 自动续拉新覆盖部分

## 链上新鲜度机制 (两个数据源同步)

- `uv run poly-data` 的 chain 阶段结束(**无论完成/中断/已最新**)都会把
  order_filled 实际覆盖窗口写进 `data/order_filled_state.json`
  (`start_time`+`last_time` + last_block + updated_at), 读最新 part, 秒级。
- `cli.py update` 前调用 `freshness.check_fresh(wallet)`: 因钱包已截止到链上点,
  检查的是**链上是否停更超过 `stale_threshold_hours`(默认 6h)** —— 是则提示
  先 `uv run poly-data`(否则钱包卡在旧点拉不动); `--force` 可绕过。
  (注: 链上 archive 天然滞后 ~1h = reorg buffer, 属正常, 不触发拦截)
- 推荐顺序: `uv run poly-data` → `cli.py update` → `cli.py annotate`
  (annotate 幂等全量重扫, 链上追平后历史 NA 自动回填)。

## 数据口径
- `/activity` = 撮合碎片级(一笔大单拆多行); 官方"笔数"= `/trades` 订单聚合级
- role 判定: 读链上 OrderFilled 的 maker/taker, **忽略系统撮合合约 0xe111** 对手行
- **role 三类**:
  - `M`/`T` — 正常标注
  - `V1` — 早于 **v1→v2 迁移截止**（config `[annotation].v1_cutoff`, 默认
    `2026-04-29T00:00:00Z`）的交易: v1 期主撮合在 v1 合约(0x4bfb41d5...), 本库
    order_filled 只采 v2(0xe111...), 查不到 → **永久不标 M/T**(扫描直接跳过该区间)
  - `NA` — v1 截止之后仍无链上记录: 真缺口/缝隙(补链上数据后重跑 annotate 可回填)
- 参考: v2 合约 2026-04-03 部署但 04-23 前基本空转; 04-28 v1→v2 迁移(实测 ohioism
  4/26-28 共 15,944 笔标 V1, 4/29 起全部正常标注)。若日后确认某钱包迁移前真走 v2,
  把 `v1_cutoff` 调早(如 "2026-04-23T00:00:00Z")即可允许标注。

## 每钱包数据隔离
每个地址在 `data/tracked_wallets/{地址}/` 下独立存放, 互不混合;
采集/更新/标注/查询均只作用于目标钱包目录.

