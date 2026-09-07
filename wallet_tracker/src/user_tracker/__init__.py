"""user_tracker — 通用 Polymarket 地址交易采集/标注/查询.

模块:
  config : 配置加载 (config.toml)
  api    : data-api /activity 分窗抓取 (幂等去重)
  store  : 按钱包 jsonl 落盘 + parquet 重建
  annotate: 读 poly_data order_filled 标 M/T
  query  : 查询 CLI 逻辑
"""
