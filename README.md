# QuantNote / Polymarket 15m Up-Down 监控

这个仓库主要用来监控 Polymarket 的 15 分钟 Up/Down 市场，并把每次轮询得到的数据持续写入 CSV。价格源使用 Chainlink Data Streams（stream），盘口使用 Polymarket CLOB（order book）。

目前支持币种：
- BTC
- ETH
- SOL
- XRP

## 环境要求

- Python 3.10+（建议 3.11/3.12）
- 依赖：`requests`（脚本使用标准库 + requests）

安装依赖：

```bash
pip install requests
```

## 主要脚本

### 1 监控脚本（推荐）

文件：[monitor_polymarket.py](https://github.com/weiuou/polymarket_15minupdown_monitor/blob/main/monitor_polymarket.py)

启动监控（只需要填币种）：

```bash
python monitor_polymarket.py --slug btc
python monitor_polymarket.py --slug eth
python monitor_polymarket.py --slug sol
python monitor_polymarket.py --slug xrp
```

运行逻辑：
- 主循环会自动轮转到“当前 15 分钟窗口”的市场 slug（例如 `xrp-updown-15m-<ts>`）
- Chainlink 价格由后台线程采集并缓存，主循环直接读取缓存
- 每次 Chainlink cache 新增数据会触发 CSV 回填（修正历史行并重算 Diff），这保证了即使没有chainlink stream亚秒级的数据流（需要花钱）也能保证数据的最终一致性，这对于分析并制定做市策略有一定帮助

## CSV 输出

每个币种输出一个 CSV（脚本自动创建）：
- `polymarket_btc_15m.csv`
- `polymarket_eth_15m.csv`
- `polymarket_sol_15m.csv`
- `polymarket_xrp_15m.csv`

CSV 表头（固定 10 列）：
- `Timestamp`：写入时间（UTC，ISO8601）
- `Time_Left_Sec`：距离到期秒数（可能为负，表示已到期）
- `Price_Stream`：来自 Chainlink stream 的价格（对应币种）
- `Strike_Price`：开盘定值（来自 stream，按 slug 时间点取值）
- `Diff`：`Price_Stream - Strike_Price`（保留 2 位小数）
- `Up_Price`：Polymarket “Up/Yes” 的价格（来自 Gamma/CLOB，CLOB 优先）
- `Best_Bid`：CLOB 最优买价
- `Best_Ask`：CLOB 最优卖价
- `Extra`：预留字段（目前为空；用于兼容/扩展）
- `Slug`：该行对应的市场 slug

## Strike / no_price_data 规则

脚本会对 strike 的时间偏移做保护：
- 基于 `slug` 的开盘时间 `...-<ts>`，从 stream 选择最近的点计算 `delta`
- 若 `delta > 1` 秒：
  - `Strike_Price` 写为 `no_price_data`
  - `Diff` 写为 `-`（不计算）

回填也遵守同样规则：若发现 delta 超过阈值，会把历史行改为 `no_price_data` 并清空 Diff。

## 常见问题

### 1 CSV 里为什么 Strike_Price 是no_price_data

这通常发生在已经开始的15min市场，但是stream并没有获取到开始时的价格数据，等到下一个15min市场开始就会正常显示了。

### 2 为什么 Diff 会“看起来不对”？

如果某一时刻写入时 strike 或 stream 点尚未就绪，可能先写入临时值或空值；后台线程会在后续 stream 更新时自动回填并重算 Diff，保证最终 CSV 一致。

## 参考：FeedId

脚本内置映射（来自 Chainlink Data Streams 页面），这个来自polymarket的规则，如果后续上了新的15min市场可以从pm的规则中获取Chainlink相关的链接：
- BTC：`0x00039d9e...75b8`
- ETH：`0x00036220...3ae9`
- SOL：`0x0003b778...c24f`
- XRP：`0x0003c16c...fc45`

