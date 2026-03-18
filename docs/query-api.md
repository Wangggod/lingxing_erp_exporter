# 数据查询模块 (`scripts/query.py`)

统一的数据查询入口，传参数返回精确 JSON，只传必要数据。

## 调用方式

```bash
./venv/bin/python -m scripts.query <command> [options]
```

## 两种查询

### 1. summary — 汇总查询

数据源：`data/processed/{DATE}/daily_summary.json`，粒度为 **日期×国家×品名**。

```bash
# 最近7天所有产品的销量和利润
./venv/bin/python -m scripts.query summary --days 7 --fields 总销量,利润

# 指定项目组 + 国家
./venv/bin/python -m scripts.query summary --days 30 --groups 半开猫砂盆 --country 美国

# 指定日期范围
./venv/bin/python -m scripts.query summary --start 2026-03-01 --end 2026-03-10

# 多项目组
./venv/bin/python -m scripts.query summary --days 7 --groups 半开猫砂盆,欧博尔面包机 --fields 总销量,利润,TAcos

# 库存查询
./venv/bin/python -m scripts.query summary --days 1 --fields FBA可售,库龄_90天内,库龄_超365天
```

### 2. detail — 订单明细查询

数据源：`data/processed/{DATE}/feishu-ready/{product}/order_list_ready.csv`，粒度为 **单条订单**。

```bash
# 按订单号精确查找（自动扫描所有日期）
./venv/bin/python -m scripts.query detail --order 114-1607332-5352268

# 某产品最近3天已发货订单
./venv/bin/python -m scripts.query detail --groups 欧博尔面包机 --days 3 --status Shipped

# 只取指定字段（评论引导系统用）
./venv/bin/python -m scripts.query detail --groups 欧博尔面包机 --days 7 --fields 防重复编号,订单状态,ASIN
```

## 参数一览

| 参数 | 适用 | 说明 |
|------|------|------|
| `--days N` | 两者 | 最近 N 天（从最新可用日期往回推） |
| `--start YYYY-MM-DD` | 两者 | 起始日期 |
| `--end YYYY-MM-DD` | 两者 | 结束日期 |
| `--groups 组1,组2` | 两者 | 按项目组过滤，不传则返回所有 |
| `--product-name 品名` | 两者 | 按品名精确过滤（组内筛选） |
| `--country 国家` | 两者 | 按国家过滤（如 `美国`、`加拿大`） |
| `--fields 字段1,字段2` | 两者 | 只返回指定字段，大幅减少输出量 |
| `--order 订单号` | detail | 按防重复编号精确查找 |
| `--status 状态` | detail | 按订单状态过滤（`Shipped`、`Pending`） |

不传 `--days`/`--start`/`--end` 时默认返回最近 1 天。

## 输出格式

所有输出为 JSON，打印到 stdout：

```json
{
  "query": "summary",
  "date_range": ["2026-03-09", "2026-03-11"],
  "data": {
    "半开猫砂盆": [
      {"站点日期": "2026-03-09", "国家": "美国", "总销量": 8, "利润": 104.3},
      {"站点日期": "2026-03-10", "国家": "美国", "总销量": 5, "利润": 120.18}
    ],
    "欧博尔面包机": [
      {"站点日期": "2026-03-09", "国家": "美国", "总销量": 12, "利润": 530.0}
    ]
  }
}
```

- `date_range`：实际返回数据的日期范围 `[最早, 最晚]`
- `data`：按产品名分组，每个产品下是记录数组
- 使用 `--fields` 时，summary 会自动保留 `站点日期` 和 `国家` 两个维度字段

## summary 可用字段

**销售与利润**：站点日期、国家、品名、货币、总销量、FBM订单、FBA订单、广告单、总销售额、
优惠券订单数、优惠券折扣总额、实际销售额、总平台佣金、总FBA费、
总广告花费、今日退款数量、今日退款金额、FBM运费、
总采购成本、总头程成本、回款、利润、TAcos

**流量与广告效率**：Sessions、PV、CPC、广告CVR

**FBA库存（每日快照）**：FBA可售、FBA待调仓、FBA调仓中、
库龄\_90天内、库龄\_91到180天、库龄\_181到365天、库龄\_超365天

## 当前已注册产品

- 半开猫砂盆
- 拓疆毛巾桶
- 欧博尔面包机

## 典型场景

| 场景 | 命令 |
|------|------|
| 日报：今天所有产品概览 | `summary --days 1` |
| 周报：7天趋势 | `summary --days 7 --fields 总销量,利润,TAcos` |
| 单产品分析 | `summary --days 30 --groups 半开猫砂盆 --fields 总销量,总广告花费,利润,TAcos` |
| 库存概览 | `summary --days 1 --fields FBA可售,FBA调仓中,库龄_超365天` |
| 库存风险：超180天库龄 | `summary --days 1 --fields FBA可售,库龄_181到365天,库龄_超365天` |
| 查某笔订单 | `detail --order 114-1607332-5352268` |
| 评论引导：获取订单列表 | `detail --groups 欧博尔面包机 --days 7 --status Shipped --fields 防重复编号,ASIN` |
