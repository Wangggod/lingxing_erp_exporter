# 领星数据自动化处理系统

## 项目概述
自动化下载领星（Lingxing）电商数据，处理后上传到飞书多维表格的完整 ETL 系统。

**当前状态**：单一产品测试阶段（半开猫砂盆）
**未来规划**：多产品支持 + 更多数据模块

## 核心数据流程 ✅

```
下载原始数据（main.py）
    ↓
筛选产品数据（process.py）
    ↓
预处理：添加编号和日期（preprocess.py）
    ↓
本地聚合：生成每日汇总表（aggregate.py）
    ↓
上传飞书多维表格（upload_to_bitable.py）✅
```

## 目录结构

```
playwright-scraper/
├── scripts/
│   ├── exporter.py         # 下载逻辑（Playwright）
│   ├── processor.py        # 产品数据筛选
│   ├── preprocessor.py     # 数据预处理
│   ├── aggregator.py       # 数据聚合
│   ├── uploader.py         # 飞书上传（普通表格，备份用）
│   ├── bitable_uploader.py # 飞书多维表格上传 ✅
│   ├── bitable_helper.py   # 多维表格 API 工具 ✅
│   ├── feishu_helper.py    # 飞书通用 API 工具
│   └── logger.py           # 日志工具
├── tools/
│   ├── create_bitable.py   # 创建多维表格工具
│   └── list_tables.py      # 列出多维表格数据表
├── data/
│   ├── raw/                # 原始下载数据（按日期）
│   │   └── YYYY-MM-DD/
│   │       ├── order_profit.xlsx
│   │       └── order_list.xlsx
│   └── processed/          # 处理后数据
│       └── YYYY-MM-DD/
│           ├── 半开猫砂盆/              # 筛选后
│           │   ├── order_profit.csv
│           │   └── order_list.csv
│           └── feishu-ready/           # 预处理后
│               └── 半开猫砂盆/
│                   ├── order_profit_ready.csv
│                   ├── order_list_ready.csv
│                   └── daily_summary.csv  # 聚合表（上传到多维表格）
├── config/
│   ├── config.json      # 领星账号配置
│   ├── feishu.json      # 飞书 API 配置
│   └── bitable.json     # 多维表格配置 ✅
├── main.py              # 入口1：下载
├── process.py           # 入口2：筛选
├── preprocess.py        # 入口3：预处理
├── aggregate.py         # 入口4：聚合
├── upload.py            # 入口5：上传到普通表格（备份）
└── upload_to_bitable.py # 入口6：上传到多维表格 ✅
```

## 关键设计决策

### 1. 数据过滤规则（重要！）
**所有统计都必须过滤：**
- 订单状态 ≠ "Canceled"
- 换货订单 ≠ "是"
- 部分指标额外过滤：是否退货 ≠ "是"

### 2. 预处理数据格式
- 第一列：防重复编号
  - order_list: 使用订单号
  - order_profit: 使用日期序号（如 20260223_001）
- 第二列：站点日期（ISO 格式：2026-02-23）
- 其他列保持原样

### 3. 聚合维度
按 **日期 + 国家** 聚合

### 4. 聚合表字段（16列）
**多维表格字段配置：**
1. 站点日期（日期类型）
2. 国家（文本）
3. 货币（文本）
4. 总销量（数字 - order_list: 数量求和）
5. FBM订单（数字 - 订单类型=MFN）
6. FBA订单（数字 - 订单类型=AFN）
7. 广告单（数字 - order_profit: 广告销量）
8. 总销售额（数字 - order_list: 单价求和，含优惠券前原价）
9. 优惠券订单数（数字 - 促销编码不为空）
10. 优惠券折扣总额（数字 - 各促销编码使用次数 × 面额，见下方说明）
11. 实际销售额（数字 - 总销售额 - 优惠券折扣总额）
12. 总平台佣金（数字 - order_list: 平台费绝对值求和，实际约11.25%）
13. 总FBA费（数字 - order_list: FBA费绝对值求和，仅AFN订单）
14. 总广告花费（数字 - order_profit: 广告花费绝对值）
15. 今日退款数量（数字 - order_profit: 退款量求和）
16. 今日退款金额（数字 - order_profit: 退款金额绝对值）
17. FBM运费（**手动填写** - 领星无第三方物流数据）

### 5. 飞书架构决策 ✅
- **普通表格**：可选的云备份（不限行数，目前未启用）
- **多维表格**：存储最终汇总数据（2000 行限制足够使用 3+ 年）
- **不使用**：普通表格 → 多维表格同步（日期格式问题）
- **采用方案**：本地聚合 → 直接写入多维表格 ✅

## 飞书配置

### config/feishu.json（应用认证）
```json
{
  "app_id": "cli_a914ced2fef95bdb",
  "app_secret": "jHYBrUBtMH7v8n8omwMWEed4zdTdPgYU",
  "spreadsheet_token": "TM5QsanmhhcQbJtEmi4cKY7snzc",
  "sheets": {
    "order_list": {
      "sheet_id": "b88577",
      "name": "领星-SC订单"
    },
    "order_profit": {
      "sheet_id": "KHKpax",
      "name": "领星-订单利润"
    }
  }
}
```

### config/bitable.json（多维表格配置）✅
```json
{
  "product": "半开猫砂盆",
  "app_token": "MsYxbyF7yak7TGsZwrgc3SWunSb",
  "table_id": "tbl94y30jp2DTTHu",
  "url": "https://kvwl7f2a7c.feishu.cn/base/MsYxbyF7yak7TGsZwrgc3SWunSb"
}
```

### 飞书应用权限要求
- ✅ `sheets:spreadsheet` - 读写普通表格
- ✅ `bitable:app` - 创建多维表格
- ✅ `bitable:record` - 读写多维表格记录

## 已实现功能 ✅

### 核心功能
1. ✅ 自动登录领星并下载数据（order_profit + order_list）
2. ✅ 按产品筛选数据
3. ✅ 添加防重复编号和站点日期
4. ✅ 数据聚合（日期-国家维度，16个字段）
5. ✅ 上传到飞书普通表格（备份用，可选）
6. ✅ 去重逻辑（基于订单号/站点日期）
7. ✅ 直接写入飞书多维表格（bitable API）
8. ✅ 完整的 ETL 数据流程

### 稳定性和可靠性（2026-02-25 重大改进）
9. ✅ **幂等性保证**（O(1) unique_key 方案）
10. ✅ **重试机制**（tenacity + exponential backoff）
11. ✅ **成功标记文件**（防止重复上传）
12. ✅ **速率限制处理**（429 自动重试）
13. ✅ **Upsert 逻辑**（查询→更新/创建）

### 费用估算增强（2026-03-07）
14. ✅ **优惠券折扣总额**：按促销编码分组，用同编码 Shipped 订单的 `促销费-商品折扣` 确定面额，Pending 订单往前回查最多4天
15. ✅ **实际销售额**：总销售额 - 优惠券折扣总额
16. ✅ **总FBA费**：直接用订单的 `FBA费` 字段（Pending 亦有值），仅 0 时才历史回查
17. ✅ **总平台佣金**：直接用订单的 `平台费` 字段（Pending 亦有值），仅 0 时才历史回查；实际佣金率约 11.25%（降佣后）

## 待实现功能 🚧

### 短期规划
1. ✅ ~~错误处理和重试机制~~ (已完成)
2. 🚧 完整的一键流程脚本（串联所有步骤）
3. 🚧 日志归档和清理

### 中期规划（多产品扩展）
1. 🚧 支持多产品并行处理
2. 🚧 产品配置文件（产品名、多维表格映射）
3. 🚧 多产品聚合结果隔离存储

### 长期规划（数据模块扩展）
1. 🚧 新数据源接入（如广告数据、库存数据等）
2. 🚧 数据归档策略（保留最近 3-6 个月）
3. 🚧 数据可视化集成

## 技术细节和注意事项

### 费用字段的 Pending vs Shipped 处理规则（重要！）

| 字段 | Pending 订单是否有值 | 处理策略 |
|------|---------------------|---------|
| `促销费-商品折扣` | **无（始终为0）** | 历史回查同促销编码的 Shipped 订单面额 |
| `FBA费` | **有**（Amazon 预填） | 直接用，仅为0时历史回查同MSKU Shipped订单 |
| `平台费` | **有**（Amazon 预填） | 直接用，仅为0时历史回查同MSKU Shipped订单 |

- 历史回查窗口：最多往前 4 天
- 平台佣金率：当前实际约 **11.25%**（降佣），最高不超过 15%，超过触发 WARNING
- FBM运费：领星无第三方物流数据，**需手动填写**

### 数据处理
- **日期格式**：统一使用 ISO 标准（YYYY-MM-DD）
- **日期字段上传**：多维表格日期字段需要 Unix 时间戳（毫秒）
- **数据类型**：
  - 防重复编号、站点日期：强制字符串
  - 数量、金额字段：保持数字类型
  - 空值处理：pandas NaN 需转换（fillna）
- **字段映射**：CSV 和多维表格字段名完全一致，无需映射

### 飞书 API
- **使用方式**：直接 HTTP 请求（lark-oapi SDK 部分方法缺失）
- **认证方式**：tenant_access_token（应用身份）
- **批量上传**：~~每批最多 500 条记录~~ 改为逐条 upsert
- **权限要求**：
  - sheets:spreadsheet（普通表格）
  - bitable:app（创建多维表格）
  - bitable:record（读写记录）

### 幂等性和 Upsert（重要！）
- **unique_key 字段**：格式 `YYYY-MM-DD|国家`（如 `2026-02-24|美国`）
- **查询方式**：使用 filter 精确查询（O(1)）
- **Upsert 流程**：
  1. 查询 unique_key 是否存在
  2. 存在则 PUT 更新
  3. 不存在则 POST 创建
- **性能**：从 O(N) 扫库查询提升到 O(1) 精确查询
- **重试**：使用 tenacity，最多 3 次，exponential backoff
- **成功标记**：创建 `.success` 文件防止重复上传

### 代码质量
- **Logger**：已修复重复初始化问题（handler 存在性检查）
- **错误处理**：API 调用需检查 response.code + 重试机制
- **文件路径**：统一使用 Path 对象，避免字符串拼接
- **依赖**：tenacity>=9.0.0（重试库）

## 快速命令

```bash
# 完整流程（下载 → 处理 → 上传到多维表格）✅
python main.py && \
python process.py && \
python preprocess.py && \
python aggregate.py && \
python upload_to_bitable.py

# 单步测试
python main.py              # 1. 下载原始数据
python process.py           # 2. 筛选产品数据
python preprocess.py        # 3. 预处理（添加编号和日期）
python aggregate.py         # 4. 聚合为每日汇总
python upload_to_bitable.py # 5. 上传到多维表格 ✅
python upload.py            # (可选) 上传到普通表格备份

# 工具脚本
python tools/create_bitable.py  # 创建新的多维表格
python tools/list_tables.py     # 列出多维表格中的数据表
```

## 多产品扩展准备

### 当前单产品配置
- 产品：半开猫砂盆
- 多维表格：MsYxbyF7yak7TGsZwrgc3SWunSb
- 数据表：tbl94y30jp2DTTHu

### 未来多产品架构设想
```python
# config/products.json（待实现）
{
  "半开猫砂盆": {
    "app_token": "MsYxbyF7yak7TGsZwrgc3SWunSb",
    "table_id": "tbl94y30jp2DTTHu"
  },
  "产品B": {
    "app_token": "...",
    "table_id": "..."
  }
}
```

### 扩展要点
1. **数据隔离**：每个产品有独立的多维表格或数据表
2. **并行处理**：支持同时处理多个产品的数据
3. **配置驱动**：产品列表和映射关系通过配置文件管理
4. **代码复用**：核心处理逻辑保持通用，通过参数传递产品信息

## 相关文档

- [聚合逻辑详解](./aggregation-logic.md) - 数据聚合的详细规则和过滤条件
- [多产品架构设计](./multi-product-architecture.md) - 扩展到多产品的架构方案
- [幂等性修复](./idempotency-fix.md) - O(N)→O(1) 性能优化和可靠性改进（2026-02-25）

## 相关链接

- 多维表格（半开猫砂盆）：https://kvwl7f2a7c.feishu.cn/base/MsYxbyF7yak7TGsZwrgc3SWunSb
- 飞书普通表格（备份）：https://kvwl7f2a7c.feishu.cn/sheets/TM5QsanmhhcQbJtEmi4cKY7snzc
- 项目路径：/Users/wangggod/claude-lab/playwright-scraper
- Git 分支：feature/table-dispatcher

## 项目状态总结

✅ **已完成**：
- 完整的单产品 ETL 流程（下载 → 筛选 → 预处理 → 聚合 → 上传）
- 飞书多维表格集成（直接写入，无需中间层）
- 数据质量保证（过滤、去重、类型转换）
- **幂等性和可靠性**（2026-02-25）：
  - ✅ O(1) unique_key 查询（性能提升 20 倍）
  - ✅ 重试机制（3次 + exponential backoff）
  - ✅ 成功标记文件（防止重复上传）
  - ✅ 速率限制处理（429 自动重试）

🚧 **准备中**：
- 多产品并行处理架构
- 更多数据模块接入
- 自动化调度和监控（launchd/cron）

🎯 **生产就绪度**：95%
- 核心功能完整稳定
- 性能和可靠性已达生产级别
- 待部署到 Mac mini 进行长期验证
