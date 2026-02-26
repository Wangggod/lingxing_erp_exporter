# 多产品架构设计

## 当前状态
- **测试阶段**：单一产品（半开猫砂盆）
- **已验证**：完整的 ETL 流程（下载 → 处理 → 上传）

## 扩展需求

### 1. 多产品支持
- 同时处理多个产品的数据
- 每个产品独立的数据存储和多维表格
- 产品列表可配置化管理

### 2. 更多数据模块
- 当前模块：订单数据（order_list + order_profit）
- 潜在模块：
  - 广告数据（广告花费、ROI、关键词等）
  - 库存数据（FBA 库存、在途库存等）
  - 评论数据（星级、内容分析等）
  - 退货数据（退货原因、趋势分析等）

## 架构设计方案

### 方案 A：产品维度隔离

**目录结构**：
```
data/
├── products/
│   ├── 半开猫砂盆/
│   │   ├── raw/
│   │   │   └── 2026-02-23/
│   │   │       ├── order_profit.xlsx
│   │   │       └── order_list.xlsx
│   │   └── processed/
│   │       └── 2026-02-23/
│   │           └── daily_summary.csv
│   └── 产品B/
│       └── ...
```

**配置文件**（config/products.json）：
```json
{
  "products": [
    {
      "name": "半开猫砂盆",
      "enabled": true,
      "bitable": {
        "app_token": "MsYxbyF7yak7TGsZwrgc3SWunSb",
        "table_id": "tbl94y30jp2DTTHu"
      }
    },
    {
      "name": "产品B",
      "enabled": true,
      "bitable": {
        "app_token": "...",
        "table_id": "..."
      }
    }
  ]
}
```

**优点**：
- 数据完全隔离，不会混淆
- 每个产品独立管理
- 便于并行处理

**缺点**：
- 目录结构较深
- 需要遍历产品列表

### 方案 B：日期维度优先

**目录结构**：
```
data/
├── raw/
│   └── 2026-02-23/
│       ├── order_profit.xlsx      # 全公司数据
│       └── order_list.xlsx
└── processed/
    └── 2026-02-23/
        ├── 半开猫砂盆/
        │   └── daily_summary.csv
        └── 产品B/
            └── daily_summary.csv
```

**优点**：
- 原始数据只下载一次
- 按产品筛选后再分别处理
- 节省下载时间和存储空间

**缺点**：
- 原始数据混合所有产品

### 推荐方案：方案 B（当前已实现）

**理由**：
1. 领星下载的是全公司数据，按产品下载不现实
2. 当前架构已经是方案 B，无需重构
3. 产品配置通过循环处理即可扩展

## 实现步骤

### 第一步：配置化产品列表
创建 `config/products.json`：
```json
{
  "products": [
    {
      "name": "半开猫砂盆",
      "enabled": true,
      "bitable_config": {
        "app_token": "MsYxbyF7yak7TGsZwrgc3SWunSb",
        "table_id": "tbl94y30jp2DTTHu"
      }
    }
  ]
}
```

### 第二步：修改处理脚本
```python
# process.py 改造示例
def main():
    DATE = "2026-02-23"

    # 读取产品配置
    products_config = load_products_config()

    for product in products_config["products"]:
        if not product["enabled"]:
            continue

        product_name = product["name"]
        log.info(f"处理产品: {product_name}")

        # 筛选
        filter_by_product(DATE, product_name)

        # 预处理
        preprocess_product(DATE, product_name)

        # 聚合
        aggregate_product(DATE, product_name)

        # 上传
        upload_to_bitable(DATE, product_name, product["bitable_config"])
```

### 第三步：并行处理（可选）
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = []
    for product in products_config["products"]:
        if product["enabled"]:
            future = executor.submit(process_product, DATE, product)
            futures.append(future)

    # 等待所有任务完成
    for future in futures:
        future.result()
```

## 数据模块扩展

### 模块化设计
```python
# 模块注册表
MODULES = {
    "orders": {
        "reports": ["order_profit", "order_list"],
        "processor": "process_orders",
        "aggregator": "aggregate_orders"
    },
    "ads": {
        "reports": ["ad_performance", "search_terms"],
        "processor": "process_ads",
        "aggregator": "aggregate_ads"
    }
}
```

### 扩展新模块
1. 在领星中配置新的报告下载
2. 添加对应的处理器（processor）
3. 添加对应的聚合器（aggregator）
4. 创建新的多维表格或数据表
5. 更新配置文件

## 注意事项

### 性能考虑
- 多产品并行处理：注意飞书 API 调用频率限制
- 大数据量：考虑分批上传（当前已支持 500 条/批）
- 下载时间：Playwright 下载速度受网络影响，不建议并行

### 数据一致性
- 同一天的数据应该原子化处理
- 失败重试机制
- 日志记录每个产品的处理状态

### 配置管理
- 产品配置集中管理（products.json）
- 每个产品的多维表格配置独立
- 支持启用/禁用产品

## 未来优化方向

1. **调度系统**：定时自动执行（cron / Airflow）
2. **错误通知**：处理失败时发送飞书消息通知
3. **数据校验**：上传前检查数据完整性
4. **增量更新**：只上传新数据，避免重复
5. **监控面板**：可视化展示处理状态和数据质量
