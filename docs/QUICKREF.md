# 快速参考 - 常用命令和注意事项

## 日常运行命令

### 手动运行完整流程
```bash
# 进入项目目录
cd ~/Projects/playwright-scraper
source venv/bin/activate

# 方式1：一键运行
./run_daily.sh

# 方式2：分步运行
python main.py              # 下载
python process.py           # 筛选
python preprocess.py        # 预处理
python aggregate.py         # 聚合
python upload_to_bitable.py # 上传
```

### 上传相关命令
```bash
# 正常上传（有幂等性保护，不会重复）
python upload_to_bitable.py

# 强制重新上传（忽略已上传标记）
python upload_to_bitable.py --force

# 指定日期上传
python upload_to_bitable.py --date 2026-02-24

# 组合使用
python upload_to_bitable.py --date 2026-02-24 --force
```

### 批量处理历史数据
```bash
# 处理指定日期范围
python batch_process.py --start 2026-02-01 --end 2026-02-22

# 指定产品（默认：半开猫砂盆）
python batch_process.py --start 2026-02-01 --end 2026-02-22 --product "半开猫砂盆"
```

### 数据清理
```bash
# 试运行（查看将删除什么，不实际删除）
python cleanup_old_data.py --days 30 --dry-run

# 实际清理（保留最近 30 天）
python cleanup_old_data.py --days 30

# 分别设置原始数据和处理后数据的保留天数
python cleanup_old_data.py --raw-days 30 --processed-days 90
```

## 定时任务管理

### macOS launchd
```bash
# 查看任务状态
launchctl list | grep com.lingxing.etl

# 手动触发
launchctl start com.lingxing.etl

# 停止任务
launchctl stop com.lingxing.etl

# 重新加载配置
launchctl unload ~/Library/LaunchAgents/com.lingxing.etl.plist
launchctl load ~/Library/LaunchAgents/com.lingxing.etl.plist

# 查看日志
tail -f ~/Library/Logs/lingxing-etl.log
tail -f ~/Library/Logs/lingxing-etl-error.log
```

## 重要文件路径

### 配置文件
```
config/config.json      - 领星账号配置
config/feishu.json      - 飞书 API 配置
config/bitable.json     - 多维表格配置
```

### 数据文件
```
data/raw/YYYY-MM-DD/                           - 原始下载数据
data/processed/YYYY-MM-DD/半开猫砂盆/            - 筛选后数据
data/processed/YYYY-MM-DD/feishu-ready/半开猫砂盆/ - 预处理后数据
  ├── order_profit_ready.csv
  ├── order_list_ready.csv
  ├── daily_summary.csv                        - 聚合表（上传这个）
  └── daily_summary.success                    - 成功标记
```

### 日志文件
```
~/Library/Logs/lingxing-etl.log        - 标准日志
~/Library/Logs/lingxing-etl-error.log  - 错误日志
```

## 关键概念

### 幂等性保证
- **unique_key 字段**：`YYYY-MM-DD|国家`（如 `2026-02-24|美国`）
- **查询方式**：filter 精确查询（O(1)）
- **工作原理**：
  1. 查询 unique_key 是否存在
  2. 存在 → 更新
  3. 不存在 → 创建
- **重复上传保护**：.success 文件

### 数据聚合维度
- **颗粒度**：日期 + 国家
- **示例**：`2026-02-24 + 美国` 聚合为一行

### 过滤规则（重要）
**所有统计都必须过滤**：
- 订单状态 ≠ "Canceled"
- 换货订单 ≠ "是"

**销售额和优惠券额外过滤**：
- 是否退货 ≠ "是"

## 常见问题

### Q1: 如何确认数据已成功上传？
```bash
# 方法1：检查 .success 文件
ls -la data/processed/2026-02-24/feishu-ready/半开猫砂盆/daily_summary.success

# 方法2：查看日志
tail ~/Library/Logs/lingxing-etl.log | grep "上传完成"

# 方法3：访问飞书多维表格
# https://kvwl7f2a7c.feishu.cn/base/MsYxbyF7yak7TGsZwrgc3SWunSb
```

### Q2: 重复运行会产生重复数据吗？
**不会**！系统有双重保护：
1. .success 文件检查（跳过已上传）
2. unique_key upsert（更新而非创建）

### Q3: 如何重新上传某一天的数据？
```bash
# 方式1：删除 .success 文件后正常上传
rm data/processed/2026-02-24/feishu-ready/半开猫砂盆/daily_summary.success
python upload_to_bitable.py --date 2026-02-24

# 方式2：使用 --force 参数
python upload_to_bitable.py --date 2026-02-24 --force
```

### Q4: 定时任务没有执行怎么办？
```bash
# 1. 检查任务是否加载
launchctl list | grep com.lingxing.etl

# 2. 查看系统日志
log show --predicate 'process == "launchd"' --last 1h | grep lingxing

# 3. 手动触发测试
launchctl start com.lingxing.etl

# 4. 检查 plist 文件语法
plutil ~/Library/LaunchAgents/com.lingxing.etl.plist

# 5. 重新加载
launchctl unload ~/Library/LaunchAgents/com.lingxing.etl.plist
launchctl load ~/Library/LaunchAgents/com.lingxing.etl.plist
```

### Q5: 如何修改定时执行时间？
编辑 plist 文件：
```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key>
    <integer>2</integer>     <!-- 凌晨 2 点 -->
    <key>Minute</key>
    <integer>0</integer>     <!-- 0 分 -->
</dict>
```
然后重新加载配置。

## 性能指标

### 正常运行耗时（参考）
- 下载数据：~30s
- 筛选 + 预处理：~2s
- 聚合：~1s
- 上传（2条记录）：~1s
- **总耗时**：~35s

### 批量处理（22天）
- 总耗时：~15分钟
- 平均每天：~40s

## 紧急联系

### 项目文档
- `MEMORY.md` - 项目总览
- `DEPLOYMENT.md` - 部署指南
- `idempotency-fix.md` - 幂等性修复详解
- `aggregation-logic.md` - 聚合逻辑
- `QUICKREF.md` - 本文档

### 在线资源
- 多维表格：https://kvwl7f2a7c.feishu.cn/base/MsYxbyF7yak7TGsZwrgc3SWunSb
- 飞书开放平台文档：https://open.feishu.cn/document/home

## 最后更新
2026-02-25 - 添加幂等性和重试机制
