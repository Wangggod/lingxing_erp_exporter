# 领星数据自动化处理系统

自动化下载领星（Lingxing）电商数据，处理后上传到飞书多维表格的完整 ETL 系统。

## 🎯 功能特性

- ✅ 自动登录领星并下载每日数据
- ✅ 按产品筛选和处理数据
- ✅ 数据预处理（添加编号、日期标准化）
- ✅ 按日期+国家维度聚合统计
- ✅ 自动上传到飞书多维表格
- ✅ 支持定时任务（macOS launchd）
- ✅ 完整的日志记录
- ✅ 数据自动清理

## 📊 数据流程

```
下载原始数据（main.py）
    ↓
筛选产品数据（process.py）
    ↓
预处理：添加编号和日期（preprocess.py）
    ↓
聚合：生成每日汇总表（aggregate.py）
    ↓
上传到飞书多维表格（upload_to_bitable.py）
```

## 🚀 快速开始

### 开发环境运行

```bash
# 1. 克隆或下载项目
cd ~/Projects/playwright-scraper

# 2. 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 3. 安装依赖
pip install -r requirements.txt
playwright install chromium

# 4. 配置
cp config/config.example.json config/config.json
cp config/feishu.example.json config/feishu.json
cp config/bitable.example.json config/bitable.json

# 编辑配置文件，填入实际信息
nano config/config.json
nano config/feishu.json
nano config/bitable.json

# 5. 运行
./run_daily.sh
```

### 生产环境部署

详见 [DEPLOYMENT.md](DEPLOYMENT.md)

快速部署（在新的 Mac mini 上）：
```bash
./deployment/setup.sh
```

## 🔧 常用命令

### 手动运行

```bash
# 完整流程
./run_daily.sh

# 或分步执行
python main.py              # 下载
python process.py           # 筛选
python preprocess.py        # 预处理
python aggregate.py         # 聚合
python upload_to_bitable.py # 上传
```

### 批量处理历史数据

```bash
# 处理指定日期范围
python batch_process.py --start 2026-02-01 --end 2026-02-22
```

### 数据清理

```bash
# 试运行（查看将删除什么）
python cleanup_old_data.py --days 30 --dry-run

# 实际清理（保留最近 30 天）
python cleanup_old_data.py --days 30
```

### 环境检查

```bash
python check_environment.py
```

## 📚 相关文档

- [DEPLOYMENT.md](DEPLOYMENT.md) - 详细部署指南
- [deployment/README.md](deployment/README.md) - 部署文件说明

## 🆘 支持

遇到问题？
1. 运行环境检查：`python check_environment.py`
2. 查看日志文件
3. 参考 DEPLOYMENT.md 的故障排查部分
