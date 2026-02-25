# 部署指南 - Mac mini 生产环境

## 📋 部署前准备

### 系统要求
- macOS 10.15+
- Python 3.9+
- 至少 10GB 可用磁盘空间
- 稳定的网络连接

### 需要迁移的文件
```
playwright-scraper/
├── scripts/          # 所有脚本文件 ✅
├── config/           # ⚠️ 包含敏感信息，需要单独处理
│   ├── config.json
│   ├── feishu.json
│   └── bitable.json
├── tools/            # 工具脚本 ✅
├── *.py              # 所有入口脚本 ✅
└── requirements.txt  # Python 依赖 ✅
```

## 🔧 步骤 1：在 Mac mini 上准备环境

### 1.1 克隆代码（或复制文件）
```bash
# 方式 1：如果使用 Git
cd ~/Projects  # 或你想要的目录
git clone <repository-url> playwright-scraper
cd playwright-scraper

# 方式 2：手动复制
# 将整个项目文件夹复制到 Mac mini
```

### 1.2 安装 Python（如果还没有）
```bash
# 检查 Python 版本
python3 --version

# 如果需要安装，推荐使用 Homebrew
brew install python@3.9
```

### 1.3 创建虚拟环境
```bash
cd ~/Projects/playwright-scraper
python3 -m venv venv
source venv/bin/activate
```

### 1.4 安装依赖
```bash
pip install --upgrade pip
pip install -r requirements.txt

# 安装 Playwright 浏览器
playwright install chromium
```

### 1.5 运行环境检查
```bash
python check_environment.py
```

## 🔐 步骤 2：配置敏感信息

### 2.1 创建配置文件
```bash
cd config/

# 复制示例配置
cp config.example.json config.json
cp feishu.example.json feishu.json
cp bitable.example.json bitable.json
```

### 2.2 填写配置信息
编辑 `config/config.json`：
```json
{
  "username": "你的领星账号",
  "password": "你的领星密码",
  ...
}
```

编辑 `config/feishu.json`：
```json
{
  "app_id": "你的飞书 app_id",
  "app_secret": "你的飞书 app_secret",
  ...
}
```

编辑 `config/bitable.json`：
```json
{
  "app_token": "你的多维表格 token",
  "table_id": "你的数据表 ID",
  ...
}
```

### 2.3 设置文件权限（重要！）
```bash
chmod 600 config/*.json  # 只有所有者可读写
```

## ⚙️ 步骤 3：测试运行

### 3.1 手动测试完整流程
```bash
# 确保在虚拟环境中
source venv/bin/activate

# 运行一键脚本
./run_daily.sh
```

### 3.2 检查结果
- 查看日志输出
- 检查飞书多维表格中的数据
- 确认 data/ 目录下生成了文件

## ⏰ 步骤 4：设置定时任务（launchd）

### 4.1 创建 launchd plist 文件
```bash
# 复制模板
cp deployment/com.lingxing.etl.plist.example ~/Library/LaunchAgents/com.lingxing.etl.plist

# 编辑文件，修改路径
nano ~/Library/LaunchAgents/com.lingxing.etl.plist
```

### 4.2 修改 plist 中的路径
将以下路径替换为实际路径：
- `WorkingDirectory`: 项目目录
- `Program`: run_daily.sh 的完整路径
- `StandardOutPath`: 日志文件路径
- `StandardErrorPath`: 错误日志路径

### 4.3 加载定时任务
```bash
# 加载任务
launchctl load ~/Library/LaunchAgents/com.lingxing.etl.plist

# 查看任务状态
launchctl list | grep com.lingxing.etl

# 立即运行一次测试
launchctl start com.lingxing.etl
```

### 4.4 设置执行时间
在 plist 文件中修改 `StartCalendarInterval`：
```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key>
    <integer>2</integer>     <!-- 凌晨 2 点 -->
    <key>Minute</key>
    <integer>0</integer>     <!-- 0 分 -->
</dict>
```

## 🧹 步骤 5：数据清理策略

### 5.1 自动清理旧数据
```bash
# 编辑 run_daily.sh，取消注释清理部分
# 或手动运行
python cleanup_old_data.py --days 30
```

### 5.2 设置清理策略
在 `cleanup_old_data.py` 中配置：
- 保留原始数据天数（默认 30 天）
- 保留处理后数据天数（默认 90 天）

## 📊 步骤 6：监控和维护

### 6.1 查看日志
```bash
# 查看最新日志
tail -f ~/Library/Logs/lingxing-etl.log

# 查看错误日志
tail -f ~/Library/Logs/lingxing-etl-error.log
```

### 6.2 手动运行
```bash
cd ~/Projects/playwright-scraper
source venv/bin/activate
./run_daily.sh
```

### 6.3 停止定时任务
```bash
# 临时停止
launchctl stop com.lingxing.etl

# 卸载任务
launchctl unload ~/Library/LaunchAgents/com.lingxing.etl.plist
```

## ⚠️ 故障排查

### 问题 1：定时任务没有执行
```bash
# 检查任务状态
launchctl list | grep com.lingxing.etl

# 查看系统日志
log show --predicate 'process == "launchd"' --last 1h | grep lingxing

# 重新加载任务
launchctl unload ~/Library/LaunchAgents/com.lingxing.etl.plist
launchctl load ~/Library/LaunchAgents/com.lingxing.etl.plist
```

### 问题 2：Python 环境问题
```bash
# 检查虚拟环境
which python
python --version

# 重新创建虚拟环境
rm -rf venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 问题 3：Playwright 浏览器问题
```bash
# 重新安装浏览器
playwright install --force chromium

# 检查浏览器路径
playwright install --help
```

### 问题 4：权限问题
```bash
# 确保脚本可执行
chmod +x run_daily.sh

# 确保配置文件权限正确
chmod 600 config/*.json

# 确保日志目录存在
mkdir -p ~/Library/Logs
```

## 🔒 安全建议

1. **配置文件**：
   - 不要提交到 Git（已在 .gitignore 中）
   - 设置正确的文件权限（600）
   - 定期更换密码

2. **日志文件**：
   - 定期清理旧日志
   - 不要在日志中记录敏感信息

3. **网络安全**：
   - 确保 Mac mini 防火墙开启
   - 只允许必要的网络连接

## 📝 维护清单

### 每周
- [ ] 检查日志，确认任务正常执行
- [ ] 检查飞书多维表格数据是否正常

### 每月
- [ ] 清理旧数据（如果没有自动清理）
- [ ] 检查磁盘空间使用情况
- [ ] 更新依赖包（可选）

### 每季度
- [ ] 检查并更新 Python 版本
- [ ] 检查并更新 Playwright 版本
- [ ] 审查和优化配置

## 🆘 联系和支持

如有问题，检查：
1. 日志文件（~/Library/Logs/lingxing-etl*.log）
2. 运行 `python check_environment.py`
3. 查看本文档的故障排查部分

## 📚 相关文件

- `check_environment.py` - 环境检查脚本
- `run_daily.sh` - 一键运行脚本
- `cleanup_old_data.py` - 数据清理脚本
- `deployment/com.lingxing.etl.plist.example` - launchd 配置模板
