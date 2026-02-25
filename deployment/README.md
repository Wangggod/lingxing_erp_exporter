# 部署文件说明

这个目录包含了将项目部署到生产环境（Mac mini）所需的所有配置文件和脚本。

## 📁 文件说明

### setup.sh
**快速部署脚本** - 在新机器上自动完成基础设置

运行此脚本会：
- 检查 Python 环境
- 创建虚拟环境
- 安装所有依赖
- 安装 Playwright 浏览器
- 创建配置文件模板
- 设置正确的文件权限

使用方法：
```bash
cd ~/Projects/playwright-scraper
./deployment/setup.sh
```

### com.lingxing.etl.plist
**launchd 定时任务配置文件**

这是 macOS 的定时任务配置文件，用于每天自动运行数据处理任务。

使用步骤：
1. 复制到 LaunchAgents 目录：
   ```bash
   cp deployment/com.lingxing.etl.plist ~/Library/LaunchAgents/
   ```

2. 编辑文件，替换所有 `YOUR_USERNAME` 为实际用户名：
   ```bash
   nano ~/Library/LaunchAgents/com.lingxing.etl.plist
   ```

3. 加载任务：
   ```bash
   launchctl load ~/Library/LaunchAgents/com.lingxing.etl.plist
   ```

4. 测试运行：
   ```bash
   launchctl start com.lingxing.etl
   ```

配置说明：
- `Hour`: 执行时间的小时（0-23）
- `Minute`: 执行时间的分钟（0-59）
- 默认：每天凌晨 2:00 执行

## 📖 完整部署指南

详细的部署步骤请参考项目根目录的 `DEPLOYMENT.md` 文件。

## 🔧 常用命令

### 查看定时任务状态
```bash
launchctl list | grep com.lingxing.etl
```

### 查看日志
```bash
tail -f ~/Library/Logs/lingxing-etl.log
```

### 停止定时任务
```bash
launchctl stop com.lingxing.etl
```

### 卸载定时任务
```bash
launchctl unload ~/Library/LaunchAgents/com.lingxing.etl.plist
```

### 重新加载定时任务
```bash
launchctl unload ~/Library/LaunchAgents/com.lingxing.etl.plist
launchctl load ~/Library/LaunchAgents/com.lingxing.etl.plist
```

## ⚠️ 注意事项

1. **配置文件路径**：确保 plist 文件中的所有路径都是绝对路径
2. **权限设置**：确保配置文件权限为 600（只有所有者可读写）
3. **日志目录**：确保 `~/Library/Logs` 目录存在
4. **虚拟环境**：确保虚拟环境路径正确

## 🆘 故障排查

如果定时任务没有执行：
1. 检查任务是否已加载：`launchctl list | grep lingxing`
2. 查看错误日志：`cat ~/Library/Logs/lingxing-etl-error.log`
3. 手动运行脚本测试：`./run_daily.sh`
4. 检查 plist 文件语法：`plutil ~/Library/LaunchAgents/com.lingxing.etl.plist`
