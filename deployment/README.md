# 部署指南

Mac mini 上部署 ETL 自动调度 + OpenClaw 巡检/日报。

## 架构

```
launchd (15:30/16:30)
  → run_daily.sh（ETL 6步流程）
    → 成功: webhook → OpenClaw 巡检群（确认数据） + 日报群（生成日报）
    → 失败: webhook → OpenClaw 巡检群（诊断+自愈）
    → 兜底: OpenClaw heartbeat 每30min 检查 ETL 是否已跑
```

## 快速部署

### 1. 基础环境

```bash
cd ~/Projects/lingxing_erp_exporter
./deployment/setup.sh
```

### 2. 配置 launchd 定时任务

```bash
# 编辑 plist，替换占位符（TOKEN、群ID）
nano deployment/com.lingxing.etl.plist

# 安装
cp deployment/com.lingxing.etl.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.lingxing.etl.plist

# 验证
launchctl list | grep lingxing
```

plist 同时注册 15:30 和 16:30 两个触发点，`run_daily.sh` 内部根据夏令时自动判断是否执行。

### 3. 配置 OpenClaw

#### 3.1 创建两个飞书群

| 群 | 用途 | 成员 |
|----|------|------|
| ETL巡检 | 系统运维，自愈告警 | 管理员 + bot |
| 数据日报 | 每日报告，团队查询 | 团队 + bot |

把 OpenClaw bot 拉进两个群，从日志获取 group ID（`oc_xxx`）。

#### 3.2 合并配置

将 `deployment/openclaw/openclaw-snippet.json5` 的内容合并到 `~/.openclaw/openclaw.json`，替换占位符：
- `YOUR_HOOK_TOKEN` → 自定义强密码
- `oc_PATROL_GROUP_ID` → 巡检群 ID
- `oc_REPORT_GROUP_ID` → 日报群 ID
- `ou_ADMIN_USER_ID` → 你的飞书 user ID

#### 3.3 部署 Agent 配置

**方案 A：巡检群复用 OpenClaw 默认 workspace**
```bash
# 巡检用 HEARTBEAT.md
cp deployment/openclaw/HEARTBEAT.md ~/.openclaw/workspace/HEARTBEAT.md

# 巡检用 AGENTS.md（追加或替换现有内容）
cp deployment/openclaw/patrol-AGENTS.md ~/.openclaw/workspace/AGENTS.md
```

**方案 B：日报群使用独立 workspace（推荐）**

OpenClaw 不同群可以绑定不同 workspace。在 openclaw.json 中为日报群指定独立目录：
```json5
groups: {
  "oc_REPORT_GROUP_ID": {
    requireMention: true,
    workspace: "~/.openclaw/workspace-report",
  },
}
```
然后：
```bash
mkdir -p ~/.openclaw/workspace-report
cp deployment/openclaw/report-AGENTS.md ~/.openclaw/workspace-report/AGENTS.md
```

#### 3.4 设置环境变量

在 `~/.openclaw/.env` 或 plist 的 EnvironmentVariables 中设置：
```
OPENCLAW_HOOK_TOKEN=your-secret-token
```

#### 3.5 重启 OpenClaw

```bash
openclaw gateway restart
openclaw gateway status
```

### 4. 测试

```bash
# 手动触发 ETL
launchctl start com.lingxing.etl

# 查看日志
tail -f ~/Library/Logs/lingxing-etl.log

# 手动测试 webhook
curl -X POST http://127.0.0.1:18789/hooks/agent \
  -H "Authorization: Bearer YOUR_HOOK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"message":"测试：请确认巡检功能正常。","sessionKey":"hook:etl-patrol","deliver":true,"channel":"feishu","to":"oc_PATROL_GROUP_ID"}'
```

## 文件说明

| 文件 | 用途 |
|------|------|
| `com.lingxing.etl.plist` | macOS launchd 定时任务 |
| `setup.sh` | 新机器快速部署脚本 |
| `openclaw/patrol-AGENTS.md` | 巡检群 Agent 配置（系统权限） |
| `openclaw/report-AGENTS.md` | 日报群 Agent 配置（只读查询） |
| `openclaw/HEARTBEAT.md` | 巡检心跳任务（兜底检查） |
| `openclaw/openclaw-snippet.json5` | OpenClaw 配置片段 |

## 常用运维命令

```bash
# 查看 launchd 状态
launchctl list | grep lingxing

# 停止/卸载
launchctl stop com.lingxing.etl
launchctl unload ~/Library/LaunchAgents/com.lingxing.etl.plist

# 重新加载
launchctl unload ~/Library/LaunchAgents/com.lingxing.etl.plist
launchctl load ~/Library/LaunchAgents/com.lingxing.etl.plist

# 查看日志
tail -f ~/Library/Logs/lingxing-etl.log
tail -f ~/Library/Logs/lingxing-etl-error.log

# OpenClaw 状态
openclaw gateway status
openclaw cron list
```

## 夏令时说明

plist 注册了 15:30 和 16:30 两个触发点。`run_daily.sh` 内部自动检测当前是否夏令时（PDT/PST），只在正确的时间执行。无需手动切换。
