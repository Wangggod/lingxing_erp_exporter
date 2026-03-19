#!/bin/bash

# 领星数据自动化 - 每日运行脚本
# 用于定时任务或手动执行

set -e  # 遇到错误立即退出

# ==================== 配置 ====================

# 项目目录（改为实际路径）
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 日志目录
LOG_DIR="$HOME/Library/Logs"
LOG_FILE="$LOG_DIR/lingxing-etl.log"
ERROR_LOG="$LOG_DIR/lingxing-etl-error.log"

# 虚拟环境路径
VENV_DIR="$PROJECT_DIR/venv"

# 数据保留天数（可选，如果要自动清理）
DATA_RETENTION_DAYS=30

# OpenClaw webhook 配置
OPENCLAW_HOOK_URL="http://127.0.0.1:18789/hooks/agent"
OPENCLAW_HOOK_TOKEN="${OPENCLAW_HOOK_TOKEN:-}"  # 从环境变量读取，或在此填写
OPENCLAW_PATROL_GROUP="${OPENCLAW_PATROL_GROUP:-}"   # 巡检群 ID (oc_xxx)
OPENCLAW_REPORT_GROUP="${OPENCLAW_REPORT_GROUP:-}"   # 日报群 ID (oc_xxx)

# ==================== 函数 ====================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a "$ERROR_LOG"
}

notify_openclaw() {
    local session_key="$1"
    local target_group="$2"
    local message="$3"
    local agent_id="${4:-}"
    local model="${5:-}"

    if [ -z "$OPENCLAW_HOOK_TOKEN" ]; then
        log "⚠️ OPENCLAW_HOOK_TOKEN 未设置，跳过通知"
        return 0
    fi

    local extra_fields=""
    if [ -n "$agent_id" ]; then
        extra_fields="${extra_fields},\"agentId\":\"$agent_id\""
    fi
    if [ -n "$model" ]; then
        extra_fields="${extra_fields},\"model\":\"$model\""
    fi

    curl -s -o /dev/null -w "%{http_code}" -X POST "$OPENCLAW_HOOK_URL" \
        -H "Authorization: Bearer $OPENCLAW_HOOK_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"message\":\"$message\",\"sessionKey\":\"$session_key\",\"deliver\":true,\"channel\":\"feishu\",\"to\":\"$target_group\"$extra_fields}" || true
}

etl_fail() {
    local step="$1"
    error "$step 失败"
    if [ -n "$OPENCLAW_HOOK_TOKEN" ] && [ -n "$OPENCLAW_PATROL_GROUP" ]; then
        local last_lines
        last_lines=$(tail -20 "$ERROR_LOG" 2>/dev/null | tr '"' "'" | tr '\n' ' ')
        notify_openclaw "hook:etl-patrol" "$OPENCLAW_PATROL_GROUP" \
            "ETL 失败于步骤 [$step]（$(date '+%Y-%m-%d %H:%M')）。错误摘要: $last_lines。请分析原因并尝试自愈。" \
            "patrol" "openrouter/anthropic/claude-haiku-4-5"
    fi
    exit 1
}

# ==================== 兜底机制 ====================

# 每日完成标记文件（按日期命名）
TODAY=$(date +%Y-%m-%d)
MARKER_FILE="$HOME/.lingxing_etl_${TODAY}.done"

# 自动判断美国太平洋时区是否夏令时，决定执行时间
# 夏令时 PDT (UTC-7)：美国日终 = 北京 15:00 → 15:20 执行
# 冬令时 PST (UTC-8)：美国日终 = 北京 16:00 → 16:20 执行
US_OFFSET=$(TZ="America/Los_Angeles" date +%z)  # -0700 or -0800
if [ "$US_OFFSET" = "-0700" ]; then
    RUN_HOUR=15
else
    RUN_HOUR=16
fi
RUN_MINUTE=20

CURRENT_HOUR=$(date +%H)
CURRENT_MINUTE=$(date +%M)
if [ "$CURRENT_HOUR" -lt "$RUN_HOUR" ] || { [ "$CURRENT_HOUR" -eq "$RUN_HOUR" ] && [ "$CURRENT_MINUTE" -lt "$RUN_MINUTE" ]; }; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 当前时间早于 ${RUN_HOUR}:${RUN_MINUTE}（美区${US_OFFSET}），跳过" | tee -a "$LOG_FILE"
    exit 0
fi

# 今天已执行过，跳过
if [ -f "$MARKER_FILE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 今天已执行完成，跳过（标记: $MARKER_FILE）" | tee -a "$LOG_FILE"
    exit 0
fi

# 清理 7 天前的旧标记文件
find "$HOME" -maxdepth 1 -name ".lingxing_etl_*.done" -mtime +7 -delete 2>/dev/null

# ==================== 主流程 ====================

log "=========================================="
log "开始执行每日数据处理任务"
log "=========================================="

# 1. 进入项目目录
cd "$PROJECT_DIR" || {
    error "无法进入项目目录: $PROJECT_DIR"
    exit 1
}

# 2. 激活虚拟环境
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
    log "✅ 虚拟环境已激活"
else
    error "虚拟环境不存在: $VENV_DIR"
    exit 1
fi

# 3. 检查 Python
PYTHON_VERSION=$(python --version 2>&1)
log "Python 版本: $PYTHON_VERSION"

# 4. 执行 ETL 流程
log "----------------------------------------"
log "步骤 1/6: 下载数据"
log "----------------------------------------"
if python main.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 下载完成"
else
    etl_fail "下载数据"
fi

log "----------------------------------------"
log "步骤 2/6: 筛选产品数据"
log "----------------------------------------"
if python process.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 筛选完成"
else
    etl_fail "筛选产品数据"
fi

log "----------------------------------------"
log "步骤 3/6: 预处理数据"
log "----------------------------------------"
if python preprocess.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 预处理完成"
else
    etl_fail "预处理数据"
fi

log "----------------------------------------"
log "步骤 4/6: 聚合数据"
log "----------------------------------------"
if python aggregate.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 聚合完成"
else
    etl_fail "聚合数据"
fi

log "----------------------------------------"
log "步骤 5/6: 上传到飞书多维表格"
log "----------------------------------------"
if python upload_to_bitable.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 上传完成"
else
    etl_fail "上传到飞书"
fi

log "----------------------------------------"
log "步骤 6/6: 同步订单到 SellerGhost"
log "----------------------------------------"
if python sync_to_sellerghost.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ SellerGhost 同步完成"
else
    error "SellerGhost 同步失败（非关键步骤，继续）"
fi

# 5. 数据清理（可选）
# 取消注释以启用自动清理
# log "----------------------------------------"
# log "清理旧数据（保留 ${DATA_RETENTION_DAYS} 天）"
# log "----------------------------------------"
# if python cleanup_old_data.py --days $DATA_RETENTION_DAYS >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
#     log "✅ 清理完成"
# else
#     error "清理失败（非关键错误，继续）"
# fi

# 6. 写入今日完成标记
touch "$MARKER_FILE"

# 7. 完成
log "=========================================="
log "🎉 每日数据处理任务完成"
log "=========================================="

# ==================== OpenClaw 通知 ====================

# 通知巡检群：确认数据完整性
if [ -n "$OPENCLAW_PATROL_GROUP" ]; then
    log "通知巡检群..."
    notify_openclaw "hook:etl-patrol" "$OPENCLAW_PATROL_GROUP" \
        "ETL 每日任务已完成（$(date '+%Y-%m-%d %H:%M')）。请巡检数据完整性：检查 data/raw/$TODAY/ 下 6 份报表是否齐全，检查 data/processed/$TODAY/daily_summary.json 是否存在且非空。如有异常请告警。" \
        "patrol" "openrouter/anthropic/claude-haiku-4-5"
    log "✅ 巡检群已通知"
fi

# 通知日报群：生成并发送日报
if [ -n "$OPENCLAW_REPORT_GROUP" ]; then
    log "通知日报群..."
    notify_openclaw "hook:daily-report" "$OPENCLAW_REPORT_GROUP" \
        "ETL 数据已就绪。请执行 ./venv/bin/python -m scripts.query summary --days 1 获取今日全产品数据，按日报模板生成日报并发送。" \
        "report" "openrouter/anthropic/claude-sonnet-4-6"
    log "✅ 日报群已通知"
fi

exit 0
