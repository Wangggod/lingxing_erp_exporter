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

# ==================== 函数 ====================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a "$ERROR_LOG"
}

# ==================== 兜底机制 ====================

# 每日完成标记文件（按日期命名）
TODAY=$(date +%Y-%m-%d)
MARKER_FILE="$HOME/.lingxing_etl_${TODAY}.done"

# 自动判断美国太平洋时区是否夏令时，决定执行时间
# 夏令时 PDT (UTC-7)：美国日终 = 北京 15:00 → 15:30 执行
# 冬令时 PST (UTC-8)：美国日终 = 北京 16:00 → 16:30 执行
US_OFFSET=$(TZ="America/Los_Angeles" date +%z)  # -0700 or -0800
if [ "$US_OFFSET" = "-0700" ]; then
    RUN_HOUR=15
else
    RUN_HOUR=16
fi
RUN_MINUTE=30

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
    error "下载失败"
    exit 1
fi

log "----------------------------------------"
log "步骤 2/6: 筛选产品数据"
log "----------------------------------------"
if python process.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 筛选完成"
else
    error "筛选失败"
    exit 1
fi

log "----------------------------------------"
log "步骤 3/6: 预处理数据"
log "----------------------------------------"
if python preprocess.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 预处理完成"
else
    error "预处理失败"
    exit 1
fi

log "----------------------------------------"
log "步骤 4/6: 聚合数据"
log "----------------------------------------"
if python aggregate.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 聚合完成"
else
    error "聚合失败"
    exit 1
fi

log "----------------------------------------"
log "步骤 5/6: 上传到飞书多维表格"
log "----------------------------------------"
if python upload_to_bitable.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 上传完成"
else
    error "上传失败"
    exit 1
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

exit 0
