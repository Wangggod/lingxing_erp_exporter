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
log "步骤 1/5: 下载数据"
log "----------------------------------------"
if python main.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 下载完成"
else
    error "下载失败"
    exit 1
fi

log "----------------------------------------"
log "步骤 2/5: 筛选产品数据"
log "----------------------------------------"
if python process.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 筛选完成"
else
    error "筛选失败"
    exit 1
fi

log "----------------------------------------"
log "步骤 3/5: 预处理数据"
log "----------------------------------------"
if python preprocess.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 预处理完成"
else
    error "预处理失败"
    exit 1
fi

log "----------------------------------------"
log "步骤 4/5: 聚合数据"
log "----------------------------------------"
if python aggregate.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 聚合完成"
else
    error "聚合失败"
    exit 1
fi

log "----------------------------------------"
log "步骤 5/5: 上传到飞书多维表格"
log "----------------------------------------"
if python upload_to_bitable.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
    log "✅ 上传完成"
else
    error "上传失败"
    exit 1
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

# 6. 完成
log "=========================================="
log "🎉 每日数据处理任务完成"
log "=========================================="

exit 0
