#!/bin/bash

# 快速部署脚本 - 在新的 Mac mini 上运行此脚本

set -e

echo "=========================================="
echo "🚀 领星数据自动化系统 - 快速部署"
echo "=========================================="

# 获取项目目录
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "项目目录: $PROJECT_DIR"

# 1. 检查 Python
echo ""
echo "📝 检查 Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo "✅ $PYTHON_VERSION"
else
    echo "❌ Python 3 未安装"
    echo "请先安装 Python 3.9+："
    echo "  brew install python@3.9"
    exit 1
fi

# 2. 创建虚拟环境
echo ""
echo "📝 创建虚拟环境..."
if [ -d "venv" ]; then
    echo "⚠️  venv 已存在，跳过"
else
    python3 -m venv venv
    echo "✅ 虚拟环境创建完成"
fi

# 3. 激活虚拟环境
echo ""
echo "📝 激活虚拟环境..."
source venv/bin/activate

# 4. 安装依赖
echo ""
echo "📝 安装 Python 依赖..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ 依赖安装完成"

# 5. 安装 Playwright 浏览器
echo ""
echo "📝 安装 Playwright 浏览器..."
playwright install chromium
echo "✅ 浏览器安装完成"

# 6. 创建配置文件
echo ""
echo "📝 配置文件..."
if [ ! -f "config/config.json" ]; then
    cp config/config.example.json config/config.json
    echo "✅ 创建 config/config.json（请编辑此文件填入实际信息）"
else
    echo "⚠️  config/config.json 已存在，跳过"
fi

if [ ! -f "config/feishu.json" ]; then
    cp config/feishu.example.json config/feishu.json
    echo "✅ 创建 config/feishu.json（请编辑此文件填入实际信息）"
else
    echo "⚠️  config/feishu.json 已存在，跳过"
fi

if [ ! -f "config/bitable.json" ]; then
    cp config/bitable.example.json config/bitable.json
    echo "✅ 创建 config/bitable.json（请编辑此文件填入实际信息）"
else
    echo "⚠️  config/bitable.json 已存在，跳过"
fi

# 7. 设置配置文件权限
echo ""
echo "📝 设置配置文件权限..."
chmod 600 config/*.json
echo "✅ 权限设置完成"

# 8. 创建必要的目录
echo ""
echo "📝 创建目录..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p ~/Library/Logs
echo "✅ 目录创建完成"

# 9. 设置脚本可执行权限
echo ""
echo "📝 设置脚本权限..."
chmod +x run_daily.sh
echo "✅ 脚本权限设置完成"

# 10. 运行环境检查
echo ""
echo "📝 运行环境检查..."
python check_environment.py

# 11. 提示后续步骤
echo ""
echo "=========================================="
echo "✅ 基础部署完成！"
echo "=========================================="
echo ""
echo "📝 后续步骤："
echo ""
echo "1. 编辑配置文件（填入实际的账号密码和 API 密钥）："
echo "   nano config/config.json"
echo "   nano config/feishu.json"
echo "   nano config/bitable.json"
echo ""
echo "2. 测试运行："
echo "   source venv/bin/activate"
echo "   ./run_daily.sh"
echo ""
echo "3. 设置定时任务（可选）："
echo "   参考 DEPLOYMENT.md 中的步骤 4"
echo ""
echo "=========================================="
