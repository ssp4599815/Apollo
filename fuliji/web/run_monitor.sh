#!/bin/bash

# M3U8下载监控启动脚本

echo "🚀 启动M3U8下载监控..."

# 获取当前脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "📍 工作目录: $SCRIPT_DIR"
echo "🌐 启动Streamlit应用..."

# 启动Streamlit应用
cd "$SCRIPT_DIR" || exit
streamlit run monitor.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true \
    --server.fileWatcherType none \
    --browser.gatherUsageStats false

echo "✅ 监控应用已启动"
echo "🔗 访问地址: http://localhost:8501"
