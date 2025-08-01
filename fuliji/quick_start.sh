#!/bin/bash

# 快速启动脚本
# 用于快速启动单个爬虫

# 项目配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="$HOME/Source/GitHub/Apollo/.venv"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# 显示用法
if [ $# -eq 0 ]; then
    echo "用法: $0 <spider_name>"
    echo "可用爬虫: chigua, nungvl, sfnmt"
    echo ""
    echo "示例:"
    echo "  $0 chigua    # 启动吃瓜爬虫"
    echo "  $0 nungvl    # 启动nungvl爬虫"
    echo "  $0 sfnmt     # 启动sfnmt爬虫"
    exit 1
fi

SPIDER_NAME="$1"

# 激活虚拟环境
if [ -f "$VENV_PATH/bin/activate" ]; then
    source "$VENV_PATH/bin/activate"
    echo -e "${GREEN}✓ 已激活虚拟环境${NC}"
else
    echo -e "${RED}⚠ 虚拟环境未找到，使用系统Python${NC}"
fi

# 切换到项目目录
cd "$SCRIPT_DIR" || {
    echo -e "${RED}✗ 无法切换到项目目录${NC}"
    exit 1
}

# 创建日志目录
mkdir -p logs

# 启动爬虫
echo -e "${GREEN}🚀 启动爬虫: $SPIDER_NAME${NC}"
echo "按 Ctrl+C 停止爬虫"
echo "----------------------------------------"

# 直接运行，不放到后台
scrapy crawl "$SPIDER_NAME"
