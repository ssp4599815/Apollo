#!/bin/bash

# Scrapy爬虫后台启动脚本
# 作者: Apollo
# 日期: $(date '+%Y-%m-%d')

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目路径配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
LOGS_DIR="$PROJECT_DIR/logs"
PID_DIR="$PROJECT_DIR/pids"
VENV_PATH="$HOME/Source/GitHub/Apollo/.venv"

# 创建必要的目录
mkdir -p "$LOGS_DIR"
mkdir -p "$PID_DIR"

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 显示帮助信息
show_help() {
    echo "Scrapy爬虫后台启动脚本"
    echo ""
    echo "用法: $0 [选项] [爬虫名称]"
    echo ""
    echo "选项:"
    echo "  start <spider>    启动指定爬虫"
    echo "  stop <spider>     停止指定爬虫"
    echo "  restart <spider>  重启指定爬虫"
    echo "  status [spider]   查看爬虫状态"
    echo "  logs <spider>     查看爬虫日志"
    echo "  list              列出所有可用爬虫"
    echo "  start-all         启动所有爬虫"
    echo "  stop-all          停止所有爬虫"
    echo "  clean             清理旧的日志和PID文件"
    echo "  help              显示此帮助信息"
    echo ""
    echo "可用爬虫:"
    echo "  chigua           吃瓜爬虫 - 视频下载"
    echo "  nungvl           nungvl爬虫 - 图片下载"
    echo "  sfnmt            sfnmt爬虫 - 图片下载"
    echo ""
    echo "示例:"
    echo "  $0 start chigua      # 启动吃瓜爬虫"
    echo "  $0 stop chigua       # 停止吃瓜爬虫"
    echo "  $0 status            # 查看所有爬虫状态"
    echo "  $0 logs chigua       # 查看吃瓜爬虫日志"
}

# 激活虚拟环境
activate_venv() {
    if [ -f "$VENV_PATH/bin/activate" ]; then
        source "$VENV_PATH/bin/activate"
        log_info "已激活虚拟环境: $VENV_PATH"
    else
        log_warn "虚拟环境未找到: $VENV_PATH"
        log_warn "将使用系统Python环境"
    fi
}

# 检查爬虫是否存在
check_spider_exists() {
    local spider_name="$1"
    local spider_file="$PROJECT_DIR/fuliji/spiders/${spider_name}.py"
    
    if [ ! -f "$spider_file" ]; then
        log_error "爬虫 '$spider_name' 不存在"
        return 1
    fi
    return 0
}

# 获取爬虫PID
get_spider_pid() {
    local spider_name="$1"
    local pid_file="$PID_DIR/${spider_name}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo "$pid"
            return 0
        else
            # PID文件存在但进程不存在，清理PID文件
            rm -f "$pid_file"
        fi
    fi
    return 1
}

# 启动单个爬虫
start_spider() {
    local spider_name="$1"
    
    if ! check_spider_exists "$spider_name"; then
        return 1
    fi
    
    # 检查是否已经在运行
    local existing_pid
    if existing_pid=$(get_spider_pid "$spider_name"); then
        log_warn "爬虫 '$spider_name' 已经在运行 (PID: $existing_pid)"
        return 1
    fi
    
    log_info "启动爬虫: $spider_name"
    
    # 激活虚拟环境
    activate_venv
    
    # 切换到项目目录
    cd "$PROJECT_DIR" || {
        log_error "无法切换到项目目录: $PROJECT_DIR"
        return 1
    }
    
    # 创建日志文件名
    local log_file="$LOGS_DIR/${spider_name}_$(date '+%Y%m%d_%H%M%S').log"
    local pid_file="$PID_DIR/${spider_name}.pid"
    
    # 启动爬虫
    nohup scrapy crawl "$spider_name" > "$log_file" 2>&1 &
    local spider_pid=$!
    
    # 保存PID
    echo "$spider_pid" > "$pid_file"
    
    # 等待一下确保启动成功
    sleep 2
    
    if ps -p "$spider_pid" > /dev/null 2>&1; then
        log_info "爬虫 '$spider_name' 启动成功 (PID: $spider_pid)"
        log_info "日志文件: $log_file"
    else
        log_error "爬虫 '$spider_name' 启动失败"
        rm -f "$pid_file"
        return 1
    fi
}

# 停止单个爬虫
stop_spider() {
    local spider_name="$1"
    
    local spider_pid
    if ! spider_pid=$(get_spider_pid "$spider_name"); then
        log_warn "爬虫 '$spider_name' 未在运行"
        return 1
    fi
    
    log_info "停止爬虫: $spider_name (PID: $spider_pid)"
    
    # 发送TERM信号
    kill "$spider_pid" 2>/dev/null
    
    # 等待进程结束
    local count=0
    while ps -p "$spider_pid" > /dev/null 2>&1 && [ $count -lt 10 ]; do
        sleep 1
        count=$((count + 1))
    done
    
    # 如果还在运行，强制杀死
    if ps -p "$spider_pid" > /dev/null 2>&1; then
        log_warn "进程未响应TERM信号，强制杀死"
        kill -9 "$spider_pid" 2>/dev/null
        sleep 1
    fi
    
    # 清理PID文件
    rm -f "$PID_DIR/${spider_name}.pid"
    
    if ! ps -p "$spider_pid" > /dev/null 2>&1; then
        log_info "爬虫 '$spider_name' 已停止"
    else
        log_error "无法停止爬虫 '$spider_name'"
        return 1
    fi
}

# 重启爬虫
restart_spider() {
    local spider_name="$1"
    
    log_info "重启爬虫: $spider_name"
    stop_spider "$spider_name"
    sleep 2
    start_spider "$spider_name"
}

# 查看爬虫状态
show_status() {
    local spider_name="$1"
    local spiders=("chigua" "nungvl" "sfnmt")
    
    if [ -n "$spider_name" ]; then
        spiders=("$spider_name")
    fi
    
    echo ""
    printf "%-15s %-10s %-10s %-s\n" "爬虫名称" "状态" "PID" "日志文件"
    echo "--------------------------------------------------------"
    
    for spider in "${spiders[@]}"; do
        local spider_pid
        local status="停止"
        local log_file="无"
        
        if spider_pid=$(get_spider_pid "$spider"); then
            status="运行中"
            # 查找最新的日志文件
            local latest_log=$(ls -t "$LOGS_DIR/${spider}_"*.log 2>/dev/null | head -n1)
            if [ -n "$latest_log" ]; then
                log_file=$(basename "$latest_log")
            fi
        else
            spider_pid="无"
        fi
        
        printf "%-15s %-10s %-10s %-s\n" "$spider" "$status" "$spider_pid" "$log_file"
    done
    echo ""
}

# 查看日志
show_logs() {
    local spider_name="$1"
    
    if [ -z "$spider_name" ]; then
        log_error "请指定要查看日志的爬虫名称"
        return 1
    fi
    
    # 查找最新的日志文件
    local latest_log=$(ls -t "$LOGS_DIR/${spider_name}_"*.log 2>/dev/null | head -n1)
    
    if [ -z "$latest_log" ]; then
        log_warn "未找到爬虫 '$spider_name' 的日志文件"
        return 1
    fi
    
    log_info "显示爬虫 '$spider_name' 的日志: $(basename "$latest_log")"
    echo "----------------------------------------"
    tail -f "$latest_log"
}

# 列出所有爬虫
list_spiders() {
    echo "可用的爬虫:"
    echo "  chigua  - 吃瓜爬虫 (视频下载)"
    echo "  nungvl  - nungvl爬虫 (图片下载)"
    echo "  sfnmt   - sfnmt爬虫 (图片下载)"
}

# 启动所有爬虫
start_all_spiders() {
    local spiders=("chigua" "nungvl" "sfnmt")
    
    log_info "启动所有爬虫..."
    for spider in "${spiders[@]}"; do
        start_spider "$spider"
        sleep 3  # 避免同时启动太多进程
    done
}

# 停止所有爬虫
stop_all_spiders() {
    local spiders=("chigua" "nungvl" "sfnmt")
    
    log_info "停止所有爬虫..."
    for spider in "${spiders[@]}"; do
        stop_spider "$spider"
    done
}

# 清理旧文件
clean_old_files() {
    log_info "清理旧的日志和PID文件..."
    
    # 清理7天前的日志文件
    find "$LOGS_DIR" -name "*.log" -mtime +7 -delete 2>/dev/null
    
    # 清理无效的PID文件
    for pid_file in "$PID_DIR"/*.pid; do
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if ! ps -p "$pid" > /dev/null 2>&1; then
                rm -f "$pid_file"
                log_info "已清理无效PID文件: $(basename "$pid_file")"
            fi
        fi
    done
    
    log_info "清理完成"
}

# 主函数
main() {
    case "$1" in
        "start")
            if [ -z "$2" ]; then
                log_error "请指定要启动的爬虫名称"
                show_help
                exit 1
            fi
            start_spider "$2"
            ;;
        "stop")
            if [ -z "$2" ]; then
                log_error "请指定要停止的爬虫名称"
                show_help
                exit 1
            fi
            stop_spider "$2"
            ;;
        "restart")
            if [ -z "$2" ]; then
                log_error "请指定要重启的爬虫名称"
                show_help
                exit 1
            fi
            restart_spider "$2"
            ;;
        "status")
            show_status "$2"
            ;;
        "logs")
            show_logs "$2"
            ;;
        "list")
            list_spiders
            ;;
        "start-all")
            start_all_spiders
            ;;
        "stop-all")
            stop_all_spiders
            ;;
        "clean")
            clean_old_files
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            if [ -z "$1" ]; then
                show_status
            else
                log_error "未知命令: $1"
                show_help
                exit 1
            fi
            ;;
    esac
}

# 检查是否在正确的目录
if [ ! -f "$PROJECT_DIR/scrapy.cfg" ]; then
    log_error "请在Scrapy项目根目录下运行此脚本"
    exit 1
fi

# 执行主函数
main "$@"
