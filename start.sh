#!/bin/bash
#
# Weather-Quant 一键启动脚本
#
# 用法:
#   ./start.sh          启动后端 + 前端（默认）
#   ./start.sh --api    仅启动后端 API（端口 8000）
#   ./start.sh --dev    仅启动前端开发服务器（端口 5173）
#   ./start.sh --build  构建前端 + 启动后端（生产模式）
#   ./start.sh --stop   停止所有进程
#

set -e

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PID_FILE="$ROOT_DIR/.running_pids"

log() { echo -e "${GREEN}[WQ]${NC} $1"; }
warn() { echo -e "${YELLOW}[WQ]${NC} $1"; }
err() { echo -e "${RED}[WQ]${NC} $1"; }

# ── 停止所有进程 ──
stop_all() {
    if [ -f "$PID_FILE" ]; then
        while read -r pid name; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null && log "停止 $name (PID $pid)"
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    # 也清理可能残留的进程
    pkill -f "uvicorn.*8000" 2>/dev/null || true
    pkill -f "vite.*5173" 2>/dev/null || true
    log "所有进程已停止"
}

# ── 依赖检查 ──
check_deps() {
    # Python
    if ! command -v python3 &>/dev/null; then
        err "需要 python3，请先安装"
        exit 1
    fi

    # 检查 Python 依赖
    if ! python3 -c "import fastapi, uvicorn, httpx, scipy" 2>/dev/null; then
        warn "正在安装 Python 依赖..."
        pip3 install -r requirements.txt -q
    fi

    # Node.js (前端)
    if ! command -v node &>/dev/null; then
        warn "未检测到 Node.js，前端将不可用"
        return 1
    fi

    # 检查前端依赖
    if [ ! -d "frontend/node_modules" ]; then
        warn "正在安装前端依赖..."
        (cd frontend && npm install --silent)
    fi

    return 0
}

# ── 启动后端 ──
start_backend() {
    log "启动后端 API (端口 8000)..."
    python3 main.py --web &
    local pid=$!
    echo "$pid backend" >> "$PID_FILE"

    # 等待后端就绪
    for i in $(seq 1 15); do
        if curl -s http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
            log "后端已就绪 ${CYAN}http://localhost:8000${NC}"
            return 0
        fi
        sleep 1
    done
    warn "后端启动中（可能需要更多时间加载数据）"
}

# ── 启动前端开发服务器 ──
start_frontend_dev() {
    log "启动前端开发服务器 (端口 5173)..."
    (cd frontend && npm run dev -- --host 2>&1 | sed 's/^/  [vite] /') &
    local pid=$!
    echo "$pid frontend" >> "$PID_FILE"
    sleep 2
    log "前端已就绪 ${CYAN}http://localhost:5173${NC}"
}

# ── 构建前端（生产模式）──
build_frontend() {
    log "构建前端..."
    (cd frontend && npm run build)
    log "前端构建完成 → frontend/dist/"
}

# ── 打印状态面板 ──
print_status() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   Weather-Quant 量化交易系统已启动       ║${NC}"
    echo -e "${CYAN}╠══════════════════════════════════════════╣${NC}"
    echo -e "${CYAN}║${NC}  后端 API:  ${GREEN}http://localhost:8000${NC}        ${CYAN}║${NC}"
    if [ "$1" = "dev" ]; then
    echo -e "${CYAN}║${NC}  前端面板:  ${GREEN}http://localhost:5173${NC}        ${CYAN}║${NC}"
    else
    echo -e "${CYAN}║${NC}  前端面板:  ${GREEN}http://localhost:8000${NC}        ${CYAN}║${NC}"
    fi
    echo -e "${CYAN}║${NC}  健康检查:  /api/health                 ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}  Brier回测: /api/backtest/brier          ${CYAN}║${NC}"
    echo -e "${CYAN}╠══════════════════════════════════════════╣${NC}"
    echo -e "${CYAN}║${NC}  ${YELLOW}Ctrl+C${NC} 停止  |  ${YELLOW}./start.sh --stop${NC} 停止   ${CYAN}║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════╝${NC}"
    echo ""
}

# ── 主逻辑 ──
trap stop_all EXIT INT TERM

case "${1:-}" in
    --stop)
        stop_all
        exit 0
        ;;
    --api)
        check_deps || true
        rm -f "$PID_FILE"
        start_backend
        print_status "api"
        wait
        ;;
    --dev)
        check_deps
        rm -f "$PID_FILE"
        start_frontend_dev
        wait
        ;;
    --build)
        check_deps
        rm -f "$PID_FILE"
        build_frontend
        start_backend
        print_status "prod"
        wait
        ;;
    ""|--all)
        HAS_NODE=true
        check_deps || HAS_NODE=false
        rm -f "$PID_FILE"
        start_backend
        if [ "$HAS_NODE" = true ]; then
            start_frontend_dev
            print_status "dev"
        else
            print_status "api"
        fi
        wait
        ;;
    *)
        echo "用法: $0 [--api|--dev|--build|--stop]"
        echo ""
        echo "  (无参数)   启动后端 + 前端开发服务器"
        echo "  --api      仅启动后端 API"
        echo "  --dev      仅启动前端开发服务器"
        echo "  --build    构建前端 + 启动后端（生产模式）"
        echo "  --stop     停止所有进程"
        exit 1
        ;;
esac
