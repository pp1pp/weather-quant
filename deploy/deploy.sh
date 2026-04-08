#!/bin/bash
# ============================================================
# Weather Quant - 一键部署/更新脚本
# 用法: bash deploy/deploy.sh
# ============================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "========================================="
echo "  Weather Quant 部署"
echo "  目录: $PROJECT_DIR"
echo "========================================="

# 检查 .env 是否存在
if [ ! -f config/.env ]; then
    echo "❌ 错误: config/.env 不存在"
    echo "   请先执行: cp config/.env.example config/.env"
    echo "   然后编辑填写你的 API 密钥和密码"
    exit 1
fi

# 从 .env 读取密码用于 Nginx
DASHBOARD_PASSWORD=$(grep -E "^DASHBOARD_PASSWORD=" config/.env | cut -d'=' -f2-)
if [ -z "$DASHBOARD_PASSWORD" ]; then
    echo "⚠️  警告: DASHBOARD_PASSWORD 未设置，Dashboard 将无密码保护"
    echo "   建议在 config/.env 中设置 DASHBOARD_PASSWORD=你的密码"
    echo ""
fi

# 1. 拉取最新代码（如果是 git 仓库）
if [ -d .git ]; then
    echo "[1/4] 拉取最新代码..."
    git pull origin main 2>/dev/null || echo "  跳过 git pull（可能有本地修改）"
else
    echo "[1/4] 非 git 仓库，跳过拉取"
fi

# 2. 构建 Docker 镜像
echo "[2/4] 构建 Docker 镜像（首次约 3-5 分钟）..."
docker compose build --no-cache 2>/dev/null || docker-compose build --no-cache

# 3. 启动/重启服务
echo "[3/4] 启动服务..."
docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true
docker compose up -d 2>/dev/null || docker-compose up -d

# 4. 配置 Nginx 反向代理
echo "[4/4] 配置 Nginx..."
NGINX_CONF="/etc/nginx/sites-available/weather-quant"

if [ -f /etc/nginx/sites-available/default ]; then
    # 标准 Debian/Ubuntu Nginx 目录结构
    cat > "$NGINX_CONF" << 'NGINX'
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name _;

    # 安全头
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;

    # Gzip 压缩
    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml;
    gzip_min_length 256;

    # 限制请求体大小
    client_max_body_size 10M;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket 支持（如果将来需要）
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # 超时设置
        proxy_connect_timeout 30s;
        proxy_read_timeout 60s;
    }

    # 静态资源缓存
    location /assets/ {
        proxy_pass http://127.0.0.1:8000/assets/;
        expires 7d;
        add_header Cache-Control "public, immutable";
    }
}
NGINX

    ln -sf "$NGINX_CONF" /etc/nginx/sites-enabled/weather-quant
    rm -f /etc/nginx/sites-enabled/default
    nginx -t && systemctl reload nginx
    echo "  Nginx 已配置"
else
    echo "  跳过 Nginx 配置（非标准目录结构，请手动配置）"
fi

# 等待服务启动
echo ""
echo "等待服务启动..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/api/health >/dev/null 2>&1; then
        echo ""
        echo "========================================="
        echo "  ✅ 部署成功！"
        echo ""
        echo "  本地访问:  http://localhost"
        echo "  公网访问:  http://$(curl -s ifconfig.me 2>/dev/null || echo '你的服务器IP')"
        if [ -n "$DASHBOARD_PASSWORD" ]; then
            echo "  用户名:    任意（如 admin）"
            echo "  密码:      (已在 .env 中设置)"
        fi
        echo ""
        echo "  查看日志:  docker compose logs -f"
        echo "  停止服务:  docker compose down"
        echo "  更新部署:  bash deploy/deploy.sh"
        echo "========================================="
        exit 0
    fi
    sleep 2
    printf "."
done

echo ""
echo "⚠️  服务可能仍在启动中（正在获取天气数据）"
echo "   请等待 1-2 分钟后访问，或查看日志:"
echo "   docker compose logs -f"
