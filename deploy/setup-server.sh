#!/bin/bash
# ============================================================
# Weather Quant - 服务器初始化脚本 (腾讯云/阿里云/任意Linux)
# 在全新服务器上运行一次即可
# 用法: curl -sL <your-raw-url> | bash
#   或: bash setup-server.sh
# ============================================================
set -e

echo "========================================="
echo "  Weather Quant 服务器环境初始化"
echo "========================================="

# 1. 系统更新
echo "[1/6] 更新系统包..."
apt-get update -y && apt-get upgrade -y

# 2. 安装 Docker
echo "[2/6] 安装 Docker..."
if command -v docker &>/dev/null; then
    echo "  Docker 已安装: $(docker --version)"
else
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
    echo "  Docker 安装完成: $(docker --version)"
fi

# 3. 安装 Docker Compose
echo "[3/6] 安装 Docker Compose..."
if command -v docker-compose &>/dev/null || docker compose version &>/dev/null 2>&1; then
    echo "  Docker Compose 已安装"
else
    apt-get install -y docker-compose-plugin
    echo "  Docker Compose 安装完成"
fi

# 4. 安装 Nginx + Certbot (用于HTTPS和反向代理)
echo "[4/6] 安装 Nginx..."
apt-get install -y nginx certbot python3-certbot-nginx
systemctl enable nginx

# 5. 配置防火墙
echo "[5/6] 配置防火墙..."
if command -v ufw &>/dev/null; then
    ufw allow 22/tcp    # SSH
    ufw allow 80/tcp    # HTTP
    ufw allow 443/tcp   # HTTPS
    ufw --force enable
    echo "  防火墙已开启 (22, 80, 443)"
fi

# 6. 创建项目目录
echo "[6/6] 创建项目目录..."
mkdir -p /opt/weather-quant/data/logs
mkdir -p /opt/weather-quant/data/reviews
mkdir -p /opt/weather-quant/config

echo ""
echo "========================================="
echo "  ✅ 服务器环境准备完毕！"
echo ""
echo "  接下来请执行:"
echo "  1. cd /opt/weather-quant"
echo "  2. git clone https://github.com/pp1pp/weather-quant.git ."
echo "  3. cp config/.env.example config/.env"
echo "  4. nano config/.env   (填写你的API密钥和密码)"
echo "  5. bash deploy/deploy.sh"
echo "========================================="
