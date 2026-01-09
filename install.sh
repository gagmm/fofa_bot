#!/bin/bash

# 定义颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
SKYBLUE='\033[0;36m'
PLAIN='\033[0m'

# 定义变量
SCRIPT_URL="https://raw.githubusercontent.com/gagmm/fofa_bot/refs/heads/main/fofa.py"
INSTALL_DIR="/opt/fofa_bot"
SCRIPT_NAME="fofa.py"
SERVICE_NAME="fofa_bot"

# 检查是否为root用户
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}错误: 必须使用 root 用户运行此脚本！${PLAIN}" 
   exit 1
fi

echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN}    Fofa Bot 一键安装与自启动脚本 (自动版)   ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"

# 1. 网络环境选择 (GitHub 加速)
echo -e "${YELLOW}请选择下载节点 (如果你的服务器在中国大陆，建议选择 2):${PLAIN}"
echo -e "1. GitHub 官方源 (默认)"
echo -e "2. GitHub 代理加速 (ghproxy.net - 适用于国内)"
read -p "请输入选项 [1-2] (默认1): " download_opt

if [[ "$download_opt" == "2" ]]; then
    FINAL_URL="https://mirror.ghproxy.com/${SCRIPT_URL}"
    echo -e "${GREEN}已选择加速节点。${PLAIN}"
else
    FINAL_URL="${SCRIPT_URL}"
    echo -e "${GREEN}已选择官方节点。${PLAIN}"
fi

# 2. 安装系统基础依赖
echo -e "${YELLOW}[1/6] 更新系统并安装 Python 环境...${PLAIN}"
if [ -f /etc/debian_version ]; then
    apt-get update -y
    apt-get install -y python3 python3-pip wget curl
elif [ -f /etc/redhat-release ]; then
    yum update -y
    yum install -y python3 python3-pip wget curl
else
    echo -e "${RED}无法检测到操作系统版本，请手动安装 python3 和 pip！${PLAIN}"
    exit 1
fi

# 3. 创建目录并下载脚本
echo -e "${YELLOW}[2/6] 创建目录并下载脚本...${PLAIN}"
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR" || exit

echo -e "正在下载: $FINAL_URL"
wget -O "$SCRIPT_NAME" "$FINAL_URL"

if [ ! -f "$SCRIPT_NAME" ]; then
    echo -e "${RED}下载失败！请检查网络连接或源地址。${PLAIN}"
    exit 1
fi
chmod +x "$SCRIPT_NAME"
echo -e "${GREEN}脚本下载成功。${PLAIN}"

# 4. 安装 Python 依赖
echo -e "${YELLOW}[3/6] 安装 Python 依赖库...${PLAIN}"
echo -e "${SKYBLUE}正在安装 pandas, requests, python-dateutil...${PLAIN}"

# 升级 pip 防止安装报错
python3 -m pip install --upgrade pip

# 安装依赖
# 注意：指定 python-telegram-bot==13.15 是因为代码使用了 Updater (v20+已弃用)
pip3 install requests pandas python-dateutil "python-telegram-bot==13.15" --upgrade

if [ $? -ne 0 ]; then
    echo -e "${RED}Python 依赖安装失败！请检查 pip 环境。${PLAIN}"
    exit 1
fi

# 5. 创建 Systemd 服务 (开机自启)
echo -e "${YELLOW}[4/6] 配置 Systemd 开机自启服务...${PLAIN}"

cat > /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=Fofa Telegram Bot Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}
ExecStart=/usr/bin/python3 ${INSTALL_DIR}/${SCRIPT_NAME}
Restart=always
RestartSec=10s
StandardOutput=syslog
StandardError=syslog

[Install]
WantedBy=multi-user.target
EOF

# 6. 重新加载并启动服务
echo -e "${YELLOW}[5/6] 重新加载服务配置...${PLAIN}"
systemctl daemon-reload
systemctl enable ${SERVICE_NAME}

echo -e "${YELLOW}[6/6] 正在立即启动服务...${PLAIN}"
systemctl start ${SERVICE_NAME}

# 7. 检查状态
echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN}              安装与启动完成！               ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"

# 检查服务状态
if systemctl is-active --quiet ${SERVICE_NAME}; then
    echo -e "服务状态: ${GREEN}正在运行 (Active)${PLAIN}"
else
    echo -e "服务状态: ${RED}启动失败 (Inactive)${PLAIN}"
    echo -e "可能是因为脚本中的 Token 未配置导致报错退出。"
    echo -e "请使用以下命令查看详细错误日志："
    echo -e "${YELLOW}journalctl -u ${SERVICE_NAME} -n 20 --no-pager${PLAIN}"
fi

echo -e ""
echo -e "脚本位置: ${YELLOW}${INSTALL_DIR}/${SCRIPT_NAME}${PLAIN}"
echo -e "修改配置: ${YELLOW}nano ${INSTALL_DIR}/${SCRIPT_NAME}${PLAIN}"
echo -e "重启服务: ${YELLOW}systemctl restart ${SERVICE_NAME}${PLAIN}"
