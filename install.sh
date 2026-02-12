#!/bin/bash

# =================定义颜色与变量=================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
SKYBLUE='\033[0;36m'
PLAIN='\033[0m'

SCRIPT_URL="https://raw.githubusercontent.com/gagmm/fofa_bot/refs/heads/main/fofa.py"
INSTALL_DIR="/opt/fofa_bot"
SCRIPT_NAME="fofa.py"
SERVICE_NAME="fofa_bot"
# Python 依赖包 (去除引号，pip 本身可以直接解析)
TARGET_PACKAGES="requests pandas python-dateutil python-telegram-bot==13.15"

# =================日志与核心功能函数=================
log_info() { echo -e "${GREEN}[INFO] $1${PLAIN}"; }
log_warn() { echo -e "${YELLOW}[WARN] $1${PLAIN}"; }
log_err() { echo -e "${RED}[ERROR] $1${PLAIN}"; }

# 智能确保 Pip 存在
ensure_pip() {
    if python3 -m pip --version &> /dev/null; then
        log_info "Pip 环境检查通过: $(python3 -m pip --version | awk '{print $1, $2}')"
        return 0
    fi

    log_warn "系统包管理器未成功配置 Pip，尝试使用 get-pip.py 智能安装..."
    
    if ! command -v curl &> /dev/null; then
        log_err "缺少 curl，正在尝试自动安装..."
        apt-get install -y curl || yum install -y curl
    fi

    curl -sSL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
    
    # Root 用户下直接安装或强制安装
    log_info "正在安装 pip 核心组件..."
    if python3 /tmp/get-pip.py; then
        log_info "Pip 基础安装成功。"
    elif python3 /tmp/get-pip.py --break-system-packages; then
        log_warn "Pip 强制安装成功 (--break-system-packages)。"
    else
        log_err "Pip 核心组件安装失败，请检查网络或系统环境。"
        rm -f /tmp/get-pip.py
        exit 1
    fi
    rm -f /tmp/get-pip.py
}

# 智能安装 Python 依赖 (处理 PEP 668)
safe_install() {
    local packages=$1
    echo -e "${SKYBLUE}正在安装: $packages${PLAIN}"

    # 尝试更新 pip 本体 (静默执行，失败不影响后续)
    python3 -m pip install --upgrade pip >/dev/null 2>&1 || python3 -m pip install --upgrade pip --break-system-packages >/dev/null 2>&1

    # 第一次尝试：常规升级安装
    OUTPUT=$(python3 -m pip install --upgrade $packages 2>&1)
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        log_info "依赖包安装/更新成功。"
    else
        # 检查是否包含受管环境错误 (PEP 668)
        if echo "$OUTPUT" | grep -q "externally-managed-environment"; then
            log_warn "触发 Linux 系统 Python 保护机制 (PEP 668)。"
            log_warn "正在启用 --break-system-packages 强制覆盖模式安装..."
            
            # 第二次尝试：强制安装
            if python3 -m pip install --upgrade $packages --break-system-packages; then
                log_info "依赖包强制安装/更新成功。"
            else
                echo -e "${RED}$OUTPUT${PLAIN}"
                log_err "即使使用强制参数，依赖安装依然失败，请检查上方报错。"
                exit 1
            fi
        else
            # 其他错误 (如网络错误、编译错误)
            echo -e "${RED}$OUTPUT${PLAIN}"
            log_err "Python 依赖安装失败 (非系统保护原因)。请检查网络或 pip 环境。"
            exit 1
        fi
    fi
}

# =================主执行流程=================

# 检查是否为root用户
if [[ $EUID -ne 0 ]]; then
   log_err "必须使用 root 用户运行此脚本！" 
   exit 1
fi

echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN}    Fofa Bot 一键安装与自启动脚本 (高鲁棒版) ${PLAIN}"
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
echo -e "\n${YELLOW}[1/6] 更新系统并安装 Python 环境...${PLAIN}"
if command -v apt-get &> /dev/null; then
    apt-get update -y
    apt-get install -y python3 python3-pip wget curl
elif command -v yum &> /dev/null; then
    yum update -y
    yum install -y python3 python3-pip wget curl
else
    log_err "无法检测到适用的包管理器 (apt/yum)，请手动安装 python3, pip, wget 和 curl！"
    exit 1
fi

# 3. 确保 Pip 可用 (注入的高鲁棒性逻辑)
echo -e "\n${YELLOW}[2/6] 检查与配置 Pip 环境...${PLAIN}"
ensure_pip

# 4. 创建目录并下载脚本
echo -e "\n${YELLOW}[3/6] 创建目录并下载机器码...${PLAIN}"
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR" || exit

echo -e "正在下载: $FINAL_URL"
wget -O "$SCRIPT_NAME" "$FINAL_URL"

if [ ! -f "$SCRIPT_NAME" ]; then
    log_err "下载失败！请检查网络连接或源地址。"
    exit 1
fi
chmod +x "$SCRIPT_NAME"
log_info "脚本下载成功。"

# 5. 安装 Python 依赖 (注入的高鲁棒性逻辑)
echo -e "\n${YELLOW}[4/6] 安装 Python 依赖库...${PLAIN}"
safe_install "$TARGET_PACKAGES"

# 6. 创建 Systemd 服务 (开机自启)
echo -e "\n${YELLOW}[5/6] 配置 Systemd 开机自启服务...${PLAIN}"

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

# 7. 重新加载并启动服务
echo -e "\n${YELLOW}[6/6] 重新加载并启动服务...${PLAIN}"
systemctl daemon-reload
systemctl enable ${SERVICE_NAME}
systemctl start ${SERVICE_NAME}

# 8. 检查状态
echo -e "\n${GREEN}=============================================${PLAIN}"
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
