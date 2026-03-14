#!/bin/bash
# ============================================================================
# Fofa Telegram Bot — 自适应安装脚本 v2.0
# 支持: Debian 10+/Ubuntu 18+/CentOS 7+/AlmaLinux/Rocky/Fedora/Alpine/Arch
# 要求: root 权限, 可联网
# ============================================================================
set -euo pipefail
IFS=$'\n\t'

# ========================== 全局常量 ==========================
readonly SCRIPT_VERSION="2.0.0"
readonly INSTALL_DIR="/opt/fofa-bot"
readonly SCRIPT_NAME="fofa_bot.py"
readonly SERVICE_NAME="fofa-bot"
readonly VENV_DIR="${INSTALL_DIR}/venv"
readonly LOG_FILE="/var/log/${SERVICE_NAME}-install.log"
readonly REQUIRED_PYTHON_MAJOR=3
readonly REQUIRED_PYTHON_MINOR=7          # 最低 Python 3.7
readonly TARGET_PACKAGES="python-telegram-bot requests aiohttp"
readonly LOCK_FILE="/tmp/${SERVICE_NAME}-install.lock"

# ========================== 颜色定义 ==========================
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    PLAIN='\033[0m'
    BOLD='\033[1m'
else
    RED='' GREEN='' YELLOW='' BLUE='' CYAN='' PLAIN='' BOLD=''
fi

# ========================== 日志函数 ==========================
log()      { echo -e "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
info()     { log "${GREEN}[INFO]${PLAIN}  $*"; }
warn()     { log "${YELLOW}[WARN]${PLAIN}  $*"; }
error()    { log "${RED}[ERROR]${PLAIN} $*"; }
fatal()    { error "$*"; cleanup_on_failure; exit 1; }
step()     { echo -e "\n${CYAN}${BOLD}>>> $*${PLAIN}" | tee -a "$LOG_FILE"; }
divider()  { echo -e "${BLUE}$(printf '=%.0s' {1..60})${PLAIN}"; }

# ========================== 锁 / 清理 ==========================
acquire_lock() {
    if [[ -f "$LOCK_FILE" ]]; then
        local pid
        pid=$(cat "$LOCK_FILE" 2>/dev/null || true)
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            fatal "另一个安装进程 (PID=$pid) 正在运行，如确认无冲突请删除 $LOCK_FILE"
        fi
        warn "发现残留锁文件，已自动清除"
        rm -f "$LOCK_FILE"
    fi
    echo $$ > "$LOCK_FILE"
}

release_lock() { rm -f "$LOCK_FILE"; }

cleanup_on_failure() {
    release_lock
    warn "安装中途失败，日志已写入 ${LOG_FILE}"
}

trap 'cleanup_on_failure' ERR
trap 'release_lock' EXIT

# ========================== 权限检查 ==========================
check_root() {
    if [[ $EUID -ne 0 ]]; then
        fatal "此脚本需要 root 权限，请使用 sudo 或 root 用户执行"
    fi
}

# ========================== 发行版检测 ==========================
detect_os() {
    OS_ID="unknown"
    OS_VERSION_ID=""
    PKG_MANAGER=""
    PKG_UPDATE=""
    PKG_INSTALL=""
    PYTHON_PKG=""
    PYTHON_DEV_PKG=""
    PIP_PKG=""
    VENV_PKG=""
    BUILD_DEPS=""

    if [[ -f /etc/os-release ]]; then
        # shellcheck source=/dev/null
        source /etc/os-release
        OS_ID="${ID,,}"
        OS_VERSION_ID="${VERSION_ID:-0}"
    elif [[ -f /etc/redhat-release ]]; then
        OS_ID="centos"
    elif command -v apk &>/dev/null; then
        OS_ID="alpine"
    fi

    case "$OS_ID" in
        ubuntu|debian|linuxmint|pop|kali|deepin|uos)
            PKG_MANAGER="apt"
            PKG_UPDATE="apt-get update -qq"
            PKG_INSTALL="apt-get install -y -qq"
            PYTHON_PKG="python3"
            PYTHON_DEV_PKG="python3-dev"
            PIP_PKG="python3-pip"
            VENV_PKG="python3-venv"
            BUILD_DEPS="build-essential libffi-dev libssl-dev"
            ;;
        centos|rhel|almalinux|rocky|ol)
            PKG_MANAGER="yum"
            if command -v dnf &>/dev/null; then
                PKG_MANAGER="dnf"
            fi
            PKG_UPDATE="${PKG_MANAGER} makecache -q"
            PKG_INSTALL="${PKG_MANAGER} install -y -q"
            # CentOS 7 特殊处理
            local major_ver="${OS_VERSION_ID%%.*}"
            if [[ "$major_ver" -le 7 ]]; then
                PYTHON_PKG="python3"
                PIP_PKG="python3-pip"
                VENV_PKG=""        # CentOS 7 的 venv 内置于 python3
                PYTHON_DEV_PKG="python3-devel"
            else
                PYTHON_PKG="python3"
                PIP_PKG="python3-pip"
                VENV_PKG=""
                PYTHON_DEV_PKG="python3-devel"
            fi
            BUILD_DEPS="gcc libffi-devel openssl-devel"
            ;;
        fedora)
            PKG_MANAGER="dnf"
            PKG_UPDATE="dnf makecache -q"
            PKG_INSTALL="dnf install -y -q"
            PYTHON_PKG="python3"
            PIP_PKG="python3-pip"
            VENV_PKG=""
            PYTHON_DEV_PKG="python3-devel"
            BUILD_DEPS="gcc libffi-devel openssl-devel"
            ;;
        alpine)
            PKG_MANAGER="apk"
            PKG_UPDATE="apk update"
            PKG_INSTALL="apk add --no-cache"
            PYTHON_PKG="python3"
            PIP_PKG="py3-pip"
            VENV_PKG=""
            PYTHON_DEV_PKG="python3-dev"
            BUILD_DEPS="gcc musl-dev libffi-dev openssl-dev"
            ;;
        arch|manjaro|endeavouros)
            PKG_MANAGER="pacman"
            PKG_UPDATE="pacman -Sy --noconfirm"
            PKG_INSTALL="pacman -S --noconfirm --needed"
            PYTHON_PKG="python"
            PIP_PKG="python-pip"
            VENV_PKG=""
            PYTHON_DEV_PKG=""
            BUILD_DEPS="base-devel libffi openssl"
            ;;
        *)
            warn "未能识别的发行版: ${OS_ID}，尝试通用模式"
            # 尝试逐一检测包管理器
            if command -v apt-get &>/dev/null; then
                OS_ID="debian"
                detect_os   # 递归用 debian 分支
                return
            elif command -v yum &>/dev/null; then
                OS_ID="centos"
                detect_os
                return
            elif command -v apk &>/dev/null; then
                OS_ID="alpine"
                detect_os
                return
            elif command -v pacman &>/dev/null; then
                OS_ID="arch"
                detect_os
                return
            fi
            fatal "无法识别操作系统且未找到已知包管理器。请手动安装 Python≥3.7 后重试。"
            ;;
    esac

    info "检测到系统: ${BOLD}${OS_ID} ${OS_VERSION_ID}${PLAIN}  包管理器: ${PKG_MANAGER}"
}

# ========================== 网络检测 ==========================
check_network() {
    step "[预检] 检查网络连通性..."

    local test_urls=("https://pypi.org" "https://www.google.com" "https://mirrors.aliyun.com")
    local reachable=false

    for url in "${test_urls[@]}"; do
        if curl -sS --max-time 8 --head "$url" &>/dev/null; then
            reachable=true
            info "网络可达 (${url})"
            break
        fi
    done

    if ! $reachable; then
        warn "所有测试站点均不可达，pip 安装可能失败"
        warn "如处于内网环境，请确保已配置内部 PyPI 镜像"
    fi
}

# ========================== Python 检测与安装 ==========================
# 返回满足版本要求的 python3 路径，或空字符串
find_suitable_python() {
    local candidates=("python3" "python3.12" "python3.11" "python3.10" "python3.9" "python3.8" "python3.7" "python")
    for cmd in "${candidates[@]}"; do
        local path
        path=$(command -v "$cmd" 2>/dev/null || true)
        [[ -z "$path" ]] && continue

        # 检查是否为 Python 3 且版本 >= 要求
        local ver
        ver=$("$path" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || true)
        [[ -z "$ver" ]] && continue

        local major minor
        major="${ver%%.*}"
        minor="${ver##*.}"

        if [[ "$major" -eq "$REQUIRED_PYTHON_MAJOR" && "$minor" -ge "$REQUIRED_PYTHON_MINOR" ]]; then
            echo "$path"
            return 0
        fi
    done
    return 1
}

install_python_from_pkg() {
    info "通过系统包管理器安装 Python3..."

    $PKG_UPDATE >> "$LOG_FILE" 2>&1 || warn "包索引更新失败（可忽略）"

    # 安装 Python + pip + venv + 构建依赖
    local pkgs_to_install="$PYTHON_PKG"
    [[ -n "${PIP_PKG:-}" ]]        && pkgs_to_install+=" $PIP_PKG"
    [[ -n "${VENV_PKG:-}" ]]       && pkgs_to_install+=" $VENV_PKG"
    [[ -n "${PYTHON_DEV_PKG:-}" ]] && pkgs_to_install+=" $PYTHON_DEV_PKG"

    # shellcheck disable=SC2086
    $PKG_INSTALL $pkgs_to_install >> "$LOG_FILE" 2>&1 || true

    # 验证
    if ! find_suitable_python &>/dev/null; then
        return 1
    fi
    return 0
}

# 从源码编译 Python（最终回退方案）
install_python_from_source() {
    local py_ver="3.11.9"
    warn "系统包管理器中无可用 Python ≥ ${REQUIRED_PYTHON_MAJOR}.${REQUIRED_PYTHON_MINOR}"
    info "即将从源码编译 Python ${py_ver}（约需 3-10 分钟）..."

    # 安装编译依赖
    # shellcheck disable=SC2086
    $PKG_INSTALL ${BUILD_DEPS} >> "$LOG_FILE" 2>&1 || true

    case "$PKG_MANAGER" in
        apt)     $PKG_INSTALL zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
                              libncurses5-dev libncursesw5-dev xz-utils tk-dev liblzma-dev >> "$LOG_FILE" 2>&1 || true ;;
        yum|dnf) $PKG_INSTALL zlib-devel bzip2-devel readline-devel sqlite-devel wget curl \
                              ncurses-devel xz-devel tk-devel >> "$LOG_FILE" 2>&1 || true ;;
    esac

    local src_dir="/tmp/python-build-${py_ver}"
    mkdir -p "$src_dir" && cd "$src_dir"

    if [[ ! -f "Python-${py_ver}.tgz" ]]; then
        info "下载 Python ${py_ver} 源码..."
        curl -sSL "https://www.python.org/ftp/python/${py_ver}/Python-${py_ver}.tgz" -o "Python-${py_ver}.tgz" \
            || curl -sSL "https://mirrors.huaweicloud.com/python/${py_ver}/Python-${py_ver}.tgz" -o "Python-${py_ver}.tgz" \
            || fatal "Python 源码下载失败"
    fi

    tar -xzf "Python-${py_ver}.tgz"
    cd "Python-${py_ver}"

    info "编译中 (./configure && make -j$(nproc))..."
    ./configure --prefix=/usr/local --enable-optimizations --with-ensurepip=install >> "$LOG_FILE" 2>&1
    make -j"$(nproc)" >> "$LOG_FILE" 2>&1
    make altinstall >> "$LOG_FILE" 2>&1        # altinstall 避免覆盖系统 python

    # 创建符号链接（仅在没有 python3 时）
    if ! command -v python3 &>/dev/null; then
        ln -sf /usr/local/bin/python3.11 /usr/local/bin/python3
        ln -sf /usr/local/bin/pip3.11    /usr/local/bin/pip3
    fi

    cd /
    rm -rf "$src_dir"

    if ! find_suitable_python &>/dev/null; then
        fatal "源码编译后仍未找到可用 Python，请手动排查"
    fi

    info "Python ${py_ver} 源码编译安装成功"
}

ensure_python() {
    step "[1/6] 检测 / 安装 Python 环境..."

    PYTHON_BIN=$(find_suitable_python || true)

    if [[ -n "$PYTHON_BIN" ]]; then
        local ver
        ver=$("$PYTHON_BIN" --version 2>&1)
        info "已找到可用 Python: ${BOLD}${PYTHON_BIN}${PLAIN} (${ver})"
        return
    fi

    warn "未找到 Python ≥ ${REQUIRED_PYTHON_MAJOR}.${REQUIRED_PYTHON_MINOR}，开始自动安装..."

    if install_python_from_pkg; then
        PYTHON_BIN=$(find_suitable_python)
        info "包管理器安装成功: $PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"
        return
    fi

    install_python_from_source
    PYTHON_BIN=$(find_suitable_python)
    info "最终使用: $PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"
}

# ========================== venv 虚拟环境 ==========================
ensure_venv() {
    step "[2/6] 创建 / 验证 Python 虚拟环境..."

    # 确保 venv 模块可用
    if ! "$PYTHON_BIN" -m venv --help &>/dev/null; then
        warn "venv 模块不可用，尝试安装..."

        case "$PKG_MANAGER" in
            apt)
                local py_ver
                py_ver=$("$PYTHON_BIN" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
                $PKG_INSTALL "python${py_ver}-venv" >> "$LOG_FILE" 2>&1 \
                    || $PKG_INSTALL python3-venv >> "$LOG_FILE" 2>&1 \
                    || true
                ;;
            yum|dnf)
                $PKG_INSTALL python3-libs >> "$LOG_FILE" 2>&1 || true
                ;;
        esac

        if ! "$PYTHON_BIN" -m venv --help &>/dev/null; then
            warn "venv 仍不可用，回退: 尝试使用 virtualenv"
            "$PYTHON_BIN" -m pip install --quiet virtualenv >> "$LOG_FILE" 2>&1 || true
            if "$PYTHON_BIN" -m virtualenv --version &>/dev/null; then
                USE_VIRTUALENV=true
            else
                warn "virtualenv 也不可用，将直接使用系统 pip（不推荐）"
                USE_SYSTEM_PIP=true
                return
            fi
        fi
    fi

    USE_VIRTUALENV="${USE_VIRTUALENV:-false}"
    USE_SYSTEM_PIP="${USE_SYSTEM_PIP:-false}"

    # 如已存在旧 venv 且 Python 版本不匹配则重建
    if [[ -d "$VENV_DIR" ]]; then
        local venv_py="${VENV_DIR}/bin/python3"
        if [[ -x "$venv_py" ]]; then
            local venv_ver sys_ver
            venv_ver=$("$venv_py" --version 2>&1 || true)
            sys_ver=$("$PYTHON_BIN" --version 2>&1)
            if [[ "$venv_ver" == "$sys_ver" ]]; then
                info "虚拟环境已存在且版本匹配 (${venv_ver})，跳过创建"
                VENV_PYTHON="${VENV_DIR}/bin/python3"
                VENV_PIP="${VENV_DIR}/bin/pip3"
                return
            fi
            warn "虚拟环境 Python 版本不匹配 (venv=${venv_ver}, sys=${sys_ver})，重建..."
        fi
        rm -rf "$VENV_DIR"
    fi

    info "创建虚拟环境: ${VENV_DIR}"

    if [[ "$USE_VIRTUALENV" == true ]]; then
        "$PYTHON_BIN" -m virtualenv "$VENV_DIR" >> "$LOG_FILE" 2>&1
    else
        "$PYTHON_BIN" -m venv "$VENV_DIR" >> "$LOG_FILE" 2>&1
    fi

    VENV_PYTHON="${VENV_DIR}/bin/python3"
    VENV_PIP="${VENV_DIR}/bin/pip3"

    # 升级 pip
    "$VENV_PYTHON" -m pip install --upgrade pip setuptools wheel --quiet >> "$LOG_FILE" 2>&1 || true

    info "虚拟环境就绪"
}

# ========================== 目录与脚本部署 ==========================
deploy_script() {
    step "[3/6] 部署应用文件..."

    mkdir -p "$INSTALL_DIR"

    if [[ ! -f "${INSTALL_DIR}/${SCRIPT_NAME}" ]]; then
        info "生成默认 ${SCRIPT_NAME}（占位模板）"
        cat > "${INSTALL_DIR}/${SCRIPT_NAME}" <<'PYEOF'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fofa Telegram Bot — 占位模板
请替换以下配置后重启服务:
    systemctl restart fofa-bot
"""
import os, sys, logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ===================== 用户配置 =====================
TELEGRAM_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
FOFA_EMAIL         = os.getenv("FOFA_EMAIL",    "YOUR_FOFA_EMAIL")
FOFA_KEY           = os.getenv("FOFA_KEY",      "YOUR_FOFA_KEY")
# ====================================================

def main():
    if "YOUR_" in TELEGRAM_BOT_TOKEN:
        logger.error("请先配置 TELEGRAM_BOT_TOKEN！")
        logger.error("编辑文件: %s", os.path.abspath(__file__))
        sys.exit(1)

    logger.info("Bot 启动中... (token=%s***)", TELEGRAM_BOT_TOKEN[:8])
    # TODO: 在此初始化你的 bot 逻辑
    logger.info("占位模板，无实际逻辑，退出。")

if __name__ == "__main__":
    main()
PYEOF
        chmod 600 "${INSTALL_DIR}/${SCRIPT_NAME}"
        info "模板已生成 — 请稍后编辑配置"
    else
        info "目标脚本已存在，保留现有文件"
    fi
}

# ========================== pip 安装依赖 ==========================
configure_pip_mirror() {
    # 国内环境自动配置镜像
    local pip_conf_dir pip_conf
    pip_conf_dir="${INSTALL_DIR}/.pip"
    pip_conf="${pip_conf_dir}/pip.conf"

    # 检测是否中国网络 — 尝试连通 pypi.org，若超时则启用镜像
    if ! curl -sS --max-time 5 --head "https://pypi.org/simple/" &>/dev/null; then
        info "PyPI 官方源较慢/不可达，启用阿里云镜像"
        mkdir -p "$pip_conf_dir"
        cat > "$pip_conf" <<PIPEOF
[global]
index-url = https://mirrors.aliyun.com/pypi/simple/
trusted-host = mirrors.aliyun.com
timeout = 120
PIPEOF
        export PIP_CONFIG_FILE="$pip_conf"
    fi
}

safe_install() {
    local packages="$1"
    local pip_cmd
    local python_cmd
    local max_retries=3
    local attempt=0

    if [[ "${USE_SYSTEM_PIP:-false}" == true ]]; then
        pip_cmd="$PYTHON_BIN -m pip"
        python_cmd="$PYTHON_BIN"
    else
        pip_cmd="${VENV_PIP}"
        python_cmd="${VENV_PYTHON}"
    fi

    configure_pip_mirror

    while (( attempt < max_retries )); do
        attempt=$((attempt + 1))
        info "pip install 尝试 [${attempt}/${max_retries}]..."

        # shellcheck disable=SC2086
        if $python_cmd -m pip install --upgrade $packages >> "$LOG_FILE" 2>&1; then
            info "所有依赖安装成功"
            return 0
        fi

        warn "第 ${attempt} 次安装失败"

        if (( attempt < max_retries )); then
            # 逐个安装以定位问题包
            info "切换为逐包安装模式..."
            for pkg in $packages; do
                $python_cmd -m pip install --upgrade "$pkg" >> "$LOG_FILE" 2>&1 \
                    && info "  ✓ $pkg" \
                    || warn "  ✗ $pkg (将在下轮重试)"
            done
        fi

        sleep 3
    done

    # 最终检验：只要核心包存在即可
    local critical_fail=false
    for pkg in $packages; do
        local import_name="${pkg//-/_}"                 # python-telegram-bot -> python_telegram_bot
        [[ "$import_name" == "python_telegram_bot" ]] && import_name="telegram"
        if ! $python_cmd -c "import ${import_name}" &>/dev/null; then
            error "关键依赖 ${pkg} 导入失败"
            critical_fail=true
        fi
    done

    if $critical_fail; then
        error "部分关键依赖安装失败，详见日志: ${LOG_FILE}"
        error "可尝试手动安装: ${python_cmd} -m pip install ${packages}"
        return 1
    fi

    info "所有关键依赖验证通过"
    return 0
}

install_dependencies() {
    step "[4/6] 安装 Python 依赖库..."
    safe_install "$TARGET_PACKAGES"
}

# ========================== Systemd / OpenRC 服务 ==========================
create_service() {
    step "[5/6] 配置系统服务（开机自启）..."

    local exec_python
    if [[ "${USE_SYSTEM_PIP:-false}" == true ]]; then
        exec_python="$PYTHON_BIN"
    else
        exec_python="${VENV_PYTHON}"
    fi

    # ---------- Systemd ----------
    if command -v systemctl &>/dev/null && [[ -d /run/systemd/system ]]; then

        cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<SVCEOF
[Unit]
Description=Fofa Telegram Bot Service
Documentation=https://github.com/your-repo
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}
ExecStart=${exec_python} ${INSTALL_DIR}/${SCRIPT_NAME}
Restart=on-failure
RestartSec=15s
StartLimitIntervalSec=300
StartLimitBurst=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=${INSTALL_DIR}
PrivateTmp=true

# 环境变量（可选）
# Environment="TG_BOT_TOKEN=xxx"
# Environment="FOFA_EMAIL=xxx"
# Environment="FOFA_KEY=xxx"
EnvironmentFile=-${INSTALL_DIR}/.env

[Install]
WantedBy=multi-user.target
SVCEOF

        info "Systemd 服务文件已写入"

    # ---------- OpenRC (Alpine) ----------
    elif command -v rc-update &>/dev/null; then

        cat > "/etc/init.d/${SERVICE_NAME}" <<RCEOF
#!/sbin/openrc-run

name="${SERVICE_NAME}"
description="Fofa Telegram Bot Service"
command="${exec_python}"
command_args="${INSTALL_DIR}/${SCRIPT_NAME}"
command_background=true
pidfile="/run/${SERVICE_NAME}.pid"
directory="${INSTALL_DIR}"

depend() {
    need net
}
RCEOF
        chmod +x "/etc/init.d/${SERVICE_NAME}"
        info "OpenRC 服务脚本已写入"

    else
        warn "未检测到 systemd 或 openrc，跳过服务创建"
        warn "请手动运行: ${exec_python} ${INSTALL_DIR}/${SCRIPT_NAME}"
        return
    fi

    # 创建可选的 .env 文件
    if [[ ! -f "${INSTALL_DIR}/.env" ]]; then
        cat > "${INSTALL_DIR}/.env" <<ENVEOF
# 取消注释并填入真实值（服务启动时自动加载）
# TG_BOT_TOKEN=your_token_here
# FOFA_EMAIL=your_email_here
# FOFA_KEY=your_key_here
ENVEOF
        chmod 600 "${INSTALL_DIR}/.env"
        info "环境变量模板已生成: ${INSTALL_DIR}/.env"
    fi
}

# ========================== 启动 & 状态检查 ==========================
start_and_verify() {
    step "[6/6] 启动服务并验证..."

    if command -v systemctl &>/dev/null && [[ -d /run/systemd/system ]]; then
        systemctl daemon-reload
        systemctl enable "${SERVICE_NAME}" >> "$LOG_FILE" 2>&1
        systemctl restart "${SERVICE_NAME}" >> "$LOG_FILE" 2>&1 || true

        # 等待 3 秒让进程启动
        sleep 3

        if systemctl is-active --quiet "${SERVICE_NAME}"; then
            info "服务状态: ${GREEN}${BOLD}正在运行 (Active)${PLAIN}"
        else
            warn "服务状态: ${RED}未运行 (Inactive/Failed)${PLAIN}"
            echo ""
            echo -e "  ${YELLOW}可能原因:${PLAIN}"
            echo "    1. Bot Token / API Key 未配置"
            echo "    2. Python 依赖导入错误"
            echo ""
            echo -e "  ${YELLOW}排查命令:${PLAIN}"
            echo "    journalctl -u ${SERVICE_NAME} -n 30 --no-pager"
            echo "    systemctl status ${SERVICE_NAME}"
        fi

    elif command -v rc-update &>/dev/null; then
        rc-update add "${SERVICE_NAME}" default >> "$LOG_FILE" 2>&1
        rc-service "${SERVICE_NAME}" start >> "$LOG_FILE" 2>&1 || true

        if rc-service "${SERVICE_NAME}" status &>/dev/null; then
            info "服务状态: ${GREEN}${BOLD}正在运行${PLAIN}"
        else
            warn "服务可能未成功启动，请检查日志"
        fi
    else
        warn "无服务管理器，请手动启动"
    fi
}

# ========================== 卸载功能 ==========================
uninstall() {
    echo -e "\n${RED}${BOLD}========== 卸载 ${SERVICE_NAME} ==========${PLAIN}"
    read -rp "确认卸载？(y/N): " confirm
    [[ "${confirm,,}" != "y" ]] && { echo "已取消"; exit 0; }

    if command -v systemctl &>/dev/null; then
        systemctl stop "${SERVICE_NAME}" 2>/dev/null || true
        systemctl disable "${SERVICE_NAME}" 2>/dev/null || true
        rm -f "/etc/systemd/system/${SERVICE_NAME}.service"
        systemctl daemon-reload
    fi

    if command -v rc-update &>/dev/null; then
        rc-service "${SERVICE_NAME}" stop 2>/dev/null || true
        rc-update del "${SERVICE_NAME}" 2>/dev/null || true
        rm -f "/etc/init.d/${SERVICE_NAME}"
    fi

    # 可选保留配置
    read -rp "是否删除安装目录 ${INSTALL_DIR}？(y/N): " del_dir
    if [[ "${del_dir,,}" == "y" ]]; then
        rm -rf "${INSTALL_DIR}"
        info "目录已删除"
    else
        info "目录已保留"
    fi

    rm -f "$LOG_FILE"
    info "卸载完成"
    exit 0
}

# ========================== 帮助信息 ==========================
show_help() {
    cat <<HELPEOF
用法: $0 [选项]

选项:
  install     安装 / 重新安装（默认）
  uninstall   完全卸载
  status      查看服务运行状态
  logs        查看最近日志
  help        显示本帮助

示例:
  bash $0                # 交互式安装
  bash $0 install        # 直接安装
  bash $0 uninstall      # 卸载
HELPEOF
    exit 0
}

show_status() {
    if command -v systemctl &>/dev/null; then
        systemctl status "${SERVICE_NAME}" --no-pager
    elif command -v rc-service &>/dev/null; then
        rc-service "${SERVICE_NAME}" status
    else
        echo "无可用的服务管理器"
    fi
    exit 0
}

show_logs() {
    if command -v journalctl &>/dev/null; then
        journalctl -u "${SERVICE_NAME}" -n 50 --no-pager
    else
        echo "journalctl 不可用，查看安装日志:"
        tail -50 "$LOG_FILE" 2>/dev/null || echo "无日志"
    fi
    exit 0
}

# ========================== 安装后信息 ==========================
print_summary() {
    local exec_python
    if [[ "${USE_SYSTEM_PIP:-false}" == true ]]; then
        exec_python="$PYTHON_BIN"
    else
        exec_python="${VENV_PYTHON}"
    fi

    echo ""
    divider
    echo -e "${GREEN}${BOLD}        ✅  安装与部署完成！${PLAIN}"
    divider
    echo ""
    echo -e "  Python:       ${CYAN}${PYTHON_BIN} ($($PYTHON_BIN --version 2>&1))${PLAIN}"
    if [[ "${USE_SYSTEM_PIP:-false}" != true ]]; then
        echo -e "  虚拟环境:     ${CYAN}${VENV_DIR}${PLAIN}"
    fi
    echo -e "  脚本位置:     ${CYAN}${INSTALL_DIR}/${SCRIPT_NAME}${PLAIN}"
    echo -e "  环境变量:     ${CYAN}${INSTALL_DIR}/.env${PLAIN}"
    echo -e "  安装日志:     ${CYAN}${LOG_FILE}${PLAIN}"
    echo ""
    divider
    echo -e "  ${BOLD}常用命令:${PLAIN}"
    echo -e "  编辑配置:     ${YELLOW}nano ${INSTALL_DIR}/${SCRIPT_NAME}${PLAIN}"
    echo -e "  编辑 ENV:     ${YELLOW}nano ${INSTALL_DIR}/.env${PLAIN}"
    echo -e "  重启服务:     ${YELLOW}systemctl restart ${SERVICE_NAME}${PLAIN}"
    echo -e "  查看状态:     ${YELLOW}systemctl status ${SERVICE_NAME}${PLAIN}"
    echo -e "  查看日志:     ${YELLOW}journalctl -u ${SERVICE_NAME} -f${PLAIN}"
    echo -e "  手动运行:     ${YELLOW}${exec_python} ${INSTALL_DIR}/${SCRIPT_NAME}${PLAIN}"
    echo -e "  完全卸载:     ${YELLOW}bash $0 uninstall${PLAIN}"
    divider
    echo ""
}

# ========================== 主流程 ==========================
main() {
    local action="${1:-install}"

    case "$action" in
        uninstall|remove)  check_root; uninstall ;;
        status)            show_status ;;
        logs|log)          show_logs ;;
        help|-h|--help)    show_help ;;
        install|"")        ;;    # 继续执行
        *)                 error "未知参数: $action"; show_help ;;
    esac

    # ---- 安装流程 ----
    echo ""
    divider
    echo -e "${GREEN}${BOLD}  Fofa Telegram Bot 自适应安装脚本 v${SCRIPT_VERSION}${PLAIN}"
    divider

    check_root
    acquire_lock
    mkdir -p "$(dirname "$LOG_FILE")"
    : > "$LOG_FILE"

    detect_os              # [预检] 系统识别
    check_network          # [预检] 网络检测
    ensure_python          # [1/6] Python
    deploy_script          # [3/6] 部署文件（先于 venv，因 ensure_venv 依赖 INSTALL_DIR）
    ensure_venv            # [2/6] 虚拟环境
    install_dependencies   # [4/6] pip install
    create_service         # [5/6] Systemd/OpenRC
    start_and_verify       # [6/6] 启动 & 验证
    print_summary          # 汇总

    release_lock
}

main "$@"
