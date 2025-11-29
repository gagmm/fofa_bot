import os
import sys
import json
import logging
import base64
import time
import re
import requests
import signal
import socket
import hashlib
import shutil
import random
import csv
import asyncio
import pandas as pd
import threading
from functools import wraps
from datetime import datetime, timedelta
from dateutil import tz
from urllib.parse import urlparse
import uuid # 确保文件顶部有这行
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, ParseMode, ReplyKeyboardMarkup, KeyboardButton, InlineQueryResultArticle, InputTextMessageContent
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackContext,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    InlineQueryHandler, # <--- 确保有这个
    Filters,
)

from telegram.error import BadRequest, RetryAfter, TimedOut, NetworkError, InvalidToken

# --- 全局变量和常量 ---
CONFIG_FILE = 'config.json'
HISTORY_FILE = 'history.json'
LOG_FILE = 'fofa_bot.log'
FOFA_CACHE_DIR = 'fofa_file'
ANONYMOUS_KEYS_FILE = 'fofa_anonymous.json'
SCAN_TASKS_FILE = 'scan_tasks.json'
MONITOR_TASKS_FILE = 'monitor_tasks.json' # 新增监控配置
MONITOR_DATA_DIR = 'monitor_data' # 新增监控数据目录
MAX_HISTORY_SIZE = 50
MAX_SCAN_TASKS = 50
CACHE_EXPIRATION_SECONDS = 24 * 60 * 60
MAX_BATCH_TARGETS = 10000
FOFA_SEARCH_URL = "https://fofa.info/api/v1/search/all"
FOFA_NEXT_URL = "https://fofa.info/api/v1/search/next"
FOFA_INFO_URL = "https://fofa.info/api/v1/info/my"
FOFA_STATS_URL = "https://fofa.info/api/v1/search/stats"
FOFA_HOST_BASE_URL = "https://fofa.info/api/v1/host/"

# --- 大洲国家代码 ---
CONTINENT_COUNTRIES = {
    'Asia': ['AF', 'AM', 'AZ', 'BH', 'BD', 'BT', 'BN', 'KH', 'CN', 'CY', 'GE', 'IN', 'ID', 'IR', 'IQ', 'IL', 'JP', 'JO', 'KZ', 'KW', 'KG', 'LA', 'LB', 'MY', 'MV', 'MN', 'MM', 'NP', 'KP', 'OM', 'PK', 'PS', 'PH', 'QA', 'SA', 'SG', 'KR', 'LK', 'SY', 'TW', 'TJ', 'TH', 'TL', 'TR', 'TM', 'AE', 'UZ', 'VN', 'YE'],
    'Europe': ['AL', 'AD', 'AM', 'AT', 'BY', 'BE', 'BA', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FO', 'FI', 'FR', 'GE', 'DE', 'GI', 'GR', 'HU', 'IS', 'IE', 'IT', 'KZ', 'LV', 'LI', 'LT', 'LU', 'MK', 'MT', 'MD', 'MC', 'ME', 'NL', 'NO', 'PL', 'PT', 'RO', 'RU', 'SM', 'RS', 'SK', 'SI', 'ES', 'SE', 'CH', 'TR', 'UA', 'GB', 'VA'],
    'NorthAmerica': ['AG', 'BS', 'BB', 'BZ', 'CA', 'CR', 'CU', 'DM', 'DO', 'SV', 'GD', 'GT', 'HT', 'HN', 'JM', 'MX', 'NI', 'PA', 'KN', 'LC', 'VC', 'TT', 'US'],
    'SouthAmerica': ['AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'GY', 'PY', 'PE', 'SR', 'UY', 'VE'],
    'Africa': ['DZ', 'AO', 'BJ', 'BW', 'BF', 'BI', 'CV', 'CM', 'CF', 'TD', 'KM', 'CD', 'CG', 'CI', 'DJ', 'EG', 'GQ', 'ER', 'SZ', 'ET', 'GA', 'GM', 'GH', 'GN', 'GW', 'KE', 'LS', 'LR', 'LY', 'MG', 'MW', 'ML', 'MR', 'MU', 'YT', 'MA', 'MZ', 'NA', 'NE', 'NG', 'RW', 'ST', 'SN', 'SC', 'SL', 'SO', 'ZA', 'SS', 'SD', 'TZ', 'TG', 'TN', 'UG', 'EH', 'ZM', 'ZW'],
    'Oceania': ['AS', 'AU', 'CK', 'FJ', 'PF', 'GU', 'KI', 'MH', 'FM', 'NR', 'NC', 'NZ', 'NU', 'NF', 'MP', 'PW', 'PG', 'PN', 'WS', 'SB', 'TK', 'TO', 'TV', 'VU', 'WF']
}
ALL_COUNTRY_CODES = sorted(list(set(code for countries in CONTINENT_COUNTRIES.values() for code in countries)))

# --- FOFA 字段定义 ---
FOFA_STATS_FIELDS = "protocol,domain,port,title,os,server,country,asn,org,asset_type,fid,icp"
FREE_FIELDS = ["ip", "port", "protocol", "country", "country_name", "region", "city", "longitude", "latitude", "asn", "org", "host", "domain", "os", "server", "icp", "title", "jarm", "header", "banner", "cert", "base_protocol", "link", "cert.issuer.org", "cert.issuer.cn", "cert.subject.org", "cert.subject.cn", "tls.ja3s", "tls.version", "cert.sn", "cert.not_before", "cert.not_after", "cert.domain"]
PERSONAL_FIELDS = FREE_FIELDS + ["header_hash", "banner_hash", "banner_fid"]
BUSINESS_FIELDS = PERSONAL_FIELDS + ["cname", "lastupdatetime", "product", "product_category", "version", "icon_hash", "cert.is_valid", "cname_domain", "body", "cert.is_match", "cert.is_equal"]
ENTERPRISE_FIELDS = BUSINESS_FIELDS + ["icon", "fid", "structinfo"]
FIELD_CATEGORIES = {
    "免费字段": FREE_FIELDS,
    "个人会员字段": list(set(PERSONAL_FIELDS) - set(FREE_FIELDS)),
    "商业版本字段": list(set(BUSINESS_FIELDS) - set(PERSONAL_FIELDS)),
    "企业版本字段": list(set(ENTERPRISE_FIELDS) - set(BUSINESS_FIELDS)),
}
KEY_LEVELS = {}

# --- 日志配置 ---
if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > (5 * 1024 * 1024):
    try: os.rename(LOG_FILE, LOG_FILE + '.old')
    except OSError as e: print(f"无法轮换日志文件: {e}")
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()]
)
logging.getLogger("requests").setLevel(logging.WARNING); logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- 会话状态定义 (v10.9.6 重构) ---
# 为每个独立的 ConversationHandler 分配唯一的状态范围，防止冲突

# 主菜单交互 (ReplyKeyboardMarkup)
STATE_AWAITING_QUERY, STATE_AWAITING_HOST = range(1, 3)

# /kkfofa 和 /allfofa 查询流程
(
    QUERY_STATE_GET_GUEST_KEY,
    QUERY_STATE_ASK_CONTINENT,
    QUERY_STATE_CONTINENT_CHOICE,
    QUERY_STATE_CACHE_CHOICE,
    QUERY_STATE_KKFOFA_MODE,
    QUERY_STATE_GET_TRACEBACK_LIMIT,
    QUERY_STATE_ALLFOFA_GET_LIMIT,
) = range(10, 17)

# /settings 设置流程
(
    SETTINGS_STATE_MAIN, SETTINGS_STATE_ACTION,
    SETTINGS_STATE_GET_KEY, SETTINGS_STATE_REMOVE_API,
    SETTINGS_STATE_PRESET_MENU, SETTINGS_STATE_GET_PRESET_NAME,
    SETTINGS_STATE_GET_PRESET_QUERY, SETTINGS_STATE_REMOVE_PRESET,
    SETTINGS_STATE_GET_UPDATE_URL, SETTINGS_STATE_PROXYPOOL_MENU,
    SETTINGS_STATE_GET_PROXY_ADD, SETTINGS_STATE_GET_PROXY_REMOVE,
    SETTINGS_STATE_UPLOAD_API_MENU, SETTINGS_STATE_GET_UPLOAD_URL,
    SETTINGS_STATE_GET_UPLOAD_TOKEN, SETTINGS_STATE_ADMIN_MENU,
    SETTINGS_STATE_GET_ADMIN_ID_TO_ADD, SETTINGS_STATE_GET_ADMIN_ID_TO_REMOVE,
) = range(20, 38)

# /batch 批量导出流程
(
    BATCH_STATE_SELECT_FIELDS,
    BATCH_STATE_MODE_CHOICE,
    BATCH_STATE_GET_LIMIT,
) = range(50, 53)

# /stats, /import, /batchfind, /restore, /batchcheckapi 等独立流程
(
    STATS_STATE_GET_QUERY,
    IMPORT_STATE_GET_FILE,
    BATCHFIND_STATE_GET_FILE, BATCHFIND_STATE_SELECT_FEATURES,
    RESTORE_STATE_GET_FILE,
    BATCHCHECKAPI_STATE_GET_FILE,
) = range(80, 86)

# /scan 扫描流程 (CallbackQueryHandler)
(
    SCAN_STATE_GET_CONCURRENCY,
    SCAN_STATE_GET_TIMEOUT,
) = range(100, 102)

# --- 配置管理 & 缓存 ---
def load_json_file(filename, default_content):
    if not os.path.exists(filename):
        with open(filename, 'w', encoding='utf-8') as f: json.dump(default_content, f, indent=4); return default_content
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            config = json.load(f)
            if isinstance(default_content, dict):
                for key, value in default_content.items(): config.setdefault(key, value)
            return config
    except (json.JSONDecodeError, IOError):
        logger.error(f"{filename} 损坏，将使用默认配置重建。");
        with open(filename, 'w', encoding='utf-8') as f: json.dump(default_content, f, indent=4); return default_content
def save_json_file(filename, data):
    with open(filename, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4, ensure_ascii=False)
DEFAULT_CONFIG = { "bot_token": "YOUR_BOT_TOKEN_HERE", "apis": [], "admins": [], "proxy": "", "proxies": [], "full_mode": False, "public_mode": False, "presets": [], "update_url": "", "upload_api_url": "", "upload_api_token": "" }
CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
HISTORY = load_json_file(HISTORY_FILE, {"queries": []})
ANONYMOUS_KEYS = load_json_file(ANONYMOUS_KEYS_FILE, {})
SCAN_TASKS = load_json_file(SCAN_TASKS_FILE, {})
MONITOR_TASKS = load_json_file(MONITOR_TASKS_FILE, {}) # 加载监控任务
def save_config(): save_json_file(CONFIG_FILE, CONFIG)
def save_anonymous_keys(): save_json_file(ANONYMOUS_KEYS_FILE, ANONYMOUS_KEYS)
def save_scan_tasks():
    logger.info(f"Saving {len(SCAN_TASKS)} scan tasks to {SCAN_TASKS_FILE}")
    save_json_file(SCAN_TASKS_FILE, SCAN_TASKS)
def save_monitor_tasks():
    save_json_file(MONITOR_TASKS_FILE, MONITOR_TASKS)
def add_or_update_query(query_text, cache_data=None):
    existing_query = next((q for q in HISTORY['queries'] if q['query_text'] == query_text), None)
    if existing_query:
        HISTORY['queries'].remove(existing_query); existing_query['timestamp'] = datetime.now(tz.tzutc()).isoformat()
        if cache_data: existing_query['cache'] = cache_data
        HISTORY['queries'].insert(0, existing_query)
    else:
        new_query = {"query_text": query_text, "timestamp": datetime.now(tz.tzutc()).isoformat(), "cache": cache_data}
        HISTORY['queries'].insert(0, new_query)
    while len(HISTORY['queries']) > MAX_HISTORY_SIZE: HISTORY['queries'].pop()
    save_json_file(HISTORY_FILE, HISTORY)
def find_cached_query(query_text):
    query = next((q for q in HISTORY['queries'] if q['query_text'] == query_text), None)
    if query and query.get('cache'):
        if 'file_path' in query['cache'] and os.path.exists(query['cache']['file_path']):
            return query
    return None

# --- 辅助函数与装饰器 ---
def generate_filename_from_query(query_text: str, prefix: str = "fofa", ext: str = ".txt") -> str:
    sanitized_query = re.sub(r'[^a-z0-9\-_]+', '_', query_text.lower()).strip('_')
    max_len = 100
    if len(sanitized_query) > max_len: sanitized_query = sanitized_query[:max_len].rsplit('_', 1)[0]
    timestamp = int(time.time()); return f"{prefix}_{sanitized_query}_{timestamp}{ext}"
def get_proxies(proxy_to_use=None):
    """
    返回一个代理配置字典。
    如果提供了 proxy_to_use，则专门使用它。
    否则，从代理池中随机选择一个。
    """
    proxy_str = proxy_to_use
    if proxy_str is None:
        proxies_list = CONFIG.get("proxies", [])
        if proxies_list:
            proxy_str = random.choice(proxies_list)
        else:
            proxy_str = CONFIG.get("proxy")
    
    if proxy_str:
        return {"http": proxy_str, "https": proxy_str}
    return None
def is_admin(user_id: int) -> bool: return user_id in CONFIG.get('admins', [])
def is_super_admin(user_id: int) -> bool:
    admins = CONFIG.get('admins', [])
    return admins and user_id == admins[0]

def admin_only(func):
    @wraps(func)
    def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        if not is_admin(update.effective_user.id):
            message_text = "⛔️ 抱歉，您没有权限执行此管理操作。"
            if update.callback_query: update.callback_query.answer(message_text, show_alert=True)
            elif update.message: update.message.reply_text(message_text)
            return None
        return func(update, context, *args, **kwargs)
    return wrapped
def escape_markdown_v2(text: str) -> str:
    if not isinstance(text, str): text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)
def create_progress_bar(percentage: float, length: int = 10) -> str:
    if percentage < 0: percentage = 0
    if percentage > 100: percentage = 100
    filled_length = int(length * percentage // 100)
    bar = '█' * filled_length + '░' * (length - filled_length)
    return f"[{bar}] {percentage:.1f}%"

# --- 文件上传辅助函数 ---
def send_file_safely(context: CallbackContext, chat_id: int, file_path: str, caption: str = "", parse_mode: str = None, filename: str = None):
    """安全地发送文件，处理Telegram API的大小限制。"""
    try:
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        TELEGRAM_MAX_FILE_SIZE_MB = 48

        if file_size_mb < TELEGRAM_MAX_FILE_SIZE_MB:
            with open(file_path, 'rb') as doc:
                context.bot.send_document(
                    chat_id, 
                    document=doc, 
                    filename=filename or os.path.basename(file_path), 
                    caption=caption, 
                    parse_mode=parse_mode, 
                    timeout=120 
                )
        else:
            message = (
                f"⚠️ *文件过大*\n\n"
                f"文件 `{escape_markdown_v2(filename or os.path.basename(file_path))}` \\({file_size_mb:.2f} MB\\) "
                f"超过了Telegram的发送限制 \\({TELEGRAM_MAX_FILE_SIZE_MB} MB\\)\\."
            )
            context.bot.send_message(chat_id, message, parse_mode=ParseMode.MARKDOWN_V2)
    except FileNotFoundError:
        logger.error(f"尝试发送文件失败: 文件未找到 at path {file_path}")
        context.bot.send_message(chat_id, f"❌ 内部错误: 尝试发送结果文件时找不到它。")
    except (TimedOut, NetworkError) as e:
        logger.error(f"发送文件 '{file_path}' 时出现网络错误或超时: {e}")
        context.bot.send_message(chat_id, f"⚠️ 发送文件时网络超时或出错。如果配置了外部上传，请检查那里的链接。")
    except Exception as e:
        logger.error(f"发送文件 '{file_path}' 时出现未知错误: {e}")
        context.bot.send_message(chat_id, f"⚠️ 发送文件时出现未知错误: `{escape_markdown_v2(str(e))}`", parse_mode=ParseMode.MARKDOWN_V2)

def upload_and_send_links(context: CallbackContext, chat_id: int, file_path: str):
    api_url = CONFIG.get("upload_api_url")
    api_token = CONFIG.get("upload_api_token")
    if not api_url or not api_token:
        logger.info("未配置上传API的URL或Token，跳过文件上传。")
        return
    try:
        with open(file_path, 'rb') as f:
            files = {'file': (os.path.basename(file_path), f)}
            headers = {'Authorization': api_token}
            response = requests.post(api_url, headers=headers, files=files, timeout=60, proxies=get_proxies())
            response.raise_for_status()
            result = response.json()
        if result and isinstance(result, list) and 'src' in result[0]:
            file_url_path = result[0]['src']
            parsed_main_url = urlparse(api_url)
            base_url = f"{parsed_main_url.scheme}://{parsed_main_url.netloc}"
            full_url = base_url + file_url_path
            file_name = os.path.basename(file_url_path)
            download_commands = (
                f"📥 *文件下载命令*\n\n"
                f"*cURL:*\n`curl -o \"{escape_markdown_v2(file_name)}\" \"{escape_markdown_v2(full_url)}\"`\n\n"
                f"*Wget:*\n`wget --content-disposition \"{escape_markdown_v2(full_url)}\"`"
            )
            context.bot.send_message(chat_id, download_commands, parse_mode=ParseMode.MARKDOWN_V2)
        else:
            raise ValueError(f"响应格式不正确: {result}")
    except Exception as e:
        logger.error(f"文件上传失败: {e}")
        context.bot.send_message(chat_id, f"⚠️ 文件上传到外部服务器失败: `{escape_markdown_v2(str(e))}`", parse_mode=ParseMode.MARKDOWN_V2)

# --- FOFA API 核心逻辑 ---
def _make_api_request(url, params, timeout=60, use_b64=True, retries=10, proxy_session=None):
    if use_b64 and 'q' in params:
        params['qbase64'] = base64.b64encode(params.pop('q').encode('utf-8')).decode('utf-8')
    
    last_error = None
    # v10.9.4 FIX: 为整个重试循环确定代理。
    # 如果传递了特定的会话，则使用它。否则，为此尝试获取一个随机的。
    request_proxies = get_proxies(proxy_to_use=proxy_session)

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=timeout, proxies=request_proxies, verify=False)
            if response.status_code == 429:
                wait_time = 5 * (attempt + 1)
                logger.warning(f"FOFA API rate limit hit (429). Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(wait_time)
                last_error = f"API请求因速率限制(429)失败"
                continue
            if response.status_code == 502: # Bad Gateway
                wait_time = 5 * (attempt + 1)
                logger.warning(f"FOFA API returned 502 Bad Gateway. Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(wait_time)
                last_error = "API请求失败 (502 Bad Gateway)"
                continue
            response.raise_for_status()
            data = response.json()
            if data.get("error"):
                return None, data.get("errmsg", "未知的FOFA错误")
            return data, None
        except requests.exceptions.RequestException as e:
            last_error = f"网络请求失败: {e}"
            logger.error(f"RequestException on attempt {attempt + 1}: {e}")
            time.sleep(5)
        except json.JSONDecodeError as e:
            last_error = f"解析JSON响应失败: {e}"
            break
    logger.error(f"API request failed after {retries} retries. Last error: {last_error}")
    return None, last_error if last_error else "API请求未知错误"
def verify_fofa_api(key): return _make_api_request(FOFA_INFO_URL, {'key': key}, timeout=15, use_b64=False, retries=3)
def fetch_fofa_data(key, query, page=1, page_size=10000, fields="host", proxy_session=None):
    query_lower = query.lower()
    if 'body=' in query_lower: page_size = min(page_size, 500)
    elif 'cert=' in query_lower: page_size = min(page_size, 2000)
    params = {'key': key, 'q': query, 'size': page_size, 'page': page, 'fields': fields, 'full': CONFIG.get("full_mode", False)}
    return _make_api_request(FOFA_SEARCH_URL, params, proxy_session=proxy_session)
def fetch_fofa_stats(key, query, proxy_session=None):
    params = {'key': key, 'q': query, 'fields': FOFA_STATS_FIELDS}
    return _make_api_request(FOFA_STATS_URL, params, proxy_session=proxy_session)
def fetch_fofa_host_info(key, host, detail=False, proxy_session=None):
    url = FOFA_HOST_BASE_URL + host
    params = {'key': key, 'detail': str(detail).lower()}
    return _make_api_request(url, params, use_b64=False, proxy_session=proxy_session)
def fetch_fofa_next_data(key, query, next_id=None, page_size=10000, fields="host", proxy_session=None):
    params = {'key': key, 'q': query, 'size': page_size, 'fields': fields, 'full': CONFIG.get("full_mode", False)}
    # FIX: Ensure 'next' parameter is always present, and empty on the first call, to comply with API spec.
    params['next'] = next_id if next_id is not None else ""
    return _make_api_request(FOFA_NEXT_URL, params, proxy_session=proxy_session)

# --- 智能下载核心工具 ---
def iter_fofa_traceback(key, query, limit=None, proxy_session=None, page_size=10000):
    """
    通过 before/after 时间回溯机制迭代获取数据的生成器。
    Yields: 结果列表
    """
    current_query = query
    last_page_date = None
    collected_count = 0
    
    # 简单的哈希去重（用于处理同一天的分页重叠）
    page_hashes = set() 
    
    while True:
        # 获取第一页
        # 注意：这里需要请求 lastupdatetime 以便确定下一页的 before 时间锚点
        # 为了兼容性，如果没有 VIP 权限，这个 fields 请求可能会被忽略或者需要外部确保 Key 权限
        # 这里假设调用此函数时已使用了具备权限的 Key
        fields = "host,lastupdatetime"
        
        # 使用 execute_query_with_fallback 的等价单次调用，或者直接调 fetch。
        # 这里是迭代器内部，假定 key 是确定的。
        # 如果 Key 等级 < 1 (无法查询 lastupdatetime)，则只能查普通翻页，这会导致大量数据下的死循环，
        # 所以外部必须确保 key level >= 1
        
        data, error = fetch_fofa_data(key, current_query, page=1, page_size=page_size, fields=fields, proxy_session=proxy_session)
        
        if error or not data or not data.get('results'):
            break

        results = data.get('results', [])
        if not results:
            break

        # Yield current batch
        # 我们返回完整结果以便外部处理
        yield results
        collected_count += len(results)
        if limit and collected_count >= limit:
            break

        # 分析最后一条的时间，设置新的 Time Anchor
        # FOFA 结果是倒序的，最后一条是最旧的
        # 取最后一条的时间，作为下一轮的 before
        valid_anchor_found = False
        
        # 倒序寻找有效时间戳
        for i in range(len(results) - 1, -1, -1):
            if not results[i] or len(results[i]) < 2: continue
            
            # 格式可能是 "2023-01-01 12:00:00"
            ts_str = results[i][-1] # lastupdatetime
            try:
                current_date_obj = datetime.strptime(ts_str.split(' ')[0], '%Y-%m-%d').date()
                
                # 防止死循环：如果这页找到的日期 >= 上一页找到的锚点日期，说明在这一天内卡住了
                # 我们需要强制将日期 -1 天来跳过这一天（会有数据损失，但好过死循环）
                # 或者，FOFA api 支持 page 翻页，如果是在同一天，我们可以尝试翻 page 2?
                # 简化起见：Time Slicing 策略是“天”级的。如果一天 > 10000 条，这里的逻辑会跳过当天剩余数据。
                # 但根据 Smart Slicing 假设，国家被剥离后，单日单国数据很难 > 10000。
                
                next_page_date_obj = current_date_obj
                
                if last_page_date and current_date_obj >= last_page_date:
                    # 如果时间没有前推，强制 -1 天
                    next_page_date_obj -= timedelta(days=1)
                
                last_page_date = next_page_date_obj
                
                # 更新查询：追加 before 参数
                # 注意处理 query 中现有的括号
                current_query = f'({query}) && before="{next_page_date_obj.strftime("%Y-%m-%d")}"'
                valid_anchor_found = True
                break
            except (ValueError, TypeError, IndexError):
                continue
        
        if not valid_anchor_found:
            break

def check_and_classify_keys():
    logger.info("--- 开始检查并分类API Keys ---")
    global KEY_LEVELS
    KEY_LEVELS.clear()
    for key in CONFIG.get('apis', []):
        data, error = verify_fofa_api(key)
        if error:
            logger.warning(f"Key '...{key[-4:]}' 无效: {error}")
            KEY_LEVELS[key] = -1
            continue
        is_vip = data.get('isvip', False)
        api_level = data.get('vip_level', 0)
        level = 0
        if not is_vip:
            level = 0
        else:
            if api_level == 2: level = 1
            elif api_level == 3: level = 2
            elif api_level >= 4: level = 3
            else: level = 1 
        KEY_LEVELS[key] = level
        level_name = {0: "免费会员", 1: "个人会员", 2: "商业会员", 3: "企业会员"}.get(level, "未知等级")
        logger.info(f"Key '...{key[-4:]}' ({data.get('username', 'N/A')}) - 等级: {level} ({level_name})")
    logger.info("--- API Keys 分类完成 ---")

def get_fields_by_level(level):
    if level >= 3: return ENTERPRISE_FIELDS
    if level == 2: return BUSINESS_FIELDS
    if level == 1: return PERSONAL_FIELDS
    return FREE_FIELDS

def execute_query_with_fallback(query_func, preferred_key_index=None, proxy_session=None, min_level=0):
    if not CONFIG['apis']: return None, None, None, None, None, "没有配置任何API Key。"
    
    keys_to_try = [k for k in CONFIG['apis'] if KEY_LEVELS.get(k, -1) >= min_level]
    
    if not keys_to_try:
        if min_level > 0:
            return None, None, None, None, None, f"没有找到等级不低于“个人会员”的有效API Key以执行此操作。"
        return None, None, None, None, None, "所有配置的API Key都无效。"
    
    start_index = 0
    if preferred_key_index is not None and 1 <= preferred_key_index <= len(CONFIG['apis']):
        preferred_key = CONFIG['apis'][preferred_key_index - 1]
        if preferred_key in keys_to_try:
            start_index = keys_to_try.index(preferred_key)

    # v10.9.4 FIX: 如果未锁定代理会话，则在此回退序列的持续时间内选择一个。
    current_proxy_session_str = proxy_session
    if current_proxy_session_str is None:
        proxies_list = CONFIG.get("proxies", [])
        if proxies_list:
            current_proxy_session_str = random.choice(proxies_list)
        else:
            current_proxy_session_str = CONFIG.get("proxy")

    for i in range(len(keys_to_try)):
        idx = (start_index + i) % len(keys_to_try)
        key = keys_to_try[idx]
        key_num = CONFIG['apis'].index(key) + 1
        key_level = KEY_LEVELS.get(key, 0)
        
        # v10.9.4 FIX: 将key、key_level和一致的proxy_session传递给查询函数。
        data, error = query_func(key, key_level, current_proxy_session_str)
        
        if not error:
            # 返回成功使用的代理。
            return data, key, key_num, key_level, current_proxy_session_str, None
        if "[820031]" in str(error):
            logger.warning(f"Key [#{key_num}] F点余额不足...");
            continue
        # 对于其他错误，快速失败并返回问题key的信息
        return None, key, key_num, key_level, current_proxy_session_str, error
        
    return None, None, None, None, None, "所有Key均尝试失败 (可能F点均不足)。"

# --- 异步扫描逻辑 ---
async def async_check_port(host, port, timeout):
    try:
        fut = asyncio.open_connection(host, port)
        _, writer = await asyncio.wait_for(fut, timeout=timeout)
        writer.close(); await writer.wait_closed()
        return f"{host}:{port}"
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError, socket.gaierror): return None
    except Exception: return None

async def async_scanner_orchestrator(targets, concurrency, timeout, mode='tcping', progress_callback=None):
    semaphore = asyncio.Semaphore(concurrency)
    scan_targets = []
    if mode == 'tcping':
        for t in targets:
            try:
                host, port_str = t.split(':', 1)
                scan_targets.append((host, int(port_str)))
            except (ValueError, IndexError): continue
    elif mode == 'subnet':
        subnets_to_ports = {}
        for line in targets:
            try:
                ip_str, port_str = line.strip().split(':'); port = int(port_str)
                subnet = ".".join(ip_str.split('.')[:3])
                if subnet not in subnets_to_ports: subnets_to_ports[subnet] = set()
                subnets_to_ports[subnet].add(port)
            except ValueError: continue
        for subnet, ports in subnets_to_ports.items():
            for i in range(1, 255):
                for port in ports:
                    scan_targets.append((f"{subnet}.{i}", port))

    total_tasks = len(scan_targets)
    completed_tasks = 0
    
    async def worker(host, port):
        nonlocal completed_tasks
        async with semaphore:
            result = await async_check_port(host, port, timeout)
            completed_tasks += 1
            if progress_callback:
                await progress_callback(completed_tasks, total_tasks)
            return result

    tasks = [worker(host, port) for host, port in scan_targets]
    results = await asyncio.gather(*tasks)
    return [res for res in results if res is not None]

def run_async_scan_job(context: CallbackContext):
    job_context = context.job.context
    chat_id, msg, original_query, mode = job_context['chat_id'], job_context['msg'], job_context['original_query'], job_context['mode']
    concurrency, timeout = job_context['concurrency'], job_context['timeout']
    
    cached_item = find_cached_query(original_query)
    if not cached_item:
        try: msg.edit_text("❌ 找不到结果文件的本地缓存记录。")
        except (BadRequest, RetryAfter, TimedOut): pass
        return

    try: msg.edit_text("1/3: 正在读取本地缓存文件...")
    except (BadRequest, RetryAfter, TimedOut): pass
    
    try:
        with open(cached_item['cache']['file_path'], 'r', encoding='utf-8') as f:
            targets = [line.strip() for line in f if ':' in line.strip()]
    except Exception as e:
        try: msg.edit_text(f"❌ 读取缓存文件失败: {e}")
        except (BadRequest, RetryAfter, TimedOut): pass
        return

    scan_type_text = "TCP存活扫描" if mode == 'tcping' else "子网扫描"
    
    async def main_scan_logic():
        last_update_time = 0
        
        async def progress_callback(completed, total):
            nonlocal last_update_time
            current_time = time.time()
            if total > 0 and current_time - last_update_time > 2:
                percentage = (completed / total) * 100
                progress_bar = create_progress_bar(percentage)
                try:
                    msg.edit_text(
                        f"2/3: 正在进行异步{scan_type_text}...\n"
                        f"{progress_bar} ({completed}/{total})"
                    )
                    last_update_time = current_time
                except (BadRequest, RetryAfter, TimedOut):
                    pass # Ignore if editing fails, continue scanning

        initial_message = f"2/3: 已加载 {len(targets)} 个目标，开始异步{scan_type_text} (并发: {concurrency}, 超时: {timeout}s)..."
        try:
            msg.edit_text(initial_message)
        except (BadRequest, RetryAfter, TimedOut):
            pass

        return await async_scanner_orchestrator(targets, concurrency, timeout, mode, progress_callback)

    live_results = asyncio.run(main_scan_logic())
    
    if not live_results:
        try: msg.edit_text("🤷‍♀️ 扫描完成，但未发现任何存活的目标。")
        except (BadRequest, RetryAfter, TimedOut): pass
        return

    try: msg.edit_text("3/3: 正在打包并发送新结果...")
    except (BadRequest, RetryAfter, TimedOut): pass
    
    output_filename = generate_filename_from_query(original_query, prefix=f"{mode}_scan")
    with open(output_filename, 'w', encoding='utf-8') as f: f.write("\n".join(sorted(list(live_results))))
    
    final_caption = f"✅ *异步{escape_markdown_v2(scan_type_text)}完成\!*\n\n共发现 *{len(live_results)}* 个存活目标\\."
    send_file_safely(context, chat_id, output_filename, caption=final_caption, parse_mode=ParseMode.MARKDOWN_V2)
    upload_and_send_links(context, chat_id, output_filename)
    os.remove(output_filename)
    try: msg.delete()
    except (BadRequest, RetryAfter, TimedOut): pass

# --- 扫描流程入口 ---
def offer_post_download_actions(context: CallbackContext, chat_id, query_text):
    query_hash = hashlib.md5(query_text.encode()).hexdigest()
    SCAN_TASKS[query_hash] = query_text
    while len(SCAN_TASKS) > MAX_SCAN_TASKS:
        SCAN_TASKS.pop(next(iter(SCAN_TASKS)))
    save_scan_tasks()

    keyboard = [[
        InlineKeyboardButton("⚡️ 异步TCP存活扫描", callback_data=f'start_scan_tcping_{query_hash}'),
        InlineKeyboardButton("🌐 异步子网扫描(/24)", callback_data=f'start_scan_subnet_{query_hash}')
    ]]
    context.bot.send_message(chat_id, "下载完成，需要对结果进行二次扫描吗？", reply_markup=InlineKeyboardMarkup(keyboard))
def start_scan_callback(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; query.answer()
    # v10.9.1 FIX: Correctly parse callback data to get mode and query_hash
    try:
        _, _, mode, query_hash = query.data.split('_', 3)
    except ValueError:
        logger.error(f"无法从回调数据解析扫描任务: {query.data}")
        query.message.edit_text("❌ 内部错误：无法解析扫描任务。")
        return ConversationHandler.END

    original_query = SCAN_TASKS.get(query_hash)
    if not original_query:
        query.message.edit_text("❌ 扫描任务已过期或机器人刚刚重启。请重新发起查询以启用扫描。")
        return ConversationHandler.END

    context.user_data['scan_original_query'] = original_query
    context.user_data['scan_mode'] = mode
    query.message.edit_text("请输入扫描并发数 (建议 100-1000):")
    return SCAN_STATE_GET_CONCURRENCY
def get_concurrency_callback(update: Update, context: CallbackContext) -> int:
    try:
        concurrency = int(update.message.text)
        if not 1 <= concurrency <= 5000: raise ValueError
        context.user_data['scan_concurrency'] = concurrency
        update.message.reply_text("请输入连接超时时间 (秒, 建议 1-3):")
        return SCAN_STATE_GET_TIMEOUT
    except ValueError:
        update.message.reply_text("无效输入，请输入 1-5000 之间的整数。")
        return SCAN_STATE_GET_CONCURRENCY
def get_timeout_callback(update: Update, context: CallbackContext) -> int:
    try:
        timeout = float(update.message.text)
        if not 0.1 <= timeout <= 10: raise ValueError
        msg = update.message.reply_text("✅ 参数设置完毕，任务已提交到后台。")
        job_context = {
            'chat_id': update.effective_chat.id, 'msg': msg,
            'original_query': context.user_data['scan_original_query'],
            'mode': context.user_data['scan_mode'],
            'concurrency': context.user_data['scan_concurrency'],
            'timeout': timeout
        }
        context.job_queue.run_once(run_async_scan_job, 1, context=job_context, name=f"scan_{update.effective_chat.id}")
        context.user_data.clear()
        return ConversationHandler.END
    except ValueError:
        update.message.reply_text("无效输入，请输入 0.1-10 之间的数字。")
        return SCAN_STATE_GET_TIMEOUT

# --- 后台下载任务 ---
def start_download_job(context: CallbackContext, callback_func, job_data):
    chat_id = job_data['chat_id']; job_name = f"download_job_{chat_id}"
    for job in context.job_queue.get_jobs_by_name(job_name): job.schedule_removal()
    context.bot_data.pop(f'stop_job_{chat_id}', None)
    context.job_queue.run_once(callback_func, 1, context=job_data, name=job_name)
def run_full_download_query(context: CallbackContext):
    job_data = context.job.context; bot, chat_id, query_text, total_size = context.bot, job_data['chat_id'], job_data['query'], job_data['total_size']
    output_filename = generate_filename_from_query(query_text); unique_results, stop_flag = set(), f'stop_job_{chat_id}'
    msg = bot.send_message(chat_id, "⏳ 开始全量下载任务..."); pages_to_fetch = (total_size + 9999) // 10000
    for page in range(1, pages_to_fetch + 1):
        if context.bot_data.get(stop_flag): msg.edit_text("🌀 下载任务已手动停止."); break
        try: msg.edit_text(f"下载进度: {len(unique_results)}/{total_size} (Page {page}/{pages_to_fetch})...")
        except (BadRequest, RetryAfter, TimedOut): pass
        guest_key = job_data.get('guest_key')
        if guest_key:
            data, error = fetch_fofa_data(guest_key, query_text, page, 10000, "host")
        else:
            data, _, _, _, _, error = execute_query_with_fallback(
                lambda key, key_level, proxy_session: fetch_fofa_data(key, query_text, page, 10000, "host", proxy_session=proxy_session)
            )
        if error: msg.edit_text(f"❌ 第 {page} 页下载出错: {error}"); break
        results = data.get('results', []);
        if not results: break
        unique_results.update(res for res in results if ':' in res)
    if unique_results:
        with open(output_filename, 'w', encoding='utf-8') as f: f.write("\n".join(unique_results))
        msg.edit_text(f"✅ 下载完成！共 {len(unique_results)} 条。正在发送...")
        cache_path = os.path.join(FOFA_CACHE_DIR, output_filename)
        shutil.move(output_filename, cache_path)
        send_file_safely(context, chat_id, cache_path, filename=output_filename)
        upload_and_send_links(context, chat_id, cache_path)
        cache_data = {'file_path': cache_path, 'result_count': len(unique_results)}
        add_or_update_query(query_text, cache_data); offer_post_download_actions(context, chat_id, query_text)
    elif not context.bot_data.get(stop_flag): msg.edit_text("🤷‍♀️ 任务完成，但未能下载到任何数据。")
    context.bot_data.pop(stop_flag, None)

def run_sharded_download_job(context: CallbackContext):
    """
    智能分片下载任务：按国家代码将查询拆分，绕过单次查询10000条的限制。
    """
    job_data = context.job.context
    bot, chat_id, base_query = context.bot, job_data['chat_id'], job_data['query']
    
    output_filename = generate_filename_from_query(base_query, prefix="sharded")
    unique_results = set()
    stop_flag = f'stop_job_{chat_id}'
    
    msg = bot.send_message(chat_id, f"⏳ *启动智能分片下载*\n目标：将查询按 {len(ALL_COUNTRY_CODES)} 个国家区域拆分...\n注意：此模式将消耗较多的 API 请求次数。", parse_mode=ParseMode.MARKDOWN_V2)
    
    start_time = time.time()
    last_ui_update_time = 0
    total_codes = len(ALL_COUNTRY_CODES)
    
    # 遍历所有国家
    for i, country_code in enumerate(ALL_COUNTRY_CODES):
        if context.bot_data.get(stop_flag):
            try: msg.edit_text("🛑 任务已手动停止。")
            except (BadRequest, RetryAfter, TimedOut): pass
            break
            
        current_time = time.time()
        # 更新进度UI (每2秒最多更新一次)
        if current_time - last_ui_update_time > 2 or i == 0:
            elapsed = current_time - start_time
            speed = len(unique_results) / elapsed if elapsed > 0 else 0
            progress_bar = create_progress_bar((i / total_codes) * 100)
            try:
                msg.edit_text(
                    f"🌍 *正在分片扫描...* `{country_code}`\n"
                    f"{progress_bar} {i}/{total_codes}\n"
                    f"已收集数据: *{len(unique_results)}* 条\n"
                    f"当前平均速度: *{int(speed)}* 条/秒",
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                last_ui_update_time = current_time
            except (BadRequest, RetryAfter, TimedOut):
                pass

        # 构造分片查询
        sharded_query = f'({base_query}) && country="{country_code}"'
        
        # 内部查询函数
        def query_logic(key, key_level, proxy_session):
            # 为了节省流量和速度，默认只请求第一页 (max 10000 per country is usually enough for most cases)
            return fetch_fofa_data(key, sharded_query, page=1, page_size=10000, fields="host", proxy_session=proxy_session)

        # 尝试查询
        guest_key = job_data.get('guest_key')
        if guest_key:
            data, error = fetch_fofa_data(guest_key, sharded_query, page=1, page_size=10000, fields="host")
        else:
            data, _, _, _, _, error = execute_query_with_fallback(query_logic)
        
        # 处理结果
        if not error and data and data.get('results'):
            new_data = data['results']
            # 处理简单字符串结果或列表结果
            extracted_hosts = []
            if new_data and isinstance(new_data[0], list):
                 extracted_hosts = [r[0] for r in new_data if r and r[0] and ':' in r[0]]
            else:
                 extracted_hosts = [r for r in new_data if isinstance(r, str) and ':' in r]
            
            unique_results.update(extracted_hosts)
            
            # (可选优化) 如果单个国家结果也是满的 10000，理想情况应该再对该国家按 region 分片
            # 但这里为了避免无限递归，暂时接受单个分片 10000 的上限。对于绝大多数国家已足够。

    # 循环结束后的收尾
    context.bot_data.pop(stop_flag, None)
    
    if unique_results:
        final_count = len(unique_results)
        msg.edit_text(f"✅ 分片扫描完成！\n总计发现 *{final_count}* 条唯一数据。\n正在生成并发送文件...", parse_mode=ParseMode.MARKDOWN_V2)
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write("\n".join(sorted(list(unique_results))))
            
        cache_path = os.path.join(FOFA_CACHE_DIR, output_filename)
        shutil.move(output_filename, cache_path)
        send_file_safely(context, chat_id, cache_path, filename=output_filename)
        upload_and_send_links(context, chat_id, cache_path)
        
        cache_data = {'file_path': cache_path, 'result_count': final_count}
        add_or_update_query(base_query, cache_data)
        offer_post_download_actions(context, chat_id, base_query)
    else:
        msg.edit_text("🤷‍♀️ 任务完成，但在任何国家分片中都未找到数据。")

def run_traceback_download_query(context: CallbackContext):
    job_data = context.job.context; bot, chat_id, base_query = context.bot, job_data['chat_id'], job_data['query']; limit = job_data.get('limit')
    output_filename = generate_filename_from_query(base_query); unique_results, page_count, last_page_date, termination_reason, stop_flag, last_update_time = set(), 0, None, "", f'stop_job_{chat_id}', 0
    msg = bot.send_message(chat_id, "⏳ 开始深度追溯下载...")
    current_query = base_query
    guest_key = job_data.get('guest_key')
    
    # v10.9.4 FIX: 为整个追溯过程锁定一个代理会话
    locked_proxy_session = None

    while True:
        page_count += 1
        if context.bot_data.get(stop_flag): termination_reason = "\n\n🌀 任务已手动停止."; break

        fields_were_extended = False
        if guest_key:
            # Guest keys are assumed to be low-level, don't request lastupdatetime
            data, error = fetch_fofa_data(guest_key, current_query, 1, 10000, fields="host")
        else:
            def query_logic(key, key_level, proxy_session):
                nonlocal fields_were_extended
                # Personal members and above can search this field.
                if key_level >= 1:
                    fields_were_extended = True
                    return fetch_fofa_data(key, current_query, 1, 10000, fields="host,lastupdatetime", proxy_session=proxy_session)
                else:
                    fields_were_extended = False
                    return fetch_fofa_data(key, current_query, 1, 10000, fields="host", proxy_session=proxy_session)
            
            # 仅在第一次迭代时选择并锁定代理
            if locked_proxy_session is None:
                data, _, _, _, locked_proxy_session, error = execute_query_with_fallback(query_logic)
            else:
                data, _, _, _, _, error = execute_query_with_fallback(query_logic, proxy_session=locked_proxy_session)

        if error: termination_reason = f"\n\n❌ 第 {page_count} 轮出错: {error}"; break
        results = data.get('results', [])
        if not results: termination_reason = "\n\nℹ️ 已获取所有查询结果."; break

        if fields_were_extended:
            newly_added = [r[0] for r in results if r and r[0] and ':' in r[0]]
        else:
            newly_added = [r for r in results if r and ':' in r]
        
        original_count = len(unique_results)
        unique_results.update(newly_added)
        newly_added_count = len(unique_results) - original_count

        if limit and len(unique_results) >= limit: unique_results = set(list(unique_results)[:limit]); termination_reason = f"\n\nℹ️ 已达到您设置的 {limit} 条结果上限。"; break
        current_time = time.time()
        if current_time - last_update_time > 2:
            try: msg.edit_text(f"⏳ 已找到 {len(unique_results)} 条... (第 {page_count} 轮, 新增 {newly_added_count})")
            except (BadRequest, RetryAfter, TimedOut): pass
            last_update_time = current_time

        if not fields_were_extended:
             termination_reason = "\n\n⚠️ 当前Key等级不支持时间追溯，已获取第一页结果。"
             break
        
        valid_anchor_found = False
        for i in range(len(results) - 1, -1, -1):
            if not results[i] or len(results[i]) < 2 or not results[i][1]: continue
            try:
                timestamp_str = results[i][1]; current_date_obj = datetime.strptime(timestamp_str.split(' ')[0], '%Y-%m-%d').date()
                if last_page_date and current_date_obj >= last_page_date: continue
                next_page_date_obj = current_date_obj
                if last_page_date and current_date_obj == last_page_date: next_page_date_obj -= timedelta(days=1)
                last_page_date = current_date_obj; current_query = f'({base_query}) && before="{next_page_date_obj.strftime("%Y-%m-%d")}"'; valid_anchor_found = True
                break
            except (ValueError, TypeError): continue
        if not valid_anchor_found: termination_reason = "\n\n⚠️ 无法找到有效的时间锚点以继续，可能已达查询边界."; break
    if unique_results:
        with open(output_filename, 'w', encoding='utf-8') as f: f.write("\n".join(sorted(list(unique_results))))
        msg.edit_text(f"✅ 深度追溯完成！共 {len(unique_results)} 条。{termination_reason}\n正在发送文件...")
        cache_path = os.path.join(FOFA_CACHE_DIR, output_filename)
        shutil.move(output_filename, cache_path)
        send_file_safely(context, chat_id, cache_path, filename=output_filename)
        upload_and_send_links(context, chat_id, cache_path)
        cache_data = {'file_path': cache_path, 'result_count': len(unique_results)}
        add_or_update_query(base_query, cache_data); offer_post_download_actions(context, chat_id, base_query)
    else: msg.edit_text(f"🤷‍♀️ 任务完成，但未能下载到任何数据。{termination_reason}")
    context.bot_data.pop(stop_flag, None)
def run_incremental_update_query(context: CallbackContext):
    job_data = context.job.context; bot, chat_id, base_query = context.bot, job_data['chat_id'], job_data['query']; msg = bot.send_message(chat_id, "--- 增量更新启动 ---")
    try: msg.edit_text("1/5: 正在获取旧缓存...")
    except (BadRequest, RetryAfter, TimedOut): pass
    cached_item = find_cached_query(base_query)
    if not cached_item: msg.edit_text("❌ 错误：找不到本地缓存项。"); return
    old_file_path = cached_item['cache']['file_path']; old_results = set()
    try:
        with open(old_file_path, 'r', encoding='utf-8') as f: old_results = set(line.strip() for line in f if line.strip() and ':' in line)
    except Exception as e: msg.edit_text(f"❌ 读取本地缓存文件失败: {e}"); return
    try: msg.edit_text("2/5: 正在确定更新起始点...")
    except (BadRequest, RetryAfter, TimedOut): pass
    data, _, _, _, _, error = execute_query_with_fallback(
        lambda key, key_level, proxy_session: fetch_fofa_data(key, base_query, fields="lastupdatetime", proxy_session=proxy_session)
    )
    if error or not data.get('results'): msg.edit_text(f"❌ 无法获取最新记录时间戳: {error or '无结果'}"); return
    ts_str = data['results'][0][0] if isinstance(data['results'][0], list) else data['results'][0]; cutoff_date = ts_str.split(' ')[0]
    incremental_query = f'({base_query}) && after="{cutoff_date}"'
    try: msg.edit_text(f"3/5: 正在侦察自 {cutoff_date} 以来的新数据...")
    except (BadRequest, RetryAfter, TimedOut): pass
    data, _, _, _, _, error = execute_query_with_fallback(
        lambda key, key_level, proxy_session: fetch_fofa_data(key, incremental_query, page_size=1, proxy_session=proxy_session)
    )
    if error: msg.edit_text(f"❌ 侦察查询失败: {error}"); return
    total_new_size = data.get('size', 0)
    if total_new_size == 0: msg.edit_text("✅ 未发现新数据。缓存已是最新。"); return
    new_results, stop_flag = set(), f'stop_job_{chat_id}'; pages_to_fetch = (total_new_size + 9999) // 10000
    for page in range(1, pages_to_fetch + 1):
        if context.bot_data.get(stop_flag):
            try: msg.edit_text("🌀 增量更新已手动停止。")
            except (BadRequest, RetryAfter, TimedOut): pass
            return
        try: msg.edit_text(f"3/5: 正在下载新数据... ( Page {page}/{pages_to_fetch} )")
        except (BadRequest, RetryAfter, TimedOut): pass
        data, _, _, _, _, error = execute_query_with_fallback(
            lambda key, key_level, proxy_session: fetch_fofa_data(key, incremental_query, page=page, page_size=10000, proxy_session=proxy_session)
        )
        if error: msg.edit_text(f"❌ 下载新数据失败: {error}"); return
        if data.get('results'): new_results.update(res for res in data.get('results', []) if ':' in res)
    try: msg.edit_text(f"4/5: 正在合并数据... (发现 {len(new_results)} 条新数据)")
    except (BadRequest, RetryAfter, TimedOut): pass
    combined_results = sorted(list(new_results.union(old_results)))
    with open(old_file_path, 'w', encoding='utf-8') as f: f.write("\n".join(combined_results))
    try: msg.edit_text(f"5/5: 发送更新后的文件... (共 {len(combined_results)} 条)")
    except (BadRequest, RetryAfter, TimedOut): pass
    send_file_safely(context, chat_id, old_file_path)
    upload_and_send_links(context, chat_id, old_file_path)
    cache_data = {'file_path': old_file_path, 'result_count': len(combined_results)}
    add_or_update_query(base_query, cache_data)
    msg.delete(); bot.send_message(chat_id, f"✅ 增量更新完成！"); offer_post_download_actions(context, chat_id, base_query)
def run_batch_download_query(context: CallbackContext):
    job_data = context.job.context; bot, chat_id, query_text, total_size, fields = context.bot, job_data['chat_id'], job_data['query'], job_data['total_size'], job_data['fields']
    output_filename = generate_filename_from_query(query_text, prefix="batch_export", ext=".csv"); results_list, stop_flag = [], f'stop_job_{chat_id}'
    msg = bot.send_message(chat_id, "⏳ 开始自定义字段批量导出任务..."); pages_to_fetch = (total_size + 9999) // 10000
    for page in range(1, pages_to_fetch + 1):
        if context.bot_data.get(stop_flag): msg.edit_text("🌀 下载任务已手动停止."); break
        try: msg.edit_text(f"下载进度: {len(results_list)}/{total_size} (Page {page}/{pages_to_fetch})...")
        except (BadRequest, RetryAfter, TimedOut): pass
        data, _, _, _, _, error = execute_query_with_fallback(
            lambda key, key_level, proxy_session: fetch_fofa_data(key, query_text, page, 10000, fields, proxy_session=proxy_session)
        )
        if error: msg.edit_text(f"❌ 第 {page} 页下载出错: {error}"); break
        page_results = data.get('results', [])
        if not page_results: break
        results_list.extend(page_results)
    if results_list:
        msg.edit_text(f"✅ 下载完成！共 {len(results_list)} 条。正在生成CSV文件...")
        try:
            with open(output_filename, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f); writer.writerow(fields.split(',')); writer.writerows(results_list)
            send_file_safely(context, chat_id, output_filename, caption=f"✅ 自定义导出完成\n查询: `{escape_markdown_v2(query_text)}`", parse_mode=ParseMode.MARKDOWN_V2)
            upload_and_send_links(context, chat_id, output_filename)
        except Exception as e:
            msg.edit_text(f"❌ 生成或发送CSV文件失败: {e}"); logger.error(f"Failed to generate/send CSV for batch command: {e}")
        finally:
            if os.path.exists(output_filename): os.remove(output_filename)
            msg.delete()
    elif not context.bot_data.get(stop_flag): msg.edit_text("🤷‍♀️ 任务完成，但未能下载到任何数据。")
    context.bot_data.pop(stop_flag, None)
def run_batch_traceback_query(context: CallbackContext):
    job_data = context.job.context; bot, chat_id, base_query, fields, limit = context.bot, job_data['chat_id'], job_data['query'], job_data['fields'], job_data.get('limit')
    output_filename = generate_filename_from_query(base_query, prefix="batch_traceback", ext=".csv")
    unique_results, page_count, last_page_date, termination_reason, stop_flag, last_update_time = [], 0, None, "", f'stop_job_{chat_id}', 0
    msg = bot.send_message(chat_id, "⏳ 开始自定义字段深度追溯下载...")
    current_query = base_query; seen_hashes = set()
    
    # v10.9.4 FIX: 为整个追溯过程锁定一个代理会话
    locked_proxy_session = None

    while True:
        page_count += 1
        if context.bot_data.get(stop_flag): termination_reason = "\n\n🌀 任务已手动停止."; break
        
        fields_were_extended = False
        def query_logic(key, key_level, proxy_session):
            nonlocal fields_were_extended
            if key_level >= 1:
                fields_were_extended = True
                return fetch_fofa_data(key, current_query, 1, 10000, fields=fields + ",lastupdatetime", proxy_session=proxy_session)
            else:
                fields_were_extended = False
                return fetch_fofa_data(key, current_query, 1, 10000, fields=fields, proxy_session=proxy_session)

        # 仅在第一次迭代时选择并锁定代理
        if locked_proxy_session is None:
            data, _, _, _, locked_proxy_session, error = execute_query_with_fallback(query_logic)
        else:
            data, _, _, _, _, error = execute_query_with_fallback(query_logic, proxy_session=locked_proxy_session)

        if error: termination_reason = f"\n\n❌ 第 {page_count} 轮出错: {error}"; break
        results = data.get('results', [])
        if not results: termination_reason = "\n\nℹ️ 已获取所有查询结果."; break

        newly_added_count = 0
        for r in results:
            r_hash = hashlib.md5(str(r).encode()).hexdigest()
            if r_hash not in seen_hashes:
                seen_hashes.add(r_hash)
                unique_results.append(r[:-1] if fields_were_extended else r)
                newly_added_count += 1
        if limit and len(unique_results) >= limit: unique_results = unique_results[:limit]; termination_reason = f"\n\nℹ️ 已达到您设置的 {limit} 条结果上限。"; break
        current_time = time.time()
        if current_time - last_update_time > 2:
            try: msg.edit_text(f"⏳ 已找到 {len(unique_results)} 条... (第 {page_count} 轮, 新增 {newly_added_count})")
            except (BadRequest, RetryAfter, TimedOut): pass
            last_update_time = current_time

        if not fields_were_extended:
             termination_reason = "\n\n⚠️ 当前Key等级不支持时间追溯，已获取第一页结果。"
             break
        
        valid_anchor_found = False
        for i in range(len(results) - 1, -1, -1):
            if not results[i] or len(results[i]) < 2 or not results[i][-1]: continue
            try:
                timestamp_str = results[i][-1]; current_date_obj = datetime.strptime(timestamp_str.split(' ')[0], '%Y-%m-%d').date()
                if last_page_date and current_date_obj >= last_page_date: continue
                next_page_date_obj = current_date_obj
                if last_page_date and current_date_obj == last_page_date: next_page_date_obj -= timedelta(days=1)
                last_page_date = current_date_obj; current_query = f'({base_query}) && before="{next_page_date_obj.strftime("%Y-%m-%d")}"'; valid_anchor_found = True
                break
            except (ValueError, TypeError): continue
        if not valid_anchor_found: termination_reason = "\n\n⚠️ 无法找到有效的时间锚点以继续，可能已达查询边界."; break
    if unique_results:
        msg.edit_text(f"✅ 追溯完成！共 {len(unique_results)} 条。{termination_reason}\n正在生成CSV...")
        try:
            with open(output_filename, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f); writer.writerow(fields.split(',')); writer.writerows(unique_results)
            send_file_safely(context, chat_id, output_filename)
            upload_and_send_links(context, chat_id, output_filename)
        except Exception as e:
            msg.edit_text(f"❌ 生成或发送CSV文件失败: {e}"); logger.error(f"Failed to generate/send CSV for batch traceback: {e}")
        finally:
            if os.path.exists(output_filename): os.remove(output_filename)
            msg.delete()
    else: msg.edit_text(f"🤷‍♀️ 任务完成，但未能下载到任何数据。{termination_reason}")
    context.bot_data.pop(stop_flag, None)

# --- 监控系统 (Data Reservoir + Radar Mode) ---
def monitor_command(update: Update, context: CallbackContext):
    args = context.args
    if not args:
        help_txt = (
            "📡 *监控雷达指令手册*\n\n"
            "`/monitor add <query>` \\- 添加新的监控任务\n"
            "`/monitor list` \\- 查看当前运行的任务\n"
            "`/monitor get <id>` \\- 打包提取任务数据\n"
            "`/monitor del <id>` \\- 删除监控任务\n\n"
            "_监控任务会将新数据自动沉淀到本地数据库，您随时可以提取。_"
        )
        update.message.reply_text(help_txt, parse_mode=ParseMode.MARKDOWN_V2)
        return

    sub_cmd = args[0].lower()
    
    if sub_cmd == 'add':
        if len(args) < 2:
            update.message.reply_text("用法: `/monitor add <query>`")
            return
        query_text = " ".join(args[1:])
        # 生成简短ID
        task_id = hashlib.md5(query_text.encode()).hexdigest()[:8]
        
        if task_id in MONITOR_TASKS:
            update.message.reply_text(f"⚠️ 任务已存在 (ID: `{task_id}`)", parse_mode=ParseMode.MARKDOWN_V2)
            return
            
        MONITOR_TASKS[task_id] = {
            "query": query_text,
            "chat_id": update.effective_chat.id,
            "added_at": int(time.time()),
            "last_run": 0,
            "interval": 3600, # 初始1小时
            "status": "active"
        }
        save_monitor_tasks()
        
        # 立即启动第一次调度 (Use Jitter 0 for first run)
        context.job_queue.run_once(run_monitor_execution_job, 1, context={"task_id": task_id}, name=f"monitor_{task_id}")
        update.message.reply_text(f"✅ 监控雷达已启动\nID: `{task_id}`\n查询: `{escape_markdown_v2(query_text)}`\n\n数据将自动沉淀，使用 `/monitor get {task_id}` 提取。", parse_mode=ParseMode.MARKDOWN_V2)

    elif sub_cmd == 'list':
        if not MONITOR_TASKS:
            update.message.reply_text("📭 当前没有活跃的监控任务。")
            return
        msg = ["*📡 活跃监控任务*"]
        for tid, task in MONITOR_TASKS.items():
            if task.get('status') != 'active': continue
            
            # 统计本地数据
            data_file = os.path.join(MONITOR_DATA_DIR, f"{tid}.txt")
            count = 0
            if os.path.exists(data_file):
                try: 
                    with open(data_file, 'r', encoding='utf-8') as f: count = sum(1 for _ in f)
                except: pass
                
            last_run_str = "等待中"
            if task.get('last_run'):
                dt = datetime.fromtimestamp(task['last_run']).replace(tzinfo=tz.tzlocal())
                last_run_str = dt.strftime('%H:%M')
            
            # 将interval转换为分钟或小时显示
            interval = task.get('interval', 3600)
            if interval < 3600: dur = f"{interval//60}分"
            else: dur = f"{interval/3600:.1f}小时"

            msg.append(f"📡 `{tid}`: *{escape_markdown_v2(task['query'][:25])}...*")
            msg.append(f"   📦 库存: *{count}* \| ⏱ 上次: {last_run_str} \| ⏳ 频率: {dur}")
            msg.append("") # Spacer
            
        update.message.reply_text("\n".join(msg), parse_mode=ParseMode.MARKDOWN_V2)

    elif sub_cmd == 'del':
        if len(args) < 2: 
            update.message.reply_text("用法: `/monitor del <task_id>`")
            return
        tid = args[1]
        if tid in MONITOR_TASKS:
            # 取消现有 Job
            for job in context.job_queue.get_jobs_by_name(f"monitor_{tid}"):
                job.schedule_removal()
                
            del MONITOR_TASKS[tid]
            save_monitor_tasks()
            
            # 删除数据文件? (保留数据更安全，只删任务)
            update.message.reply_text(f"🗑️ 任务 `{tid}` 已停止并移除配置。", parse_mode=ParseMode.MARKDOWN_V2)
        else:
            update.message.reply_text("❌ 任务ID不存在。")

    elif sub_cmd == 'get':
        if len(args) < 2:
            update.message.reply_text("用法: `/monitor get <task_id>`") 
            return
        tid = args[1]
        
        # 即使任务不在 config 中，只要有文件也可以取（防意外删除）
        data_file = os.path.join(MONITOR_DATA_DIR, f"{tid}.txt")
        if not os.path.exists(data_file):
            if tid not in MONITOR_TASKS:
                update.message.reply_text("❌ 找不到该ID的任务记录或数据文件。")
            else:
                update.message.reply_text("🤷‍♀️ 该任务暂无任何数据沉淀。")
            return
            
        task_info = MONITOR_TASKS.get(tid, {})
        q_info = task_info.get('query', '未知查询')
        
        send_file_safely(context, update.effective_chat.id, data_file, caption=f"📦 监控数据导出\nID: `{tid}`\nQuery: `{escape_markdown_v2(q_info)}`", parse_mode=ParseMode.MARKDOWN_V2)
        upload_and_send_links(context, update.effective_chat.id, data_file)
        
    else:
        update.message.reply_text("❌ 未知命令。请使用 `/monitor` 查看帮助。")

def run_monitor_execution_job(context: CallbackContext):
    """自适应监控雷达核心逻辑"""
    job_context = context.job.context
    task_id = job_context.get('task_id')
    
    if task_id not in MONITOR_TASKS: return
    task = MONITOR_TASKS[task_id]
    
    query_text = task['query']
    os.makedirs(MONITOR_DATA_DIR, exist_ok=True)
    db_file = os.path.join(MONITOR_DATA_DIR, f"{task_id}.txt")
    
    # 1. 载入布隆过滤器（或简易Set）
    known_hashes = set()
    if os.path.exists(db_file):
        try:
            with open(db_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line: known_hashes.add(hashlib.md5(line.encode()).hexdigest())
        except Exception as e:
            logger.error(f"Error reading monitor DB: {e}")

    # 2. 执行检测 (Radar Ping)
    # 策略：默认抓第一页（最新数据）。如果是高速模式，抓前3页。
    # 我们只关心是否有 NEW 数据来决定频率。
    
    # 这里我们只取 fields="host,ip,port"，为了省流量且通用
    fetch_func = lambda k, kl, ps: fetch_fofa_data(k, query_text, page=1, page_size=100, fields="host", proxy_session=ps)
    data, _, _, _, _, error = execute_query_with_fallback(fetch_func)
    
    new_data_lines = []
    
    if not error and data and data.get('results'):
        results = data.get('results')
        for item in results:
            line_str = item[0] if isinstance(item, list) else str(item)
            h = hashlib.md5(line_str.strip().encode()).hexdigest()
            if h not in known_hashes:
                new_data_lines.append(line_str.strip())
                
    # 3. 自适应调频算法 (Adaptive Frequency)
    # 基础频率
    base_interval = 3600 
    current_interval = task.get('interval', base_interval)
    
    if len(new_data_lines) > 0:
        # 命中新目标！--> 写入库
        with open(db_file, 'a', encoding='utf-8') as f:
            f.write("\n".join(new_data_lines) + "\n")
            
        # 激进策略：如果有新数据，立刻缩短检查间隔，以此追踪爆发期
        # 最小 10 分钟 (600s)
        new_interval = max(600, int(current_interval * 0.5))
    else:
        # 无新数据 --> 逐步冷却，节省资源
        # 最大 12 小时 (43200s)
        new_interval = min(43200, int(current_interval * 1.5))

    # 更新任务状态
    task['last_run'] = int(time.time())
    task['interval'] = new_interval
    save_monitor_tasks()
    
    # 4. 安排下一次运行 (Add Jitter: +/- 10%)
    jitter = random.randint(int(-new_interval * 0.1), int(new_interval * 0.1))
    next_run_delay = new_interval + jitter
    
    context.job_queue.run_once(run_monitor_execution_job, next_run_delay, context={"task_id": task_id}, name=f"monitor_{task_id}")

# --- 核心命令处理 ---
def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    welcome_text = f'👋 欢迎, {user.first_name}！\n请选择一个操作:'
    
    # v10.9.6 FIX: 扩展主菜单并重做工作流程
    keyboard = [
        [KeyboardButton("常规搜索"), KeyboardButton("海量搜索")],
        [KeyboardButton("主机详查"), KeyboardButton("批量导出")],
        [KeyboardButton("设置"), KeyboardButton("帮助手册")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    update.message.reply_text(welcome_text, reply_markup=reply_markup)

    if not CONFIG['admins']:
        first_admin_id = update.effective_user.id
        CONFIG.setdefault('admins', []).append(first_admin_id)
        save_config()
        update.message.reply_text(f"ℹ️ 已自动将您 (ID: `{first_admin_id}`) 添加为第一个管理员。")

def help_command(update: Update, context: CallbackContext):
    help_text = ( "📖 *Fofa 机器人指令手册 v10\\.9*\n\n"
                  "*🔍 资产搜索 \\(常规\\)*\n`/kkfofa [key] <query>`\n_FOFA搜索, 适用于1万条以内数据_\n\n"
                  "*🚚 资产搜索 \\(海量\\)*\n`/allfofa <query>`\n_使用next接口稳定获取海量数据 \\(管理员\\)_\n\n"
                  "*📦 主机详查 \\(智能\\)*\n`/host <ip|domain>`\n_自适应获取最全主机信息 \\(管理员\\)_\n\n"
                  "*🔬 主机速查 \\(聚合\\)*\n`/lowhost <ip|domain> [detail]`\n_快速获取主机聚合信息 \\(所有用户\\)_\n\n"
                  "*📊 聚合统计*\n`/stats <query>`\n_获取全局聚合统计 \\(管理员\\)_\n\n"
                  "*📂 批量智能分析*\n`/batchfind`\n_上传IP列表, 分析特征并生成Excel \\(管理员\\)_\n\n"
                  "*📤 批量自定义导出 \\(交互式\\)*\n`/batch <query>`\n_进入交互式菜单选择字段导出 \\(管理员\\)_\n\n"
                  "*⚙️ 管理与设置*\n`/settings`\n_进入交互式设置菜单 \\(管理员\\)_\n\n"
                  "*🔑 Key管理*\n`/batchcheckapi`\n_上传文件批量验证API Key \\(管理员\\)_\n\n"
                  "*💻 系统管理*\n"
                  "`/check` \\- 系统自检\n"
                  "`/update` \\- 在线更新脚本\n"
                  "`/shutdown` \\- 安全关闭/重启\n\n"
                  "*🛑 任务控制*\n`/stop` \\- 紧急停止下载任务\n`/cancel` \\- 取消当前操作" )
    update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN_V2)
def cancel(update: Update, context: CallbackContext) -> int:
    message = "操作已取消。"
    if update.message: update.message.reply_text(message)
    elif update.callback_query: update.callback_query.edit_message_text(message)
    context.user_data.clear()
    return ConversationHandler.END

# --- /kkfofa, /allfofa & 访客逻辑 ---
def query_entry_point(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    query_obj = update.callback_query
    message_obj = update.message

    if query_obj:
        query_obj.answer()
        context.user_data['command'] = '/kkfofa'
        
        if not is_admin(user_id):
            guest_key = ANONYMOUS_KEYS.get(str(user_id))
            if not guest_key:
                query_obj.message.edit_text("👋 欢迎！作为首次使用的访客，请先发送您的FOFA API Key。")
                return ConversationHandler.END
            context.user_data['guest_key'] = guest_key

        try:
            preset_index = int(query_obj.data.replace("run_preset_", ""))
            preset = CONFIG["presets"][preset_index]
            context.user_data['original_query'] = preset['query']
            context.user_data['key_index'] = None
            keyboard = [[InlineKeyboardButton("🌍 是的, 限定大洲", callback_data="continent_select"), InlineKeyboardButton("⏩ 不, 直接搜索", callback_data="continent_skip")]]
            query_obj.message.edit_text(f"预设查询: `{escape_markdown_v2(preset['query'])}`\n\n是否要将此查询限定在特定大洲范围内？", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
            return QUERY_STATE_ASK_CONTINENT
        except (ValueError, IndexError):
            query_obj.message.edit_text("❌ 预设查询失败。")
            return ConversationHandler.END

    elif message_obj:
        command = message_obj.text.split()[0].lower()

        if command == '/allfofa' and not is_admin(user_id):
            message_obj.reply_text("⛔️ 抱歉，`/allfofa` 命令仅限管理员使用。")
            return ConversationHandler.END

        if not is_admin(user_id):
            guest_key = ANONYMOUS_KEYS.get(str(user_id))
            if not guest_key:
                message_obj.reply_text("👋 欢迎！作为首次使用的访客，请输入您的FOFA API Key以继续。您的Key只会被您自己使用。")
                if context.args:
                    context.user_data['pending_query'] = " ".join(context.args)
                return QUERY_STATE_GET_GUEST_KEY
            context.user_data['guest_key'] = guest_key

        if not context.args:
            if command == '/kkfofa':
                presets = CONFIG.get("presets", [])
                if not presets:
                    message_obj.reply_text(f"欢迎使用FOFA查询机器人。\n\n➡️ 直接输入查询语法: `/kkfofa domain=\"example.com\"`\nℹ️ 当前没有可用的预设查询。管理员可通过 /settings 添加。")
                    return ConversationHandler.END
                keyboard = []
                for i, p in enumerate(presets):
                    query_preview = p['query'][:25] + '...' if len(p['query']) > 25 else p['query']
                    keyboard.append([InlineKeyboardButton(f"{p['name']} (`{query_preview}`)", callback_data=f"run_preset_{i}")])
                message_obj.reply_text("👇 请选择一个预设查询:", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                 message_obj.reply_text(f"用法: `{command} <fofa_query>`")
            return ConversationHandler.END

        key_index, query_text = None, " ".join(context.args)
        if context.args[0].isdigit() and is_admin(user_id):
            try:
                num = int(context.args[0])
                if 1 <= num <= len(CONFIG['apis']):
                    key_index = num
                    query_text = " ".join(context.args[1:])
            except ValueError:
                pass
        
        context.user_data['original_query'] = query_text
        context.user_data['key_index'] = key_index
        context.user_data['command'] = command

        keyboard = [[InlineKeyboardButton("🌍 是的, 限定大洲", callback_data="continent_select"), InlineKeyboardButton("⏩ 不, 直接搜索", callback_data="continent_skip")]]
        message_obj.reply_text(f"查询: `{escape_markdown_v2(query_text)}`\n\n是否要将此查询限定在特定大洲范围内？", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
        return QUERY_STATE_ASK_CONTINENT

    
    else:
        logger.error("query_entry_point called with an unsupported update type.")
        return ConversationHandler.END

def get_guest_key(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    guest_key = update.message.text.strip()
    msg = update.message.reply_text("⏳ 正在验证您的API Key...")
    data, error = verify_fofa_api(guest_key)
    if error:
        msg.edit_text(f"❌ Key验证失败: {error}\n请重新输入一个有效的Key，或使用 /cancel 取消。")
        return QUERY_STATE_GET_GUEST_KEY
    ANONYMOUS_KEYS[str(user_id)] = guest_key
    save_anonymous_keys()
    msg.edit_text(f"✅ Key验证成功 ({data.get('username', 'N/A')})！您的Key已保存，现在可以开始查询了。")
    if 'pending_query' in context.user_data:
        context.args = context.user_data.pop('pending_query').split()
        return query_entry_point(update, context)
    return ConversationHandler.END

def ask_continent_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); choice = query.data.split('_')[1]
    command = context.user_data['command']

    if choice == 'skip':
        context.user_data['query'] = context.user_data['original_query']
        query.message.edit_text(f"好的，将直接搜索: `{escape_markdown_v2(context.user_data['query'])}`", parse_mode=ParseMode.MARKDOWN_V2)
        if command == '/kkfofa':
            return proceed_with_kkfofa_query(update, context, message_to_edit=query.message)
        elif command == '/allfofa':
            return start_allfofa_search(update, context, message_to_edit=query.message)
    elif choice == 'select':
        keyboard = [
            [InlineKeyboardButton("🌏 亚洲", callback_data="continent_Asia"), InlineKeyboardButton("🌍 欧洲", callback_data="continent_Europe")],
            [InlineKeyboardButton("🌎 北美洲", callback_data="continent_NorthAmerica"), InlineKeyboardButton("🌎 南美洲", callback_data="continent_SouthAmerica")],
            [InlineKeyboardButton("🌍 非洲", callback_data="continent_Africa"), InlineKeyboardButton("🌏 大洋洲", callback_data="continent_Oceania")],
            [InlineKeyboardButton("↩️ 跳过", callback_data="continent_skip")]]
        query.message.edit_text("请选择一个大洲:", reply_markup=InlineKeyboardMarkup(keyboard)); return QUERY_STATE_CONTINENT_CHOICE


def continent_choice_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); continent = query.data.split('_', 1)[1]; original_query = context.user_data['original_query']
    command = context.user_data['command']

    if continent == 'skip':
        context.user_data['query'] = original_query
        query.message.edit_text(f"好的，将直接搜索: `{escape_markdown_v2(original_query)}`", parse_mode=ParseMode.MARKDOWN_V2)
    else:
        country_list = CONTINENT_COUNTRIES.get(continent)
        if not country_list: query.message.edit_text("❌ 错误：无效的大洲选项。"); return ConversationHandler.END
        country_fofa_string = " || ".join([f'country="{code}"' for code in country_list]); final_query = f"({original_query}) && ({country_fofa_string})"
        context.user_data['query'] = final_query
        query.message.edit_text(f"查询已构建:\n`{escape_markdown_v2(final_query)}`\n\n正在处理\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)

    if command == '/kkfofa':
        return proceed_with_kkfofa_query(update, context, message_to_edit=query.message)
    elif command == '/allfofa':
        return start_allfofa_search(update, context, message_to_edit=query.message)

def proceed_with_kkfofa_query(update: Update, context: CallbackContext, message_to_edit):
    query_text = context.user_data['query']
    cached_item = find_cached_query(query_text)
    if cached_item:
        dt_utc = datetime.fromisoformat(cached_item['timestamp']); dt_local = dt_utc.astimezone(tz.tzlocal()); time_str = dt_local.strftime('%Y-%m-%d %H:%M')
        message_text = (f"✅ *发现缓存*\n\n查询: `{escape_markdown_v2(query_text)}`\n缓存于: *{escape_markdown_v2(time_str)}*\n\n")
        keyboard = []; is_expired = (datetime.now(tz.tzutc()) - dt_utc).total_seconds() > CACHE_EXPIRATION_SECONDS
        if is_expired or not is_admin(update.effective_user.id):
             message_text += "⚠️ *此缓存已过期或您是访客，无法增量更新\\.*" if is_expired else ""
             keyboard.append([InlineKeyboardButton("⬇️ 下载旧缓存", callback_data='cache_download'), InlineKeyboardButton("🔍 全新搜索", callback_data='cache_newsearch')])
        else: 
            message_text += "请选择操作："; keyboard.append([InlineKeyboardButton("🔄 增量更新", callback_data='cache_incremental')]); keyboard.append([InlineKeyboardButton("⬇️ 下载缓存", callback_data='cache_download'), InlineKeyboardButton("🔍 全新搜索", callback_data='cache_newsearch')])
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data='cache_cancel')])
        message_to_edit.edit_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
        return QUERY_STATE_CACHE_CHOICE
    return start_new_kkfofa_search(update, context, message_to_edit=message_to_edit)

def cache_choice_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); choice = query.data.split('_')[1]
    if choice == 'download':
        cached_item = find_cached_query(context.user_data['query'])
        if cached_item:
            query.message.edit_text("⬇️ 正在从本地缓存发送文件..."); file_path = cached_item['cache']['file_path']
            send_file_safely(context, update.effective_chat.id, file_path, filename=os.path.basename(file_path))
            upload_and_send_links(context, update.effective_chat.id, file_path)
            query.message.delete()
        else: query.message.edit_text("❌ 找不到本地缓存记录。")
        return ConversationHandler.END
    elif choice == 'newsearch': return start_new_kkfofa_search(update, context, message_to_edit=query.message)
    elif choice == 'incremental': query.edit_message_text("⏳ 准备增量更新..."); start_download_job(context, run_incremental_update_query, context.user_data); query.message.delete(); return ConversationHandler.END
    elif choice == 'cancel': query.message.edit_text("操作已取消。"); return ConversationHandler.END

def start_new_kkfofa_search(update: Update, context: CallbackContext, message_to_edit=None):
    query_text = context.user_data['query']; key_index = context.user_data.get('key_index'); add_or_update_query(query_text)
    msg_text = f"🔄 正在对 `{escape_markdown_v2(query_text)}` 执行全新查询\\.\\.\\."
    msg = message_to_edit if message_to_edit else update.effective_message.reply_text(msg_text, parse_mode=ParseMode.MARKDOWN_V2)
    if message_to_edit: msg.edit_text(msg_text, parse_mode=ParseMode.MARKDOWN_V2)
    
    guest_key = context.user_data.get('guest_key')
    if guest_key:
        data, error = fetch_fofa_data(guest_key, query_text, page_size=1, fields="host")
        used_key_info = "您的Key"
    else:
        data, _, used_key_index, _, _, error = execute_query_with_fallback(
            lambda key, key_level, proxy_session: fetch_fofa_data(key, query_text, page_size=1, fields="host", proxy_session=proxy_session),
            preferred_key_index=key_index
        )
        used_key_info = f"Key \\[\\#{used_key_index}\\]"
    if error: msg.edit_text(f"❌ 查询出错: {error}"); return ConversationHandler.END
    
    total_size = data.get('size', 0)
    if total_size == 0: msg.edit_text("🤷‍♀️ 未找到结果。"); return ConversationHandler.END
    context.user_data.update({'total_size': total_size, 'chat_id': update.effective_chat.id, 'is_batch_mode': False})
    
    success_message = f"✅ 使用 {used_key_info} 找到 {total_size} 条结果\\."
    
    if total_size <= 10000:
        msg.edit_text(f"{success_message}\n开始下载\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
        start_download_job(context, run_full_download_query, context.user_data)
        return ConversationHandler.END
    else:
        keyboard = [
            [InlineKeyboardButton("💎 全部下载 (前1万)", callback_data='mode_full'), InlineKeyboardButton("🌍 分片下载 (突破上限)", callback_data='mode_sharding')],
            [InlineKeyboardButton("🌀 深度追溯下载", callback_data='mode_traceback'), InlineKeyboardButton("❌ 取消", callback_data='mode_cancel')]
        ]
        msg.edit_text(f"{success_message}\n检测到大量结果 ({total_size}条)。由于单次查询上限 (10,000)，您可以：\n\n1️⃣ **前1万**：仅下载最近的1万条。\n2️⃣ **分片下载**：按国家自动拆分，尽可能通过积少成多突破1万条限制 (消耗更多请求)。\n3️⃣ **深度追溯**：按时间回溯 (需高等级Key)。", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
        return QUERY_STATE_KKFOFA_MODE 

def query_mode_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); mode = query.data.split('_')[1]
    if mode == 'cancel': query.message.edit_text("操作已取消."); return ConversationHandler.END
    
    if mode == 'sharding':
        if context.user_data.get('is_batch_mode'):
             query.message.edit_text("⚠️ 抱歉，分片下载目前仅支持基础 Host 导出，不支持自定义批量字段。")
             return ConversationHandler.END
        start_download_job(context, run_sharded_download_job, context.user_data)
        query.message.delete()
        return ConversationHandler.END

    if mode == 'traceback':
        keyboard = [[InlineKeyboardButton("♾️ 全部获取", callback_data='limit_none')], [InlineKeyboardButton("❌ 取消", callback_data='limit_cancel')]]
        query.message.edit_text("请输入深度追溯获取的结果数量上限 (例如: 50000)，或选择全部获取。", reply_markup=InlineKeyboardMarkup(keyboard))
        return BATCH_STATE_GET_LIMIT if context.user_data.get('is_batch_mode') else QUERY_STATE_GET_TRACEBACK_LIMIT
    job_func = run_batch_download_query if context.user_data.get('is_batch_mode') else run_full_download_query
    if mode == 'full' and job_func:
        query.message.edit_text(f"⏳ 开始下载..."); start_download_job(context, job_func, context.user_data); query.message.delete()
    return ConversationHandler.END

def get_traceback_limit(update: Update, context: CallbackContext):
    limit = None
    if update.callback_query:
        query = update.callback_query; query.answer()
        if query.data == 'limit_cancel': query.message.edit_text("操作已取消."); return ConversationHandler.END
    elif update.message:
        try:
            limit = int(update.message.text.strip()); assert limit > 0
        except (ValueError, AssertionError):
            update.message.reply_text("❌ 无效的数字，请输入一个正整数。")
            return BATCH_STATE_GET_LIMIT if context.user_data.get('is_batch_mode') else QUERY_STATE_GET_TRACEBACK_LIMIT
    context.user_data['limit'] = limit
    job_func = run_batch_traceback_query if context.user_data.get('is_batch_mode') else run_traceback_download_query
    msg_target = update.callback_query.message if update.callback_query else update.message
    msg_target.reply_text(f"⏳ 开始深度追溯 (上限: {limit or '无'})...")
    start_download_job(context, job_func, context.user_data)
    if update.callback_query: msg_target.delete()
    return ConversationHandler.END

# --- /host 和 /lowhost 命令 ---
def _create_dict_from_fofa_result(result_list, fields_list):
    return {fields_list[i]: result_list[i] for i in range(len(fields_list))}
def get_common_host_info(results, fields_list):
    if not results: return {}
    first_entry = _create_dict_from_fofa_result(results[0], fields_list)
    info = {
        "IP": first_entry.get('ip', 'N/A'),
        "地理位置": f"{first_entry.get('country_name', '')} {first_entry.get('region', '')} {first_entry.get('city', '')}".strip(),
        "ASN": f"{first_entry.get('asn', 'N/A')} ({first_entry.get('org', 'N/A')})",
        "操作系统": first_entry.get('os', 'N/A'),
    }
    port_index = fields_list.index('port') if 'port' in fields_list else -1
    if port_index != -1:
        all_ports = sorted(list(set(res[port_index] for res in results if len(res) > port_index)))
        info["开放端口"] = all_ports
    return info
def create_host_summary(host_arg, results, fields_list):
    info = get_common_host_info(results, fields_list)
    summary = [f"📌 *主机概览: `{escape_markdown_v2(host_arg)}`*"]
    for key, value in info.items():
        if value and value != 'N/A':
            if isinstance(value, list):
                summary.append(f"*{escape_markdown_v2(key)}:* `{escape_markdown_v2(', '.join(map(str, value)))}`")
            else:
                summary.append(f"*{escape_markdown_v2(key)}:* `{escape_markdown_v2(value)}`")
    summary.append("\n📄 *详细报告已作为文件发送\\.*")
    return "\n".join(summary)
def format_full_host_report(host_arg, results, fields_list):
    info = get_common_host_info(results, fields_list)
    report = [f"📌 *主机聚合报告: `{escape_markdown_v2(host_arg)}`*\n"]
    for key, value in info.items():
        if value and value != 'N/A':
            if isinstance(value, list):
                report.append(f"*{escape_markdown_v2(key)}:* `{escape_markdown_v2(', '.join(map(str, value)))}`")
            else:
                report.append(f"*{escape_markdown_v2(key)}:* `{escape_markdown_v2(value)}`")
    report.append("\n\-\-\- *服务详情* \-\-\-\n")
    for res_list in results:
        d = _create_dict_from_fofa_result(res_list, fields_list)
        port_info = [f"🌐 *Port `{d.get('port')}` \\({escape_markdown_v2(d.get('protocol', 'N/A'))}\\)*"]
        if d.get('title'): port_info.append(f"  - *标题:* `{escape_markdown_v2(d.get('title'))}`")
        if d.get('server'): port_info.append(f"  - *服务:* `{escape_markdown_v2(d.get('server'))}`")
        if d.get('icp'): port_info.append(f"  - *ICP:* `{escape_markdown_v2(d.get('icp'))}`")
        if d.get('jarm'): port_info.append(f"  - *JARM:* `{escape_markdown_v2(d.get('jarm'))}`")
        cert_str = d.get('cert', '{}')
        try:
            cert_info = json.loads(cert_str) if isinstance(cert_str, str) and cert_str.startswith('{') else {}
            if cert_info.get('issuer', {}).get('CN'): port_info.append(f"  - *证书颁发者:* `{escape_markdown_v2(cert_info['issuer']['CN'])}`")
            if cert_info.get('subject', {}).get('CN'): port_info.append(f"  - *证书使用者:* `{escape_markdown_v2(cert_info['subject']['CN'])}`")
        except json.JSONDecodeError:
            pass
        if d.get('header'): port_info.append(f"  - *Header:* ```\n{d.get('header')}\n```")
        if d.get('banner'): port_info.append(f"  - *Banner:* ```\n{d.get('banner')}\n```")
        report.append("\n".join(port_info))
    return "\n".join(report)
def host_command_logic(update: Update, context: CallbackContext):
    if not context.args:
        update.message.reply_text(f"用法: `/host <ip_or_domain>`\n\n示例:\n`/host 1\\.1\\.1\\.1`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    host_arg = context.args[0]
    processing_message = update.message.reply_text(f"⏳ 正在查询主机 `{escape_markdown_v2(host_arg)}`\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
    query = f'ip="{host_arg}"' if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", host_arg) else f'domain="{host_arg}"'
    data, final_fields_list, error = None, [], None
    for level in range(3, -1, -1): 
        fields_to_try = get_fields_by_level(level)
        fields_str = ",".join(fields_to_try)
        try:
            processing_message.edit_text(f"⏳ 正在尝试以 *等级 {level}* 字段查询\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
        except (BadRequest, RetryAfter, TimedOut):
            time.sleep(1)
        temp_data, _, _, _, _, temp_error = execute_query_with_fallback(
            lambda key, key_level, proxy_session: fetch_fofa_data(key, query, page_size=100, fields=fields_str, proxy_session=proxy_session)
        )
        if not temp_error:
            data = temp_data
            final_fields_list = fields_to_try
            error = None
            break
        if "[820001]" not in str(temp_error):
            error = temp_error
            break
        else:
            error = temp_error
            continue
    if error:
        processing_message.edit_text(f"查询失败 😞\n*原因:* `{escape_markdown_v2(error)}`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    raw_results = data.get('results', [])
    if not raw_results:
        processing_message.edit_text(f"🤷‍♀️ 未找到关于 `{escape_markdown_v2(host_arg)}` 的任何信息\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    unique_services = {}
    ip_idx = final_fields_list.index('ip') if 'ip' in final_fields_list else -1
    port_idx = final_fields_list.index('port') if 'port' in final_fields_list else -1
    protocol_idx = final_fields_list.index('protocol') if 'protocol' in final_fields_list else -1
    
    if port_idx != -1 and protocol_idx != -1:
        for res in raw_results:
            key = (res[ip_idx] if ip_idx != -1 else host_arg, res[port_idx], res[protocol_idx])
            if key not in unique_services:
                unique_services[key] = res
        results = list(unique_services.values())
    else:
        results = raw_results

    full_report = format_full_host_report(host_arg, results, final_fields_list)
    if len(full_report) > 3800:
        summary_report = create_host_summary(host_arg, results, final_fields_list)
        processing_message.edit_text(summary_report, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
        report_filename = f"host_details_{host_arg.replace('.', '_')}.txt"
        try:
            plain_text_report = re.sub(r'([*_`\[\]\\])', '', full_report)
            with open(report_filename, 'w', encoding='utf-8') as f: f.write(plain_text_report)
            send_file_safely(context, update.effective_chat.id, report_filename, caption="📄 完整的详细报告已附上。")
            upload_and_send_links(context, update.effective_chat.id, report_filename)
        finally:
            if os.path.exists(report_filename): os.remove(report_filename)
    else:
        processing_message.edit_text(full_report, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
@admin_only
def host_command(update: Update, context: CallbackContext):
    host_command_logic(update, context)
def format_host_summary(data):
    parts = [f"📌 *主机聚合摘要: `{escape_markdown_v2(data.get('host', 'N/A'))}`*"]
    if data.get('ip'): parts.append(f"*IP:* `{escape_markdown_v2(data.get('ip'))}`")
    location = f"{data.get('country_name', '')} {data.get('region', '')} {data.get('city', '')}".strip()
    if location: parts.append(f"*位置:* `{escape_markdown_v2(location)}`")
    if data.get('asn'): parts.append(f"*ASN:* `{data.get('asn')} \\({escape_markdown_v2(data.get('org', 'N/A'))}\\)`")
    
    if data.get('ports'):
        port_list = data.get('ports', [])
        if port_list and isinstance(port_list[0], dict):
            port_numbers = sorted([p.get('port') for p in port_list if p.get('port')])
            parts.append(f"*开放端口:* `{escape_markdown_v2(', '.join(map(str, port_numbers)))}`")
        else:
            parts.append(f"*开放端口:* `{escape_markdown_v2(', '.join(map(str, port_list)))}`")

    if data.get('protocols'): parts.append(f"*协议:* `{escape_markdown_v2(', '.join(data.get('protocols', [])))}`")
    if data.get('category'): parts.append(f"*资产类型:* `{escape_markdown_v2(', '.join(data.get('category', [])))}`")
    if data.get('products'):
        product_names = [p.get('name', 'N/A') for p in data.get('products', [])]
        parts.append(f"*产品/组件:* `{escape_markdown_v2(', '.join(product_names))}`")
    return "\n".join(parts)
def format_host_details(data):
    summary = format_host_summary(data)
    details = ["\n\-\-\- *端口详情* \-\-\-"]
    for port_info in data.get('port_details', []):
        port_str = f"\n🌐 *Port `{port_info.get('port')}` \\({escape_markdown_v2(port_info.get('protocol', 'N/A'))}\\)*"
        if port_info.get('product'): port_str += f"\n  - *产品:* `{escape_markdown_v2(port_info.get('product'))}`"
        if port_info.get('title'): port_str += f"\n  - *标题:* `{escape_markdown_v2(port_info.get('title'))}`"
        if port_info.get('jarm'): port_str += f"\n  - *JARM:* `{escape_markdown_v2(port_info.get('jarm'))}`"
        if port_info.get('banner'): port_str += f"\n  - *Banner:* ```\n{port_info.get('banner')}\n```"
        details.append(port_str)
    full_report = summary + "\n".join(details)
    return full_report
def lowhost_command(update: Update, context: CallbackContext) -> None:
    if not context.args:
        update.message.reply_text("用法: `/lowhost <ip_or_domain> [detail]`\n\n示例:\n`/lowhost 1\\.1\\.1\\.1`\n`/lowhost example\\.com detail`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    host = context.args[0]
    detail = len(context.args) > 1 and context.args[1].lower() == 'detail'
    processing_message = update.message.reply_text(f"正在查询主机 `{escape_markdown_v2(host)}` 的聚合信息\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
    data, _, _, _, _, error = execute_query_with_fallback(
        lambda key, key_level, proxy_session: fetch_fofa_host_info(key, host, detail, proxy_session=proxy_session)
    )
    if error:
        processing_message.edit_text(f"查询失败 😞\n*原因:* `{escape_markdown_v2(error)}`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    if not data:
        processing_message.edit_text(f"🤷‍♀️ 未找到关于 `{escape_markdown_v2(host)}` 的任何信息\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    if detail:
        formatted_text = format_host_details(data)
    else:
        formatted_text = format_host_summary(data)
    if len(formatted_text) > 3800:
        processing_message.edit_text("报告过长，将作为文件发送。")
        report_filename = f"lowhost_details_{host.replace('.', '_')}.txt"
        try:
            plain_text_report = re.sub(r'([*_`\[\]\\])', '', formatted_text)
            with open(report_filename, 'w', encoding='utf-8') as f: f.write(plain_text_report)
            send_file_safely(context, update.effective_chat.id, report_filename, caption="📄 完整的聚合报告已附上。")
            upload_and_send_links(context, update.effective_chat.id, report_filename)
        finally:
            if os.path.exists(report_filename): os.remove(report_filename)
    else:
        processing_message.edit_text(formatted_text, parse_mode=ParseMode.MARKDOWN_V2)

# --- /stats 命令 ---
@admin_only
def stats_command(update: Update, context: CallbackContext):
    if not context.args:
        update.message.reply_text("请输入要进行聚合统计的FOFA查询语法:")
        return STATS_STATE_GET_QUERY
    return get_fofa_stats_query(update, context)
def get_fofa_stats_query(update: Update, context: CallbackContext):
    query_text = " ".join(context.args) if context.args else update.message.text
    msg = update.message.reply_text(f"⏳ 正在对 `{escape_markdown_v2(query_text)}` 进行聚合统计\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
    
    data, _, _, _, _, error = execute_query_with_fallback(
        lambda key, key_level, proxy_session: fetch_fofa_stats(key, query_text, proxy_session=proxy_session)
    )
    
    if error:
        msg.edit_text(f"❌ 统计失败: {escape_markdown_v2(error)}", parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END

    # 智能适配层：处理嵌套和扁平两种API响应格式
    stats_source = data.get("aggs", data)

    report = [f"📊 *聚合统计报告 for `{escape_markdown_v2(query_text)}`*\n"]
    
    # 完整版 display_map，包含全部12个可聚合字段
    display_map = {
        "countries": "🌍 Top 5 国家/地区",
        "org": "🏢 Top 5 组织 (ORG)",
        "asn": "📛 Top 5 ASN",
        "server": "🖥️ Top 5 服务/组件",
        "protocol": "🔌 Top 5 协议",
        "port": "🚪 Top 5 端口",
        "icp": "📜 Top 5 ICP备案",
        "title": "📰 Top 5 网站标题",
        "fid": "🔑 Top 5 FID 指纹",
        "domain": "🌐 Top 5 域名",          # <-- 新增
        "os": "💻 Top 5 操作系统",        # <-- 新增
        "asset_type": "📦 Top 5 资产类型" # <-- 新增
    }
    
    data_found = False
    for key, title in display_map.items():
        items = stats_source.get(key)
        
        if items and isinstance(items, list):
            data_found = True
            report.append(f"*{escape_markdown_v2(title)}*:")
            for item in items[:5]:
                name = escape_markdown_v2(item.get('name', 'N/A'))
                count = item.get('count', 0)
                report.append(f"  `{name}`: *{count:,}*")
            report.append("")

    if not data_found:
        report.append("_未找到可供聚合的数据。_")

    try:
        msg.edit_text("\n".join(report), parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e:
        if "message is too long" in str(e).lower():
            msg.edit_text("✅ 统计完成！报告过长，将作为文件发送。")
            report_filename = f"stats_report_{int(time.time())}.txt"
            plain_text_report = re.sub(r'([*_`\[\]\\])', '', "\n".join(report))
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(plain_text_report)
            send_file_safely(context, update.effective_chat.id, report_filename)
            os.remove(report_filename)
        else:
            msg.edit_text(f"❌ 发送报告时出错: {escape_markdown_v2(str(e))}", parse_mode=ParseMode.MARKDOWN_V2)

    return ConversationHandler.END

def inline_fofa_handler(update: Update, context: CallbackContext) -> None:
    """处理内联查询请求"""
    query_text = update.inline_query.query
    results = []

    try:
        # 如果用户只输入了@botname，没有附带查询语句
        if not query_text:
            results.append(
                InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title="开始输入FOFA查询语法...",
                    description='例如: domain="example.com"',
                    input_message_content=InputTextMessageContent(
                        "💡 **FOFA 内联查询用法** 💡\n\n"
                        "在任何聊天框中输入 `@你的机器人用户名` 然后跟上FOFA查询语法，即可快速搜索。\n\n"
                        "例如：`@你的机器人用户名 domain=\"qq.com\"`",
                        parse_mode=ParseMode.MARKDOWN
                    )
                )
            )
            update.inline_query.answer(results, cache_time=300) # 初始消息可以缓存久一点
            return

        # --- 用户输入了查询语句，开始调用FOFA API ---
        def inline_query_logic(key, key_level, proxy_session):
            return fetch_fofa_data(key, query_text, page_size=10, fields="host,title", proxy_session=proxy_session)

        data, _, _, _, _, error = execute_query_with_fallback(inline_query_logic)

        # 如果查询出错
        if error:
            results.append(
                InlineQueryResultArticle(
                    id='error',
                    title="查询出错",
                    description=str(error),
                    input_message_content=InputTextMessageContent(f"FOFA 查询失败: {error}")
                )
            )
        # 如果没有找到结果
        elif not data or not data.get('results'):
            results.append(
                InlineQueryResultArticle(
                    id='no_results',
                    title="未找到结果",
                    description=f"查询: {query_text}",
                    input_message_content=InputTextMessageContent(f"对于查询 '{query_text}'，FOFA 未返回任何结果。")
                )
            )
        # 成功找到结果
        else:
            for result in data['results']:
                host = result[0] if result and len(result) > 0 else "N/A"
                title = result[1] if result and len(result) > 1 else "无标题"
                
                results.append(
                    InlineQueryResultArticle(
                        id=str(uuid.uuid4()),
                        title=host,
                        description=title,
                        input_message_content=InputTextMessageContent(host)
                    )
                )
    
    except Exception as e:
        # 捕获任何意外的崩溃，并返回错误信息
        logger.error(f"内联查询时发生严重错误: {e}", exc_info=True)
        results = [
            InlineQueryResultArticle(
                id='critical_error',
                title="机器人内部错误",
                description="处理您的请求时发生意外错误，请检查日志。",
                input_message_content=InputTextMessageContent("机器人内部错误，请联系管理员。")
            )
        ]
    
    # 确保总能响应Telegram，避免界面卡住
    update.inline_query.answer(results, cache_time=10) # 实际查询结果缓存时间短一点


# --- /batchfind 命令 ---
BATCH_FEATURES = { "protocol": "协议", "domain": "域名", "os": "操作系统", "server": "服务/组件", "icp": "ICP备案号", "title": "标题", "jarm": "JARM指纹", "cert.issuer.org": "证书颁发组织", "cert.issuer.cn": "证书颁发CN", "cert.subject.org": "证书主体组织", "cert.subject.cn": "证书主体CN" }
@admin_only
def batchfind_command(update: Update, context: CallbackContext):
    update.message.reply_text("请上传一个包含 IP:Port 列表的 .txt 文件。")
    return BATCHFIND_STATE_GET_FILE
def get_batch_file_handler(update: Update, context: CallbackContext):
    doc = update.message.document
    file = doc.get_file()
    file_path = os.path.join(FOFA_CACHE_DIR, doc.file_name)
    file.download(custom_path=file_path)
    context.user_data['batch_file_path'] = file_path
    context.user_data['selected_features'] = set()
    keyboard = []
    features_list = list(BATCH_FEATURES.items())
    for i in range(0, len(features_list), 2):
        row = [InlineKeyboardButton(f"☐ {features_list[i][1]}", callback_data=f"batchfeature_{features_list[i][0]}")]
        if i + 1 < len(features_list):
            row.append(InlineKeyboardButton(f"☐ {features_list[i+1][1]}", callback_data=f"batchfeature_{features_list[i+1][0]}"))
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("✅ 全部选择", callback_data="batchfeature_all"), InlineKeyboardButton("➡️ 开始分析", callback_data="batchfeature_done")])
    update.message.reply_text("请选择您需要分析的特征:", reply_markup=InlineKeyboardMarkup(keyboard))
    return BATCHFIND_STATE_SELECT_FEATURES
def select_batch_features_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); feature = query.data.split('_', 1)[1]
    selected = context.user_data['selected_features']
    if feature == 'done':
        if not selected: query.answer("请至少选择一个特征！", show_alert=True); return BATCHFIND_STATE_SELECT_FEATURES
        query.message.edit_text("✅ 特征选择完毕，任务已提交到后台分析。")
        job_context = {'chat_id': query.message.chat_id, 'file_path': context.user_data['batch_file_path'], 'features': list(selected)}
        context.job_queue.run_once(run_batch_find_job, 1, context=job_context, name=f"batchfind_{query.message.chat_id}")
        return ConversationHandler.END
    if feature == 'all':
        if len(selected) == len(BATCH_FEATURES): selected.clear()
        else: selected.update(BATCH_FEATURES.keys())
    elif feature in selected: selected.remove(feature)
    else: selected.add(feature)
    keyboard = []
    features_list = list(BATCH_FEATURES.items())
    for i in range(0, len(features_list), 2):
        row = []
        key1 = features_list[i][0]; row.append(InlineKeyboardButton(f"{'☑' if key1 in selected else '☐'} {features_list[i][1]}", callback_data=f"batchfeature_{key1}"))
        if i + 1 < len(features_list):
            key2 = features_list[i+1][0]; row.append(InlineKeyboardButton(f"{'☑' if key2 in selected else '☐'} {features_list[i+1][1]}", callback_data=f"batchfeature_{key2}"))
        keyboard.append(row)
    all_text = "✅ 取消全选" if len(selected) == len(BATCH_FEATURES) else "✅ 全部选择"
    keyboard.append([InlineKeyboardButton(all_text, callback_data="batchfeature_all"), InlineKeyboardButton("➡️ 开始分析", callback_data="batchfeature_done")])
    query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))
    return BATCHFIND_STATE_SELECT_FEATURES
def run_batch_find_job(context: CallbackContext):
    job_data = context.job.context; chat_id, file_path, features = job_data['chat_id'], job_data['file_path'], job_data['features']
    bot = context.bot; msg = bot.send_message(chat_id, "⏳ 开始批量分析任务...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f: targets = [line.strip() for line in f if line.strip()]
    except Exception as e: msg.edit_text(f"❌ 读取文件失败: {e}"); return
    if not targets: msg.edit_text("❌ 文件为空。"); return
    total_targets = len(targets); processed_count = 0; detailed_results_for_excel = []
    for target in targets:
        processed_count += 1
        if processed_count % 10 == 0:
            try: msg.edit_text(f"分析进度: {create_progress_bar(processed_count/total_targets*100)} ({processed_count}/{total_targets})")
            except (BadRequest, RetryAfter, TimedOut): pass
        query = f'ip="{target}"' if ':' not in target else f'host="{target}"'
        data, _, _, _, _, error = execute_query_with_fallback(
            lambda key, key_level, proxy_session: fetch_fofa_data(key, query, page_size=1, fields=",".join(features), proxy_session=proxy_session)
        )
        if not error and data.get('results'):
            result = data['results'][0]
            row_data = {'Target': target}
            row_data.update({BATCH_FEATURES.get(f, f): result[i] for i, f in enumerate(features)})
            detailed_results_for_excel.append(row_data)
    if detailed_results_for_excel:
        try:
            df = pd.DataFrame(detailed_results_for_excel)
            excel_filename = generate_filename_from_query(os.path.basename(file_path), prefix="analysis", ext=".xlsx")
            df.to_excel(excel_filename, index=False, engine='openpyxl')
            msg.edit_text("✅ 分析完成！正在发送Excel报告...")
            send_file_safely(context, chat_id, excel_filename, caption="📄 详细特征分析Excel报告")
            upload_and_send_links(context, chat_id, excel_filename)
            os.remove(excel_filename)
        except Exception as e: msg.edit_text(f"❌ 生成Excel失败: {e}")
    else: msg.edit_text("🤷‍♀️ 分析完成，但未找到任何匹配的FOFA数据。")
    if os.path.exists(file_path): os.remove(file_path)

# --- /batch (交互式) ---
def build_batch_fields_keyboard(user_data):
    selected_fields = user_data.get('selected_fields', set())
    page = user_data.get('page', 0)
    flat_fields = []
    for category, fields in FIELD_CATEGORIES.items():
        for field in fields:
            flat_fields.append((field, category))
    items_per_page = 12
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    page_items = flat_fields[start_index:end_index]
    keyboard = []
    for i in range(0, len(page_items), 2):
        row = []
        field1, cat1 = page_items[i]
        prefix1 = "☑️" if field1 in selected_fields else "☐"
        row.append(InlineKeyboardButton(f"{prefix1} {field1}", callback_data=f"batchfield_toggle_{field1}"))
        if i + 1 < len(page_items):
            field2, cat2 = page_items[i+1]
            prefix2 = "☑️" if field2 in selected_fields else "☐"
            row.append(InlineKeyboardButton(f"{prefix2} {field2}", callback_data=f"batchfield_toggle_{field2}"))
        keyboard.append(row)
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ 上一页", callback_data="batchfield_prev"))
    if end_index < len(flat_fields):
        nav_row.append(InlineKeyboardButton("下一页 ➡️", callback_data="batchfield_next"))
    keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton("✅ 完成选择并开始", callback_data="batchfield_done")])
    return InlineKeyboardMarkup(keyboard)
@admin_only
def batch_command(update: Update, context: CallbackContext):
    if not context.args:
        update.message.reply_text("用法: `/batch <fofa_query>`")
        return ConversationHandler.END
    query_text = " ".join(context.args)
    context.user_data['query'] = query_text
    context.user_data['selected_fields'] = set(FREE_FIELDS[:5])
    context.user_data['page'] = 0
    keyboard = build_batch_fields_keyboard(context.user_data)
    update.message.reply_text(f"查询: `{escape_markdown_v2(query_text)}`\n请选择要导出的字段:", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)
    return BATCH_STATE_SELECT_FIELDS
def batch_select_fields_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    action = query.data.split('_', 1)[1]
    if action == "next":
        context.user_data['page'] += 1
    elif action == "prev":
        context.user_data['page'] -= 1
    elif action.startswith("toggle_"):
        field = action.replace("toggle_", "")
        if field in context.user_data['selected_fields']:
            context.user_data['selected_fields'].remove(field)
        else:
            context.user_data['selected_fields'].add(field)
    elif action == "done":
        selected_fields = context.user_data.get('selected_fields')
        if not selected_fields:
            query.answer("请至少选择一个字段！", show_alert=True)
            return BATCH_STATE_SELECT_FIELDS
        query_text = context.user_data['query']
        fields_str = ",".join(list(selected_fields))
        msg = query.message.edit_text("正在执行查询以预估数据量...")
        data, _, used_key_index, key_level, _, error = execute_query_with_fallback(
            lambda key, key_level, proxy_session: fetch_fofa_data(key, query_text, page_size=1, fields="host", proxy_session=proxy_session)
        )
        if error: msg.edit_text(f"❌ 查询出错: {error}"); return ConversationHandler.END
        total_size = data.get('size', 0)
        if total_size == 0: msg.edit_text("🤷‍♀️ 未找到结果。"); return ConversationHandler.END
        allowed_fields = get_fields_by_level(key_level)
        unauthorized_fields = [f for f in selected_fields if f not in allowed_fields]
        if unauthorized_fields:
            msg.edit_text(f"⚠️ 警告: 您选择的字段 `{', '.join(unauthorized_fields)}` 超出当前可用最高级Key (等级{key_level}) 的权限。请重新选择或升级Key。")
            return BATCH_STATE_SELECT_FIELDS
        context.user_data.update({'chat_id': update.effective_chat.id, 'fields': fields_str, 'total_size': total_size, 'is_batch_mode': True })
        success_message = f"✅ 使用 Key \\[\\#{used_key_index}\\] \\(等级{key_level}\\) 找到 {total_size} 条结果\\."
        if total_size <= 10000:
            msg.edit_text(f"{success_message}\n开始自定义字段批量导出\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2); start_download_job(context, run_batch_download_query, context.user_data)
            return ConversationHandler.END
        else:
            keyboard = [[InlineKeyboardButton("💎 导出前1万条", callback_data='mode_full'), InlineKeyboardButton("🌀 深度追溯导出", callback_data='mode_traceback')], [InlineKeyboardButton("❌ 取消", callback_data='mode_cancel')]]
            msg.edit_text(f"{success_message}\n请选择导出模式:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2); return BATCH_STATE_MODE_CHOICE
    keyboard = build_batch_fields_keyboard(context.user_data)
    query.message.edit_reply_markup(reply_markup=keyboard)
    return BATCH_STATE_SELECT_FIELDS

# --- /batchcheckapi 命令 ---
@admin_only
def batch_check_api_command(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("请上传一个包含 API Keys 的 .txt 文件 (每行一个 Key)。")
    return BATCHCHECKAPI_STATE_GET_FILE
def receive_api_file(update: Update, context: CallbackContext) -> int:
    doc = update.message.document
    if not doc.file_name.endswith('.txt'):
        update.message.reply_text("❌ 文件格式错误，请上传 .txt 文件。")
        return ConversationHandler.END
    file = doc.get_file()
    temp_path = os.path.join(FOFA_CACHE_DIR, f"api_check_{doc.file_id}.txt")
    file.download(custom_path=temp_path)
    try:
        with open(temp_path, 'r', encoding='utf-8') as f:
            keys_to_check = [line.strip() for line in f if line.strip()]
    except Exception as e:
        update.message.reply_text(f"❌ 读取文件失败: {e}")
        if os.path.exists(temp_path): os.remove(temp_path)
        return ConversationHandler.END
    if not keys_to_check:
        update.message.reply_text("🤷‍♀️ 文件为空或不包含任何有效的 Key。")
        if os.path.exists(temp_path): os.remove(temp_path)
        return ConversationHandler.END
    msg = update.message.reply_text(f"⏳ 开始批量验证 {len(keys_to_check)} 个 API Key...")
    valid_keys, invalid_keys = [], []
    total = len(keys_to_check)
    for i, key in enumerate(keys_to_check):
        data, error = verify_fofa_api(key)
        if not error:
            is_vip = data.get('isvip', False)
            api_level = data.get('vip_level', 0)
            level = 0
            if is_vip:
                if api_level == 2: level = 1
                elif api_level == 3: level = 2
                elif api_level >= 4: level = 3
                else: level = 1
            level_name = {0: "免费", 1: "个人", 2: "商业", 3: "企业"}.get(level, "未知")
            valid_keys.append(f"`...{key[-4:]}` \\- ✅ *有效* \\({escape_markdown_v2(data.get('username', 'N/A'))}, {level_name}会员\\)")
        else:
            invalid_keys.append(f"`...{key[-4:]}` \\- ❌ *无效* \\(原因: {escape_markdown_v2(error)}\\)")
        if (i + 1) % 10 == 0 or (i + 1) == total:
            try:
                progress_text = f"⏳ 验证进度: {create_progress_bar((i+1)/total*100)} ({i+1}/{total})"
                msg.edit_text(progress_text)
            except (BadRequest, RetryAfter, TimedOut):
                time.sleep(2)
    
    report = [f"📋 *批量API Key验证报告*"]
    report.append(f"\n总计: {total} \\| 有效: {len(valid_keys)} \\| 无效: {len(invalid_keys)}\n")
    if valid_keys:
        report.append("\-\-\- *有效 Keys* \-\-\-")
        report.extend(valid_keys)
    if invalid_keys:
        report.append("\n\-\-\- *无效 Keys* \-\-\-")
        report.extend(invalid_keys)
    
    report_text = "\n".join(report)
    if len(report_text) > 3800:
        summary = f"✅ 验证完成！\n总计: {total} \\| 有效: {len(valid_keys)} \\| 无效: {len(invalid_keys)}\n\n报告过长，已作为文件发送\\."
        msg.edit_text(summary)
        report_filename = f"api_check_report_{int(time.time())}.txt"
        try:
            plain_text_report = re.sub(r'([*_`\[\]\\])', '', report_text)
            with open(report_filename, 'w', encoding='utf-8') as f: f.write(plain_text_report)
            send_file_safely(context, update.effective_chat.id, report_filename)
        finally:
            if os.path.exists(report_filename): os.remove(report_filename)
    else:
        msg.edit_text(report_text, parse_mode=ParseMode.MARKDOWN_V2)

    if os.path.exists(temp_path): os.remove(temp_path)
    return ConversationHandler.END

# --- 其他管理命令 ---
@admin_only
def check_command(update: Update, context: CallbackContext):
    msg = update.message.reply_text("⏳ 正在执行系统自检...")
    report = ["*📋 系统自检报告*"]
    try:
        global CONFIG; CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
        report.append("✅ *配置文件*: `config\\.json` 加载正常")
    except Exception as e:
        report.append(f"❌ *配置文件*: 加载失败 \\- {escape_markdown_v2(str(e))}")
        msg.edit_text("\n".join(report), parse_mode=ParseMode.MARKDOWN_V2); return
    report.append("\n*🔑 API Keys:*")
    if not CONFIG.get('apis'): report.append("  \\- ⚠️ 未配置任何 API Key")
    else:
        for i, key in enumerate(CONFIG['apis']):
            level = KEY_LEVELS.get(key, -1)
            level_name = {-1: "❌ 无效", 0: "✅ 免费", 1: "✅ 个人", 2: "✅ 商业", 3: "✅ 企业"}.get(level, "未知")
            report.append(f"  `\\#{i+1}` \\(`...{key[-4:]}`\\): {level_name}")
    report.append("\n*🌐 代理:*")
    proxies_to_check = CONFIG.get("proxies", [])
    if not proxies_to_check and CONFIG.get("proxy"): proxies_to_check.append(CONFIG.get("proxy"))
    if not proxies_to_check: report.append("  \\- ℹ️ 未配置代理")
    else:
        for p in proxies_to_check:
            try:
                requests.get("https://fofa.info", proxies={"http": p, "https": p}, timeout=10, verify=False)
                report.append(f"  \\- `{escape_markdown_v2(p)}`: ✅ 连接成功")
            except Exception as e: report.append(f"  \\- `{escape_markdown_v2(p)}`: ❌ 连接失败 \\- `{escape_markdown_v2(str(e))}`")
    msg.edit_text("\n".join(report), parse_mode=ParseMode.MARKDOWN_V2)
@admin_only
def stop_all_tasks(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    context.bot_data[f'stop_job_{chat_id}'] = True
    update.message.reply_text("🛑 已发送停止信号，当前下载任务将在完成本页后停止。")
@admin_only
def backup_config_command(update: Update, context: CallbackContext):
    if update.callback_query:
        update.callback_query.answer()
    
    chat_id = update.effective_chat.id
    if os.path.exists(CONFIG_FILE):
        context.bot.send_message(chat_id, "📤 正在发送配置文件备份...")
        send_file_safely(context, chat_id, CONFIG_FILE)
        upload_and_send_links(context, chat_id, CONFIG_FILE)
    else:
        context.bot.send_message(chat_id, "❌ 找不到配置文件。")
@admin_only
def restore_config_command(update: Update, context: CallbackContext):
    update.message.reply_text("请发送您的 `config.json` 备份文件。")
    return RESTORE_STATE_GET_FILE
def receive_config_file(update: Update, context: CallbackContext):
    doc = update.message.document
    if doc.file_name != 'config.json':
        update.message.reply_text("❌ 文件名错误，请确保您上传的是 `config.json`。")
        return ConversationHandler.END
    file = doc.get_file()
    file.download(custom_path=CONFIG_FILE)
    global CONFIG; CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
    update.message.reply_text("✅ 配置文件已恢复。机器人将自动重启以应用更改。")
    shutdown_command(update, context, restart=True)
    return ConversationHandler.END
@admin_only
def history_command(update: Update, context: CallbackContext):
    if not HISTORY['queries']: update.message.reply_text("查询历史为空。"); return
    history_text = "*🕰️ 最近查询历史*\n\n"
    for i, item in enumerate(HISTORY['queries'][:15]):
        dt_utc = datetime.fromisoformat(item['timestamp']); dt_local = dt_utc.astimezone(tz.tzlocal()); time_str = dt_local.strftime('%Y-%m-%d %H:%M')
        history_text += f"`{i+1}\\.` `{escape_markdown_v2(item['query_text'])}`\n   _{escape_markdown_v2(time_str)}_\n"
    update.message.reply_text(history_text, parse_mode=ParseMode.MARKDOWN_V2)
@admin_only
def import_command(update: Update, context: CallbackContext):
    update.message.reply_text("请发送您要导入的旧缓存文件 (txt格式)。")
    return IMPORT_STATE_GET_FILE
def get_import_query(update: Update, context: CallbackContext):
    doc = update.message.document
    if not doc.file_name.endswith('.txt'): update.message.reply_text("❌ 请上传 .txt 文件。"); return ConversationHandler.END
    file = doc.get_file()
    temp_path = os.path.join(FOFA_CACHE_DIR, f"import_{doc.file_id}.txt")
    file.download(custom_path=temp_path)
    try:
        with open(temp_path, 'r', encoding='utf-8') as f: result_count = sum(1 for _ in f)
    except Exception as e: update.message.reply_text(f"❌ 读取文件失败: {e}"); os.remove(temp_path); return ConversationHandler.END
    query_text = update.message.text
    if not query_text: update.message.reply_text("请输入与此文件关联的原始FOFA查询语法:"); return IMPORT_STATE_GET_FILE
    final_filename = generate_filename_from_query(query_text)
    final_path = os.path.join(FOFA_CACHE_DIR, final_filename)
    shutil.move(temp_path, final_path)
    cache_data = {'file_path': final_path, 'result_count': result_count}
    add_or_update_query(query_text, cache_data)
    update.message.reply_text(f"✅ 成功导入缓存！\n查询: `{escape_markdown_v2(query_text)}`\n共 {result_count} 条记录\\.", parse_mode=ParseMode.MARKDOWN_V2)
    return ConversationHandler.END
@admin_only
def get_log_command(update: Update, context: CallbackContext):
    if os.path.exists(LOG_FILE):
        send_file_safely(context, update.effective_chat.id, LOG_FILE)
        upload_and_send_links(context, update.effective_chat.id, LOG_FILE)
    else: update.message.reply_text("❌ 未找到日志文件。")

@admin_only
def shutdown_command(update: Update, context: CallbackContext, restart=False):
    message = "🤖 机器人正在重启..." if restart else "🤖 机器人正在关闭..."
    update.message.reply_text(message)
    logger.info(f"Shutdown/Restart initiated by user {update.effective_user.id}")
    
    # v10.9 FIX: Use OS signals for a truly robust and deadlock-free shutdown.
    # This sends a SIGINT signal (like Ctrl+C) to the bot's own process,
    # which updater.idle() is designed to catch gracefully.
    threading.Thread(target=lambda: (time.sleep(1), os.kill(os.getpid(), signal.SIGINT))).start()

@admin_only
def update_script_command(update: Update, context: CallbackContext):
    update_url = CONFIG.get("update_url")
    if not update_url:
        update.message.reply_text("❌ 未在设置中配置更新URL。请使用 /settings \\-\\> 脚本更新 \\-\\> 设置URL。")
        return
    msg = update.message.reply_text("⏳ 正在从远程URL下载新脚本...")
    try:
        response = requests.get(update_url, timeout=30, proxies=get_proxies())
        response.raise_for_status()
        script_content = response.text
        with open(__file__, 'w', encoding='utf-8') as f:
            f.write(script_content)
        msg.edit_text("✅ 脚本更新成功！机器人将自动重启以应用新版本。")
        shutdown_command(update, context, restart=True)
    except Exception as e:
        msg.edit_text(f"❌ 更新失败: {escape_markdown_v2(str(e))}", parse_mode=ParseMode.MARKDOWN_V2)

# --- 设置菜单 ---
@admin_only
def settings_command(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("🔑 API 管理", callback_data='settings_api'), InlineKeyboardButton("✨ 预设管理", callback_data='settings_preset')],
        [InlineKeyboardButton("🌐 代理池管理", callback_data='settings_proxypool'), InlineKeyboardButton("📤 上传接口设置", callback_data='settings_upload')],
        [InlineKeyboardButton("👨‍💼 管理员设置", callback_data='settings_admin')],
        [InlineKeyboardButton("💾 备份与恢复", callback_data='settings_backup'), InlineKeyboardButton("🔄 脚本更新", callback_data='settings_update')],
        [InlineKeyboardButton("❌ 关闭菜单", callback_data='settings_close')]
    ]
    message_text = "⚙️ *设置菜单*"; reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query: update.callback_query.message.edit_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
    else: update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_MAIN
def settings_callback_handler(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); menu = query.data.split('_', 1)[1]
    if menu == 'api': return show_api_menu(update, context, force_check=False)
    if menu == 'proxypool': return show_proxypool_menu(update, context)
    if menu == 'backup': return show_backup_restore_menu(update, context)
    if menu == 'preset': return show_preset_menu(update, context)
    if menu == 'update': return show_update_menu(update, context)
    if menu == 'upload': return show_upload_api_menu(update, context)
    if menu == 'admin': return show_admin_menu(update, context)
    if menu == 'close': query.message.edit_text("菜单已关闭."); return ConversationHandler.END
    return SETTINGS_STATE_ACTION
def settings_action_handler(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); action = query.data.split('_', 1)[1]
    if action == 'add_api': query.message.edit_text("请输入新的FOFA API Key:"); return SETTINGS_STATE_GET_KEY
    if action == 'remove_api': query.message.edit_text("请输入要移除的API Key的编号:"); return SETTINGS_STATE_REMOVE_API
    if action == 'check_api': return show_api_menu(update, context, force_check=True)
    if action == 'back': return settings_command(update, context)
def show_api_menu(update: Update, context: CallbackContext, force_check=False):
    query = update.callback_query
    if force_check: 
        query.message.edit_text("⏳ 正在重新检查所有API Key状态...")
        check_and_classify_keys()
    api_list_text = ["*🔑 当前 API Keys:*"]
    if not CONFIG['apis']: api_list_text.append("  \\- _空_")
    else:
        for i, key in enumerate(CONFIG['apis']):
            level = KEY_LEVELS.get(key, -1)
            level_name = {-1: "❌ 无效", 0: "✅ 免费", 1: "✅ 个人", 2: "✅ 商业", 3: "✅ 企业"}.get(level, "未知")
            api_list_text.append(f"  `\\#{i+1}` `...{key[-4:]}` \\- {level_name}")
    keyboard = [
        [InlineKeyboardButton("➕ 添加", callback_data='action_add_api'), InlineKeyboardButton("➖ 移除", callback_data='action_remove_api')],
        [InlineKeyboardButton("🔄 状态检查", callback_data='action_check_api'), InlineKeyboardButton("🔙 返回", callback_data='action_back')]
    ]
    query.message.edit_text("\n".join(api_list_text), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_ACTION
def get_key(update: Update, context: CallbackContext):
    new_key = update.message.text.strip()
    if new_key in CONFIG['apis']:
        update.message.reply_text("⚠️ 此 Key 已存在。")
        return settings_command(update, context)

    msg = update.message.reply_text("⏳ 正在验证新的 API Key...")
    data, error = verify_fofa_api(new_key)
    if error:
        msg.edit_text(f"❌ Key 验证失败: {error}\n请重新输入一个有效的Key，或使用 /cancel 取消。")
        return SETTINGS_STATE_GET_KEY  
    
    CONFIG['apis'].append(new_key)
    save_config()
    check_and_classify_keys() 
    msg.edit_text(f"✅ API Key ({data.get('username', 'N/A')}) 已成功添加。")
    
    # 使用一个新的 update 对象来调用 settings_command，因为它需要一个有效的 update 对象
    # 来发送新消息，而我们编辑了旧消息。
    fake_update = type('FakeUpdate', (), {'message': update.message, 'callback_query': None})
    return settings_command(fake_update, context)

def remove_api(update: Update, context: CallbackContext):
    input_text = update.message.text.strip()
    # 使用正则表达式查找所有数字，支持逗号、空格等分隔符
    indices_to_remove_str = re.findall(r'\d+', input_text)
    
    if not indices_to_remove_str:
        update.message.reply_text("❌ 请输入一个或多个有效的数字编号。")
        return settings_command(update, context)

    indices_to_remove = set()
    invalid_indices = []
    for index_str in indices_to_remove_str:
        try:
            index = int(index_str) - 1
            if 0 <= index < len(CONFIG['apis']):
                indices_to_remove.add(index)
            else:
                invalid_indices.append(index_str)
        except ValueError:
            invalid_indices.append(index_str)

    if invalid_indices:
        update.message.reply_text(f"⚠️ 无效的编号: {', '.join(invalid_indices)}。")

    if not indices_to_remove:
        return settings_command(update, context)

    # 对索引进行降序排序，以防止在删除时出现索引错误
    sorted_indices = sorted(list(indices_to_remove), reverse=True)
    
    removed_keys_display = []
    for index in sorted_indices:
        removed_key = CONFIG['apis'].pop(index)
        # v10.9.6 FIX: 手动转义Markdown字符以用于确认消息。
        removed_keys_display.append(f"`...{removed_key[-4:]}` \\(原编号 \\#{index + 1}\\)")

    save_config()
    check_and_classify_keys()
    
    update.message.reply_text(f"✅ 已成功移除以下 Key:\n{', '.join(reversed(removed_keys_display))}", parse_mode=ParseMode.MARKDOWN_V2)
    
    fake_update = type('FakeUpdate', (), {'message': update.message, 'callback_query': None})
    return settings_command(fake_update, context)
def show_preset_menu(update: Update, context: CallbackContext):
    query = update.callback_query; presets = CONFIG.get("presets", [])
    text = ["*✨ 预设查询管理*"]
    if not presets: text.append("  \\- _空_")
    else:
        for i, p in enumerate(presets): text.append(f"`{i+1}\\.` *{escape_markdown_v2(p['name'])}*: `{escape_markdown_v2(p['query'])}`")
    keyboard = [
        [InlineKeyboardButton("➕ 添加", callback_data='preset_add'), InlineKeyboardButton("➖ 移除", callback_data='preset_remove')],
        [InlineKeyboardButton("🔙 返回", callback_data='preset_back')]
    ]
    query.message.edit_text("\n".join(text), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_PRESET_MENU
def preset_menu_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); action = query.data.split('_')[1]
    if action == 'add': query.message.edit_text("请输入预设的名称:"); return SETTINGS_STATE_GET_PRESET_NAME
    if action == 'remove': query.message.edit_text("请输入要移除的预设的编号:"); return SETTINGS_STATE_REMOVE_PRESET
    if action == 'back': return settings_command(update, context)
def get_preset_name(update: Update, context: CallbackContext):
    context.user_data['preset_name'] = update.message.text.strip()
    update.message.reply_text("请输入此预设的FOFA查询语法:")
    return SETTINGS_STATE_GET_PRESET_QUERY
def get_preset_query(update: Update, context: CallbackContext):
    preset_query = update.message.text.strip(); preset_name = context.user_data['preset_name']
    CONFIG.setdefault("presets", []).append({"name": preset_name, "query": preset_query}); save_config()
    update.message.reply_text("✅ 预设已添加。")
    return settings_command(update, context)
def remove_preset(update: Update, context: CallbackContext):
    try:
        index = int(update.message.text.strip()) - 1
        if 0 <= index < len(CONFIG['presets']):
            CONFIG['presets'].pop(index); save_config()
            update.message.reply_text("✅ 预设已移除。")
        else: update.message.reply_text("❌ 无效的编号。")
    except ValueError: update.message.reply_text("❌ 请输入一个有效的数字编号。")
    return settings_command(update, context)
def show_update_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    url = CONFIG.get("update_url") or "未设置"
    text = f"🔄 *脚本更新设置*\n\n当前更新URL: `{escape_markdown_v2(url)}`"
    keyboard = [[InlineKeyboardButton("✏️ 设置URL", callback_data='update_set_url'), InlineKeyboardButton("🔙 返回", callback_data='update_back')]]
    query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_ACTION
def get_update_url(update: Update, context: CallbackContext):
    url = update.message.text.strip()
    if url.lower().startswith('http'): CONFIG['update_url'] = url; save_config(); update.message.reply_text("✅ 更新URL已设置。")
    else: update.message.reply_text("❌ 无效的URL格式。")
    return settings_command(update, context)
def show_backup_restore_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    text = "💾 *备份与恢复*\n\n\\- *备份*: 发送当前的 `config\\.json` 文件给您。\n\\- *恢复*: 您需要向机器人发送一个 `config\\.json` 文件来覆盖当前配置。"
    keyboard = [[InlineKeyboardButton("📤 备份", callback_data='backup_now'), InlineKeyboardButton("📥 恢复", callback_data='restore_now')], [InlineKeyboardButton("🔙 返回", callback_data='backup_back')]]
    query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_ACTION
def show_proxypool_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    proxies = CONFIG.get("proxies", [])
    text = ["*🌐 代理池管理*"]
    if not proxies: text.append("  \\- _空_")
    else:
        for i, p in enumerate(proxies): text.append(f"`{i+1}\\.` `{escape_markdown_v2(p)}`")
    keyboard = [
        [InlineKeyboardButton("➕ 添加", callback_data='proxypool_add'), InlineKeyboardButton("➖ 移除", callback_data='proxypool_remove')],
        [InlineKeyboardButton("🔙 返回", callback_data='proxypool_back')]
    ]
    query.message.edit_text("\n".join(text), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_PROXYPOOL_MENU
def proxypool_menu_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); action = query.data.split('_')[1]
    if action == 'add': query.message.edit_text("请输入要添加的代理 (格式: `http://user:pass@host:port`):"); return SETTINGS_STATE_GET_PROXY_ADD
    if action == 'remove': query.message.edit_text("请输入要移除的代理的编号:"); return SETTINGS_STATE_GET_PROXY_REMOVE
    if action == 'back': return settings_command(update, context)
def get_proxy_to_add(update: Update, context: CallbackContext):
    proxy = update.message.text.strip()
    if proxy not in CONFIG['proxies']: CONFIG['proxies'].append(proxy); save_config(); update.message.reply_text("✅ 代理已添加。")
    else: update.message.reply_text("⚠️ 此代理已存在。")
    return settings_command(update, context)
def get_proxy_to_remove(update: Update, context: CallbackContext):
    try:
        index = int(update.message.text.strip()) - 1
        if 0 <= index < len(CONFIG['proxies']):
            CONFIG['proxies'].pop(index); save_config()
            update.message.reply_text("✅ 代理已移除。")
        else: update.message.reply_text("❌ 无效的编号。")
    except ValueError: update.message.reply_text("❌ 请输入一个有效的数字编号。")
    return settings_command(update, context)
def show_upload_api_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    url = CONFIG.get("upload_api_url") or "未设置"
    token_status = "已设置" if CONFIG.get("upload_api_token") else "未设置"
    text = (f"📤 *上传接口设置*\n\n"
            f"此功能可将机器人生成的所有文件自动上传到您指定的服务器，并返回下载命令。\n\n"
            f"*API URL:* `{escape_markdown_v2(url)}`\n"
            f"*API Token:* `{token_status}`")
    kbd = [
        [InlineKeyboardButton("✏️ 设置 URL", callback_data='upload_set_url'), InlineKeyboardButton("🔑 设置 Token", callback_data='upload_set_token')],
        [InlineKeyboardButton("🔙 返回", callback_data='upload_back')]
    ]
    query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(kbd), parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_UPLOAD_API_MENU
def upload_api_menu_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); action = query.data.split('_', 1)[1]
    if action == 'back': return settings_command(update, context)
    if action == 'set_url': query.message.edit_text("请输入您的上传接口 URL:"); return SETTINGS_STATE_GET_UPLOAD_URL
    if action == 'set_token': query.message.edit_text("请输入您的上传接口 Token:"); return SETTINGS_STATE_GET_UPLOAD_TOKEN
    return SETTINGS_STATE_UPLOAD_API_MENU
def get_upload_url(update: Update, context: CallbackContext):
    url = update.message.text.strip()
    if url.lower().startswith('http'):
        CONFIG['upload_api_url'] = url; save_config()
        update.message.reply_text("✅ 上传 URL 已更新。")
    else: update.message.reply_text("❌ 无效的 URL 格式。")
    return settings_command(update, context)
def get_upload_token(update: Update, context: CallbackContext):
    token = update.message.text.strip()
    CONFIG['upload_api_token'] = token; save_config()
    update.message.reply_text("✅ 上传 Token 已更新。")
    return settings_command(update, context)

# --- Admin Management ---
def show_admin_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    admins = CONFIG.get('admins', [])
    text = ["*👨‍💼 管理员列表*"]
    if not admins:
        text.append("  \\- _空_")
    else:
        for i, admin_id in enumerate(admins):
            user_label = "⭐ 超级管理员" if i == 0 else f"  `\\#{i+1}`"
            text.append(f"{user_label} \\- `{admin_id}`")
    
    keyboard = []
    if is_super_admin(query.from_user.id):
        keyboard.append([
            InlineKeyboardButton("➕ 添加管理员", callback_data='admin_add'),
            InlineKeyboardButton("➖ 移除管理员", callback_data='admin_remove')
        ])
    keyboard.append([InlineKeyboardButton("🔙 返回", callback_data='admin_back')])
    
    query.message.edit_text("\n".join(text), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_ADMIN_MENU

def admin_menu_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    action = query.data.split('_')[1]

    if not is_super_admin(query.from_user.id):
        query.answer("⛔️ 只有超级管理员才能执行此操作。", show_alert=True)
        return SETTINGS_STATE_ADMIN_MENU


    if action == 'add':
        query.message.edit_text("请输入新管理员的 Telegram User ID:")
        return SETTINGS_STATE_GET_ADMIN_ID_TO_ADD

    if action == 'remove':
        query.message.edit_text("请输入要移除的管理员的编号 (例如: 2):")
        return SETTINGS_STATE_GET_ADMIN_ID_TO_REMOVE

    if action == 'back':
        return settings_command(update, context)

def get_admin_id_to_add(update: Update, context: CallbackContext):
    try:
        new_id = int(update.message.text.strip())
        admins = CONFIG.get('admins', [])
        if new_id in admins:
            update.message.reply_text("⚠️ 此用户已经是管理员。")
        else:
            CONFIG['admins'].append(new_id)
            save_config()
            update.message.reply_text("✅ 管理员已添加。")
    except ValueError:
        update.message.reply_text("❌ 无效的 User ID，请输入纯数字。")
    
    return settings_command(update, context)

def get_admin_id_to_remove(update: Update, context: CallbackContext):
    try:
        index = int(update.message.text.strip())
        admins = CONFIG.get('admins', [])
        if index == 1:
            update.message.reply_text("❌ 不能移除超级管理员。")
        elif 1 < index <= len(admins):
            removed_admin = CONFIG['admins'].pop(index - 1)
            save_config()
            update.message.reply_text(f"✅ 已移除管理员 `{removed_admin}`。")
        else:
            update.message.reply_text("❌ 无效的编号。")
    except ValueError:
        update.message.reply_text("❌ 请输入一个有效的数字编号。")
    
    return settings_command(update, context)

# --- /allfofa Command Logic ---
def start_allfofa_search(update: Update, context: CallbackContext, message_to_edit=None):
    query_text = context.user_data['query']
    msg = message_to_edit if message_to_edit else update.effective_message.reply_text(f"🚚 正在为查询 `{escape_markdown_v2(query_text)}` 准备海量数据获取任务\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
    
    # v10.9.5 FIX: Set min_level=1 for /allfofa pre-check to ensure a VIP key is used.
    data, used_key, _, _, used_proxy, error = execute_query_with_fallback(
        lambda key, key_level, proxy_session: fetch_fofa_next_data(key, query_text, page_size=10000, proxy_session=proxy_session),
        min_level=1
    )

    if error:
        msg.edit_text(f"❌ 查询预检失败: {escape_markdown_v2(error)}", parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
        
    total_size = data.get('size', 0)
    if total_size == 0:
        msg.edit_text("🤷‍♀️ 未找到任何结果。")
        return ConversationHandler.END

    initial_results = data.get('results', [])
    initial_next_id = data.get('next')

    context.user_data['query'] = query_text
    context.user_data['total_size'] = total_size
    context.user_data['chat_id'] = update.effective_chat.id
    context.user_data['start_key'] = used_key
    context.user_data['initial_results'] = initial_results
    context.user_data['initial_next_id'] = initial_next_id
    # v10.9.4 FIX: Lock the proxy session for the background job.
    context.user_data['proxy_session'] = used_proxy

    keyboard = [
        [InlineKeyboardButton(f"♾️ 全部获取 ({total_size}条)", callback_data='allfofa_limit_none')],
        [InlineKeyboardButton("❌ 取消", callback_data='allfofa_limit_cancel')]
    ]
    msg.edit_text(
        f"✅ 查询预检成功，共发现 {total_size} 条结果。\n\n"
        "请输入您希望获取的数量上限 (例如: 50000)，或选择全部获取。",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return QUERY_STATE_ALLFOFA_GET_LIMIT

def allfofa_get_limit(update: Update, context: CallbackContext):
    limit = None
    query = update.callback_query
    
    if query:
        query.answer()
        if query.data == 'allfofa_limit_cancel':
            query.message.edit_text("操作已取消.")
            return ConversationHandler.END
        msg_target = query.message
    else:
        try:
            limit = int(update.message.text.strip())
            assert limit > 0
        except (ValueError, AssertionError):
            update.message.reply_text("❌ 无效的数字，请输入一个正整数。")
            return QUERY_STATE_ALLFOFA_GET_LIMIT
        msg_target = update.message

    context.user_data['limit'] = limit
    msg_target.reply_text(f"✅ 任务已提交！\n将使用 `next` 接口获取数据 (上限: {limit or '无'})...")
    start_download_job(context, run_allfofa_download_job, context.user_data)
    if query:
        msg_target.delete()
    return ConversationHandler.END

def run_allfofa_download_job(context: CallbackContext):
    """
    智能剥离下载器 (Smart Peeling + Time Slicing)
    核心策略: 
    1. 循环检测当前Query的数据量。
    2. >10000: 取 Top1 国家，拆分为 Slice (该国家) 和 Remaining (非该国家)。
       对 Slice 使用 Time Traceback 暴力下载。
       对 Remaining 进入下一次循环。
    3. <10000: 直接普通翻页下载。
    """
    job_data = context.job.context
    bot, chat_id = context.bot, job_data['chat_id']
    limit = job_data.get('limit')
    
    # 原始查询
    original_query = job_data['query']
    
    # 使用锁定的 Key 和 Proxy Session (从 allfofa command 初始化传过来的)
    current_key = job_data.get('start_key') 
    proxy_session = job_data.get('proxy_session')

    if not current_key:
        bot.send_message(chat_id, "❌ 内部错误：任务上下文丢失 Key 信息。")
        return

    # 输出文件名管理
    output_filename = generate_filename_from_query(original_query, prefix="smart_all")
    cache_path = os.path.join(FOFA_CACHE_DIR, output_filename)
    
    # 用于显示的进度更新
    msg = bot.send_message(chat_id, "🚀 智能剥离引擎已启动...\n正在分析数据分布...")
    stop_flag = f'stop_job_{chat_id}'
    
    current_query_scope = original_query
    collected_results = set() # 为了最后去重 (海量数据内存是个问题，但对于set str通常还能接受，如果百万级考虑落盘去重)
    
    loop_count = 0
    start_time = time.time()
    last_ui_update = 0

    try:
        while True:
            loop_count += 1
            if context.bot_data.get(stop_flag):
                msg.edit_text("🌀 任务已收到停止信号，正在中止...")
                break
                
            if limit and len(collected_results) >= limit:
                break

            # 1. 估算当前 Scope 大小
            data_size_chk, error = fetch_fofa_data(current_key, current_query_scope, page_size=1, fields="host", proxy_session=proxy_session)
            if error: 
                msg.edit_text(f"❌ 侦查失败: {error}")
                break
            
            scope_size = data_size_chk.get('size', 0)
            
            # --- 阶段 A: 小数据量直接吞噬 ---
            if scope_size <= 10000: # 小于1万，一锅端
                if loop_count == 1: 
                    msg.edit_text(f"🔍 数据量 ({scope_size}) 小于单次限制，直接下载...")
                
                # 普通翻页获取 (Normal Page Iteration)
                pages = (scope_size + 9999) // 10000
                for p in range(1, pages + 1):
                    # 获取
                    d, e = fetch_fofa_data(current_key, current_query_scope, page=p, page_size=10000, fields="host", proxy_session=proxy_session)
                    if not e and d.get('results'):
                        collected_results.update([r for r in d.get('results') if isinstance(r, str) and ':' in r])
                    
                    # 进度UI
                    if time.time() - last_ui_update > 3:
                        msg.edit_text(f"📥 直接下载中... (已收录: {len(collected_results)})")
                        last_ui_update = time.time()
                        
                break # 当前剩余的所有都在这一轮被拿走了，大循环结束

            # --- 阶段 B: 大数据量空间剥离 (Country Slicing) ---
            # 获取 Top1 国家
            stats_data, e = fetch_fofa_stats(current_key, current_query_scope, proxy_session=proxy_session)
            if e: 
                msg.edit_text(f"❌ 聚合分析失败: {e}")
                break
            
            aggs = stats_data.get("aggs", stats_data)
            countries = aggs.get("countries", [])
            
            if not countries:
                # 极端情况：查到了Size但没有Stats国家？可能是IP类型。
                # 强制进入时间切片模式 (Blind Traceback)
                top_country_code = None
            else:
                top_country_code = countries[0].get('name') # e.g., "US" or "CN"
            
            # 构造切片查询
            if top_country_code:
                slice_query = f'({current_query_scope}) && country="{top_country_code}"'
                # 剩余部分 = 当前Scope && 不等于 Top1
                next_round_query = f'({current_query_scope}) && country!="{top_country_code}"'
                slice_desc = f"国家={top_country_code}"
            else:
                # 如果没法按国家分，那就整个当做一块肉，尝试硬切 (fallback to Time Trace on whole query)
                slice_query = current_query_scope
                next_round_query = None # 没有下一轮了，这是最后一搏
                slice_desc = "全部剩余数据"

            # 对 Slice 使用深度追溯下载 (Time Peeling)
            # 用户核心策略：复用深度追溯，利用时间轴把这个巨大的 slice 扒下来
            trace_count_added = 0
            iterator = iter_fofa_traceback(current_key, slice_query, limit=limit, proxy_session=proxy_session)
            
            for batch in iterator:
                if context.bot_data.get(stop_flag): break
                
                # 批量添加
                valid_items = [item[0] for item in batch if item and isinstance(item, list) and len(item)>0]
                new_items_count = 0
                for item in valid_items:
                    if item not in collected_results:
                        collected_results.add(item)
                        new_items_count += 1
                        
                trace_count_added += new_items_count
                
                if time.time() - last_ui_update > 3:
                    try:
                        prog_bar = create_progress_bar(min(len(collected_results) / (limit or (len(collected_results)+100000)) * 100, 100))
                        msg.edit_text(
                            f"✂️ *正在剥离数据块:* `{slice_desc}`\n"
                            f"📉 策略: 时间轴降维打击 (Time Trace)\n"
                            f"{prog_bar} 总数: {len(collected_results)}\n"
                            f"(本轮新增: {trace_count_added})",
                            parse_mode=ParseMode.MARKDOWN_V2
                        )
                    except Exception: pass
                    last_ui_update = time.time()
                
                if limit and len(collected_results) >= limit: break
            
            if not next_round_query or context.bot_data.get(stop_flag):
                break
                
            # 准备进入下一轮，处理被排除了 Top1 后的剩余世界
            current_query_scope = next_round_query
            # 防止无限死循环保护 (例如 Stats 返回空但Size > 0)
            if loop_count > 50:
                msg.edit_text("⚠️ 警告：智能剥离循环次数过多，自动停止以防死锁。")
                break

    except Exception as e:
        logger.error(f"Smart download fatal error: {e}", exc_info=True)
        msg.edit_text(f"❌ 任务发生严重错误: {e}")
        return
    
    # 结果交付
    final_limit_msg = ""
    if limit and len(collected_results) >= limit: final_limit_msg = f" (已达上限 {limit})"
    
    if collected_results:
        # 排序并写入文件
        sorted_results = sorted(list(collected_results))
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(sorted_results))
            
        final_caption = f"✅ *海量下载完成*\n\n🎯 原始查询: `{escape_markdown_v2(original_query)}`\n🔢 最终获取: *{len(collected_results)}* 条{escape_markdown_v2(final_limit_msg)}\n⏱ 耗时: {int(time.time()-start_time)}s"
        send_file_safely(context, chat_id, cache_path, caption=final_caption, parse_mode=ParseMode.MARKDOWN_V2)
        upload_and_send_links(context, chat_id, cache_path)
        
        # 本地记录更新
        cache_entry = {'file_path': cache_path, 'result_count': len(collected_results)}
        add_or_update_query(original_query, cache_entry)
        
        offer_post_download_actions(context, chat_id, original_query)
        msg.delete() # 删掉进度条
        
    else:
        msg.edit_text("🤷‍♀️ 任务结束，未收集到有效数据。")
    
    context.bot_data.pop(stop_flag, None)

# --- 菜单查询处理器 (v10.9.6) ---
def prompt_for_query(update: Update, context: CallbackContext) -> int:
    """要求用户为菜单命令输入查询字符串。"""
    button_text = update.message.text
    command_map = { "常规搜索": "/kkfofa", "海量搜索": "/allfofa", "批量导出": "/batch" }
    command = command_map.get(button_text)
    if not command: return ConversationHandler.END
    context.user_data['menu_command'] = command
    update.message.reply_text(f"请输入 `{command}` 的查询语句:")
    return STATE_AWAITING_QUERY

def prompt_for_host(update: Update, context: CallbackContext) -> int:
    """要求用户为主机命令输入主机字符串。"""
    context.user_data['menu_command'] = '/host'
    update.message.reply_text("请输入要查询的主机 (IP或域名):")
    return STATE_AWAITING_HOST

def run_query_from_menu(update: Update, context: CallbackContext):
    """使用用户提供的文本运行查询命令。"""
    command = context.user_data.pop('menu_command', None)
    query_text = update.message.text
    context.args = query_text.split()

    if command == '/batch':
        return batch_command(update, context)
    elif command in ['/kkfofa', '/allfofa']:
        return query_entry_point(update, context)
    return ConversationHandler.END

def run_host_from_menu(update: Update, context: CallbackContext):
    """使用用户提供的文本运行主机命令。"""
    context.user_data.pop('menu_command', None)
    host_text = update.message.text
    context.args = host_text.split()
    
    # host_command 带有 admin_only 装饰器
    host_command(update, context)
    return ConversationHandler.END


# --- 主函数与调度器 ---
def interactive_setup():
    """Handles the initial interactive setup for the bot."""
    global CONFIG
    print("--- 首次运行或配置不完整，进入交互式设置 ---")
    bot_token = input("请输入您的 Telegram Bot Token (留空则退出): ").strip()
    if not bot_token:
        return False
    
    admin_id_str = ""
    while not admin_id_str.isdigit():
        admin_id_str = input("请输入您的 Telegram User ID (作为第一个管理员): ").strip()
        if not admin_id_str.isdigit():
            print("错误: User ID 必须是纯数字。")

    admin_id = int(admin_id_str)
    
    CONFIG["bot_token"] = bot_token
    if not CONFIG.get("admins"): # Only set admins if list is empty
        CONFIG["admins"] = [admin_id]

    fofa_keys = []
    if not CONFIG.get("apis"): # Only ask for keys if none are present
        print("请输入您的 FOFA API Key (输入空行结束):")
        while True:
            key = input(f"  - Key #{len(fofa_keys) + 1}: ").strip()
            if not key: break
            fofa_keys.append(key)
        CONFIG["apis"] = fofa_keys

    save_config()
    print("✅ 配置已保存到 config.json。")
    CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
    return True

def main() -> None:
    global CONFIG
    os.makedirs(FOFA_CACHE_DIR, exist_ok=True)

    if not os.path.exists(CONFIG_FILE) or CONFIG.get("bot_token") == "YOUR_BOT_TOKEN_HERE":
        if not interactive_setup():
            sys.exit(0)

    while True:
        try:
            bot_token = CONFIG.get("bot_token")
            if not bot_token or bot_token == "YOUR_BOT_TOKEN_HERE":
                logger.critical("错误: 'bot_token' 未在 config.json 中设置!")
                if not interactive_setup():
                    break
                continue

            check_and_classify_keys()
            updater = Updater(token=bot_token, use_context=True, request_kwargs={'read_timeout': 20, 'connect_timeout': 20})
            break  # Break loop if updater is created successfully
        except InvalidToken:
            logger.error("!!!!!! 无效的 Bot Token !!!!!!")
            print("当前配置的 Telegram Bot Token 无效。")
            if not interactive_setup():
                sys.exit(0)
        except Exception as e:
            logger.critical(f"启动时发生无法恢复的错误: {e}")
            sys.exit(1)

    dispatcher = updater.dispatcher
    dispatcher.bot_data['updater'] = updater
    commands = [
        BotCommand("start", "🚀 启动机器人"), BotCommand("help", "❓ 命令手册"),
        BotCommand("kkfofa", "🔍 资产搜索 (常规)"), BotCommand("allfofa", "🚚 资产搜索 (海量)"),
        BotCommand("host", "📦 主机详查 (智能)"), BotCommand("lowhost", "🔬 主机速查 (聚合)"),
        BotCommand("stats", "📊 全局聚合统计"), BotCommand("batchfind", "📂 批量智能分析 (Excel)"),
        BotCommand("batch", "📤 批量自定义导出 (交互式)"), BotCommand("batchcheckapi", "🔑 批量验证API Key"),
        BotCommand("check", "🩺 系统自检"), BotCommand("settings", "⚙️ 设置菜单"),
        BotCommand("history", "🕰️ 查询历史"), BotCommand("import", "🖇️ 导入旧缓存"),
        BotCommand("backup", "📤 备份配置"), BotCommand("restore", "📥 恢复配置"),
        BotCommand("update", "🔄 在线更新脚本"), BotCommand("getlog", "📄 获取日志"),
        BotCommand("shutdown", "🔌 关闭机器人"), BotCommand("stop", "🛑 停止任务"),
        BotCommand("cancel", "❌ 取消操作")
    ]
    try: updater.bot.set_my_commands(commands)
    except Exception as e: logger.warning(f"设置机器人命令失败: {e}")
    settings_conv = ConversationHandler(
        entry_points=[CommandHandler("settings", settings_command)],
        states={
            SETTINGS_STATE_MAIN: [CallbackQueryHandler(settings_callback_handler, pattern=r"^settings_")],
            SETTINGS_STATE_ACTION: [
                CallbackQueryHandler(settings_action_handler, pattern=r"^action_"),
                CallbackQueryHandler(show_update_menu, pattern=r"^settings_update"),
                CallbackQueryHandler(show_backup_restore_menu, pattern=r"^settings_backup"),
                CallbackQueryHandler(backup_config_command, pattern=r"^backup_now"),
                CallbackQueryHandler(lambda u,c: restore_config_command(u.callback_query.message, c), pattern=r"^restore_now"),
                CallbackQueryHandler(get_update_url, pattern=r"^update_set_url"),
                CallbackQueryHandler(settings_command, pattern=r"^(update_back|backup_back)"),
            ],
            SETTINGS_STATE_ADMIN_MENU: [CallbackQueryHandler(admin_menu_callback, pattern=r"^admin_")],
            SETTINGS_STATE_GET_ADMIN_ID_TO_ADD: [MessageHandler(Filters.text & ~Filters.command, get_admin_id_to_add)],
            SETTINGS_STATE_GET_ADMIN_ID_TO_REMOVE: [MessageHandler(Filters.text & ~Filters.command, get_admin_id_to_remove)],
            SETTINGS_STATE_GET_KEY: [MessageHandler(Filters.text & ~Filters.command, get_key)],
            SETTINGS_STATE_REMOVE_API: [MessageHandler(Filters.text & ~Filters.command, remove_api)],
            SETTINGS_STATE_PRESET_MENU: [CallbackQueryHandler(preset_menu_callback, pattern=r"^preset_")],
            SETTINGS_STATE_GET_PRESET_NAME: [MessageHandler(Filters.text & ~Filters.command, get_preset_name)],
            SETTINGS_STATE_GET_PRESET_QUERY: [MessageHandler(Filters.text & ~Filters.command, get_preset_query)],
            SETTINGS_STATE_REMOVE_PRESET: [MessageHandler(Filters.text & ~Filters.command, remove_preset)],
            SETTINGS_STATE_GET_UPDATE_URL: [MessageHandler(Filters.text & ~Filters.command, get_update_url)],
            SETTINGS_STATE_PROXYPOOL_MENU: [CallbackQueryHandler(proxypool_menu_callback, pattern=r"^proxypool_")],
            SETTINGS_STATE_GET_PROXY_ADD: [MessageHandler(Filters.text & ~Filters.command, get_proxy_to_add)],
            SETTINGS_STATE_GET_PROXY_REMOVE: [MessageHandler(Filters.text & ~Filters.command, get_proxy_to_remove)],
            SETTINGS_STATE_UPLOAD_API_MENU: [CallbackQueryHandler(upload_api_menu_callback, pattern=r"^upload_")],
            SETTINGS_STATE_GET_UPLOAD_URL: [MessageHandler(Filters.text & ~Filters.command, get_upload_url)],
            SETTINGS_STATE_GET_UPLOAD_TOKEN: [MessageHandler(Filters.text & ~Filters.command, get_upload_token)],
        },
        fallbacks=[CommandHandler("cancel", cancel)], conversation_timeout=300,
    )
    query_conv = ConversationHandler(
        entry_points=[ CommandHandler("kkfofa", query_entry_point), CommandHandler("allfofa", query_entry_point), CallbackQueryHandler(query_entry_point, pattern=r"^run_preset_") ],
        states={
            QUERY_STATE_GET_GUEST_KEY: [MessageHandler(Filters.text & ~Filters.command, get_guest_key)],
            QUERY_STATE_ASK_CONTINENT: [CallbackQueryHandler(ask_continent_callback, pattern=r"^continent_")], 
            QUERY_STATE_CONTINENT_CHOICE: [CallbackQueryHandler(continent_choice_callback, pattern=r"^continent_")], 
            QUERY_STATE_CACHE_CHOICE: [CallbackQueryHandler(cache_choice_callback, pattern=r"^cache_")],
            QUERY_STATE_KKFOFA_MODE: [CallbackQueryHandler(query_mode_callback, pattern=r"^mode_")],
            QUERY_STATE_GET_TRACEBACK_LIMIT: [MessageHandler(Filters.text & ~Filters.command, get_traceback_limit), CallbackQueryHandler(get_traceback_limit, pattern=r"^limit_")],
            QUERY_STATE_ALLFOFA_GET_LIMIT: [MessageHandler(Filters.text & ~Filters.command, allfofa_get_limit), CallbackQueryHandler(allfofa_get_limit, pattern=r"^allfofa_limit_")]
        },
        fallbacks=[CommandHandler("cancel", cancel)], conversation_timeout=300,
    )
    batch_conv = ConversationHandler(
        entry_points=[CommandHandler("batch", batch_command)], 
        states={
            BATCH_STATE_SELECT_FIELDS: [CallbackQueryHandler(batch_select_fields_callback, pattern=r"^batchfield_")],
            BATCH_STATE_MODE_CHOICE: [CallbackQueryHandler(query_mode_callback, pattern=r"^mode_")],
            BATCH_STATE_GET_LIMIT: [MessageHandler(Filters.text & ~Filters.command, get_traceback_limit), CallbackQueryHandler(get_traceback_limit, pattern=r"^limit_")]
        },
        fallbacks=[CommandHandler('cancel', cancel)], conversation_timeout=600,
    )
    import_conv = ConversationHandler(entry_points=[CommandHandler("import", import_command)], states={IMPORT_STATE_GET_FILE: [MessageHandler(Filters.document.mime_type("text/plain"), get_import_query)]}, fallbacks=[CommandHandler("cancel", cancel)], conversation_timeout=300)
    stats_conv = ConversationHandler(entry_points=[CommandHandler("stats", stats_command)], states={STATS_STATE_GET_QUERY: [MessageHandler(Filters.text & ~Filters.command, get_fofa_stats_query)]}, fallbacks=[CommandHandler("cancel", cancel)], conversation_timeout=300)
    batchfind_conv = ConversationHandler(entry_points=[CommandHandler("batchfind", batchfind_command)], states={BATCHFIND_STATE_GET_FILE: [MessageHandler(Filters.document.mime_type("text/plain"), get_batch_file_handler)], BATCHFIND_STATE_SELECT_FEATURES: [CallbackQueryHandler(select_batch_features_callback, pattern=r"^batchfeature_")]}, fallbacks=[CommandHandler("cancel", cancel)], conversation_timeout=300)
    restore_conv = ConversationHandler(entry_points=[CommandHandler("restore", restore_config_command)], states={RESTORE_STATE_GET_FILE: [MessageHandler(Filters.document, receive_config_file)]}, fallbacks=[CommandHandler("cancel", cancel)], conversation_timeout=300)
    scan_conv = ConversationHandler(entry_points=[CallbackQueryHandler(start_scan_callback, pattern=r'^start_scan_')], states={SCAN_STATE_GET_CONCURRENCY: [MessageHandler(Filters.text & ~Filters.command, get_concurrency_callback)], SCAN_STATE_GET_TIMEOUT: [MessageHandler(Filters.text & ~Filters.command, get_timeout_callback)]}, fallbacks=[CommandHandler('cancel', cancel)], conversation_timeout=120)
    batch_check_api_conv = ConversationHandler(entry_points=[CommandHandler("batchcheckapi", batch_check_api_command)], states={BATCHCHECKAPI_STATE_GET_FILE: [MessageHandler(Filters.document.mime_type("text/plain"), receive_api_file)]}, fallbacks=[CommandHandler("cancel", cancel)], conversation_timeout=300)
    
    dispatcher.add_handler(CommandHandler("start", start_command)); dispatcher.add_handler(CommandHandler("help", help_command)); dispatcher.add_handler(CommandHandler("host", host_command)); dispatcher.add_handler(CommandHandler("lowhost", lowhost_command)); dispatcher.add_handler(CommandHandler("check", check_command)); dispatcher.add_handler(CommandHandler("stop", stop_all_tasks)); dispatcher.add_handler(CommandHandler("backup", backup_config_command)); dispatcher.add_handler(CommandHandler("history", history_command)); dispatcher.add_handler(CommandHandler("getlog", get_log_command)); dispatcher.add_handler(CommandHandler("shutdown", shutdown_command)); dispatcher.add_handler(CommandHandler("update", update_script_command)); dispatcher.add_handler(CommandHandler("monitor", monitor_command)) # 注册监控命令
    dispatcher.add_handler(InlineQueryHandler(inline_fofa_handler)); 
    
    # --- 恢复监控任务 ---
    if MONITOR_TASKS:
        count = 0
        for task_id, task in MONITOR_TASKS.items():
            if task.get('status') == 'active':
                # 计算初始延迟：分散启动，避免洪峰 (0 - 60s)
                delay = random.randint(5, 60)
                updater.job_queue.run_once(run_monitor_execution_job, delay, context={"task_id": task_id, "is_restore": True}, name=f"monitor_{task_id}")
                count += 1
        logger.info(f"已恢复 {count} 个监控任务。")

    # --- 主菜单按钮处理器 (v10.9.6) ---
    menu_conv = ConversationHandler(
        entry_points=[
            MessageHandler(Filters.regex('^常规搜索$'), prompt_for_query),
            MessageHandler(Filters.regex('^海量搜索$'), prompt_for_query),
            MessageHandler(Filters.regex('^批量导出$'), prompt_for_query),
            MessageHandler(Filters.regex('^主机详查$'), prompt_for_host),
        ],
        states={
            STATE_AWAITING_QUERY: [MessageHandler(Filters.text & ~Filters.command, run_query_from_menu)],
            STATE_AWAITING_HOST: [MessageHandler(Filters.text & ~Filters.command, run_host_from_menu)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        conversation_timeout=300
    )
    dispatcher.add_handler(menu_conv)
    dispatcher.add_handler(MessageHandler(Filters.regex(r'^设置$'), settings_command))
    dispatcher.add_handler(MessageHandler(Filters.regex(r'^帮助手册$'), help_command))

    dispatcher.add_handler(settings_conv); dispatcher.add_handler(query_conv); dispatcher.add_handler(batch_conv); dispatcher.add_handler(import_conv); dispatcher.add_handler(stats_conv); dispatcher.add_handler(batchfind_conv); dispatcher.add_handler(restore_conv); dispatcher.add_handler(scan_conv); dispatcher.add_handler(batch_check_api_conv)
    
    logger.info(f"🚀 Fofa Bot v10.9 (稳定版) 已启动...")
    updater.start_polling()
    updater.idle()
    logger.info("Bot has been shut down gracefully.")

if __name__ == "__main__":
    main()
