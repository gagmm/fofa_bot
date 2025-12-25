import os
import logging
import json
import re
from datetime import datetime
from functools import wraps
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ConversationHandler,
    CallbackQueryHandler
)
import traceback
import asyncio
import glob
import zipfile
import tempfile
import math

# --- 全局变量和配置 ---
CONFIG_FILE = 'config.json'
HISTORY_FILE = 'history.json'
DEFAULT_CONFIG = {
    "bot_token": "YOUR_BOT_TOKEN_HERE",
    "apis": [],
    "admins": [],
    "proxy": "",
    "proxies": [],
    "full_mode": False,
    "public_mode": False,
    "presets": [],
    "update_url": "",
    "upload_api_url": "",
    "upload_api_token": "",
    "show_download_links": True  # 新增: 控制是否显示下载链接
}

# --- 日志记录 ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- 辅助函数 ---
def load_json_file(filename, default_data):
    """加载JSON文件，如果文件不存在或为空则创建并使用默认数据"""
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        with open(filename, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return default_data
    else:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(default_data, f, indent=4)
        return default_data

def save_json_file(filename, data):
    """保存数据到JSON文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def escape_markdown_v2(text: str) -> str:
    """转义MarkdownV2特殊字符"""
    if not isinstance(text, str):
        text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

async def send_file_safely(update: Update, context: CallbackContext, filepath: str):
    """
    安全地发送文件，处理大小限制。
    策略：
    1. 尝试直接发送。
    2. 如果失败 (过大)，尝试压缩后发送。
    3. 如果压缩后仍过大，进行分卷压缩发送。
    """
    if not os.path.exists(filepath):
        await update.message.reply_text("⛔️ 目标文件不存在。")
        return

    file_size = os.path.getsize(filepath)
    max_size = 50 * 1024 * 1024  # Telegram API限制 (50 MB)
    chat_id = update.effective_chat.id
    base_filename = os.path.basename(filepath)

    try:
        # 策略1: 直接发送
        if file_size <= max_size:
            logger.info(f"文件大小 ({file_size} bytes) 在限制内，尝试直接发送 {filepath}")
            await context.bot.send_document(chat_id, document=open(filepath, 'rb'), connect_timeout=60, read_timeout=60)
            return
        
        # 策略2: 压缩后发送
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip_file:
            zip_path = tmp_zip_file.name
        
        logger.info(f"文件过大，尝试压缩到 {zip_path}")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(filepath, arcname=base_filename)
        
        zip_size = os.path.getsize(zip_path)
        if zip_size <= max_size:
            logger.info(f"压缩后大小 ({zip_size} bytes) 在限制内，发送压缩文件。")
            await context.bot.send_document(chat_id, document=open(zip_path, 'rb'), connect_timeout=60, read_timeout=60,
                                            caption=f"文件 '{base_filename}' 因过大已被压缩。")
            os.remove(zip_path)
            return
        
        os.remove(zip_path) # 清理未分卷的压缩文件

        # 策略3: 分卷压缩发送
        logger.info(f"压缩后仍过大，开始分卷压缩。")
        split_size = max_size - (1024 * 1024) # 留出1MB的余量
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_base_name = os.path.join(tmp_dir, base_filename)
            part_num = 1
            with zipfile.ZipFile(f"{zip_base_name}.zip.001", 'w', zipfile.ZIP_DEFLATED) as zf:
                total_size = 0
                
                # 创建一个虚拟的大文件来进行分卷测试
                # In a real scenario, you'd process the actual large file
                zf.write(filepath, arcname=base_filename)
            
            # 这部分需要一个更复杂的逻辑来真实地分割一个大文件的压缩流
            # `zipfile`库不直接支持分卷写入。需要手动管理流。
            # 以下是一个简化的实现，直接分割压缩好的文件
            with open(zip_path, 'rb') as f_in:
                 part_num = 1
                 while True:
                    part_filename = f"{zip_path}.{str(part_num).zfill(3)}"
                    with open(part_filename, 'wb') as f_out:
                        chunk = f_in.read(split_size)
                        if not chunk:
                            break
                        f_out.write(chunk)
                    
                    logger.info(f"发送分卷: {part_filename}")
                    await context.bot.send_document(chat_id, document=open(part_filename, 'rb'),
                                                    caption=f"'{base_filename}' 分卷 {part_num}",
                                                    connect_timeout=60, read_timeout=60)
                    os.remove(part_filename)
                    part_num += 1

            if os.path.exists(zip_path):
                 os.remove(zip_path)

            await update.message.reply_text(f"✅ 文件已分卷压缩并发送完毕。请下载所有分卷后解压。")


    except Exception as e:
        logger.error(f"发送文件 {filepath} 时出错: {e}", exc_info=True)
        await update.message.reply_text(f"⛔️ 发送文件时遇到错误: {e}")


# --- 加载配置 ---
CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
HISTORY = load_json_file(HISTORY_FILE, {})

# --- 权限和装饰器 ---
def is_super_admin(user_id: int) -> bool:
    """检查用户是否为超级管理员（管理员列表中的第一个）"""
    return user_id in CONFIG['admins'] and CONFIG['admins'].index(user_id) == 0

def is_admin(user_id: int) -> bool:
    """检查用户是否为管理员"""
    return user_id in CONFIG['admins']

def admin_only(func):
    """装饰器：限制只有管理员才能访问"""
    @wraps(func)
    def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        if not is_admin(update.effective_user.id):
            message_text = "⛔️ 抱歉，您不是授权管理员。"
            if update.callback_query:
                update.callback_query.answer(message_text, show_alert=True)
            elif update.message:
                update.message.reply_text(message_text)
            return None
        return func(update, context, *args, **kwargs)
    return wrapped

def super_admin_only(func):
    """装饰器：限制只有超级管理员才能访问"""
    @wraps(func)
    def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        if not is_super_admin(update.effective_user.id):
            message_text = "⛔️ 抱歉，此为超级管理员专属功能。"
            if update.callback_query:
                update.callback_query.answer(message_text, show_alert=True)
            elif update.message:
                update.message.reply_text(message_text)
            return None
        return func(update, context, *args, **kwargs)
    return wrapped

# --- 命令处理函数 ---

@admin_only
async def start_command(update: Update, context: CallbackContext):
    """处理 /start 命令"""
    user = update.effective_user
    await update.message.reply_html(
        f"👋 你好, {user.mention_html()}!\n\n这是一个FOFA查询机器人。使用 /help 查看可用命令。",
    )
    
@admin_only
async def help_command(update: Update, context: CallbackContext):
    """处理 /help 命令"""
    # ... 省略了 help 内容的定义 ...
    is_super = is_super_admin(update.effective_user.id)
    
    base_commands = """
*查询功能*
/fofa <query> \- 执行FOFA查询
/preview <file> \- 快速预览文件内容
/history \- 查看查询历史
/file <query> \- 在查询历史中搜索并获取文件
    """
    
    super_admin_commands = """
*⚙️ 超级管理员设置*
/settings \- 打开设置菜单
/addadmin <user\_id> \- 添加管理员
/deladmin <user\_id> \- 删除管理员
/addapi <name> <email> <key> \- 添加FOFA API
/delapi <name> \- 删除FOFA API
/setproxy <url> \- 设置代理
/delproxy <url> \- 删除代理
/addpreset <name> <query> \- 添加预设查询
/delpreset <name> \- 删除预设查询
/setuploadapi <url> <token> \- 设置上传API
/backup \- 备份所有JSON配置文件
/selfupdate \- 自我更新
    """

    help_text = escape_markdown_v2("可用命令:\n") + base_commands
    if is_super:
        help_text += super_admin_commands

    await update.message.reply_text(help_text, parse_mode='MarkdownV2')


# --- 设置菜单 (超级管理员专属) ---
SETTINGS_MENU, ADMIN_SETTINGS_MENU = range(2)

@super_admin_only
async def settings_command(update: Update, context: CallbackContext):
    """显示设置菜单"""
    keyboard = [
        [InlineKeyboardButton(f"综合查询模式: {'✅' if CONFIG.get('full_mode') else '❌'}", callback_data='toggle_full_mode')],
        [InlineKeyboardButton(f"公开模式: {'✅' if CONFIG.get('public_mode') else '❌'}", callback_data='toggle_public_mode')],
        [InlineKeyboardButton(f"显示下载链接: {'✅' if CONFIG.get('show_download_links', True) else '❌'}", callback_data='toggle_show_download_links')],
        [InlineKeyboardButton("关闭菜单", callback_data='close_settings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text('⚙️ *机器人设置*', reply_markup=reply_markup, parse_mode='Markdown')
    return SETTINGS_MENU

@admin_only
async def settings_menu_handler(update: Update, context: CallbackContext):
    """处理普通管理员和超级管理员的菜单访问"""
    if is_super_admin(update.effective_user.id):
        # 完整的设置菜单
        return await settings_command(update, context)
    else:
        # 受限的菜单（或无菜单）
        await update.message.reply_text(" केवल सुपर एडमिन ही सेटिंग्स बदल सकते हैं।")
        return ConversationHandler.END

async def toggle_setting_handler(update: Update, context: CallbackContext):
    """处理切换配置选项"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if not is_super_admin(user_id):
        await query.answer("⛔️ 权限不足！", show_alert=True)
        return SETTINGS_MENU
        
    toggle_map = {
        'toggle_full_mode': 'full_mode',
        'toggle_public_mode': 'public_mode',
        'toggle_show_download_links': 'show_download_links'
    }
    
    setting_key = toggle_map.get(query.data)
    if not setting_key:
        await query.answer("内部错误", show_alert=True)
        return SETTINGS_MENU

    CONFIG[setting_key] = not CONFIG.get(setting_key, False if setting_key != 'show_download_links' else True)
    save_json_file(CONFIG_FILE, CONFIG)

    # 重新生成键盘以反映新状态
    keyboard = [
        [InlineKeyboardButton(f"综合查询模式: {'✅' if CONFIG.get('full_mode') else '❌'}", callback_data='toggle_full_mode')],
        [InlineKeyboardButton(f"公开模式: {'✅' if CONFIG.get('public_mode') else '❌'}", callback_data='toggle_public_mode')],
        [InlineKeyboardButton(f"显示下载链接: {'✅' if CONFIG.get('show_download_links', True) else '❌'}", callback_data='toggle_show_download_links')],
        [InlineKeyboardButton("关闭菜单", callback_data='close_settings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text='⚙️ *机器人设置*', reply_markup=reply_markup, parse_mode='Markdown')
    return SETTINGS_MENU

async def close_settings_handler(update: Update, context: CallbackContext):
    """关闭设置菜单"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(text="设置菜单已关闭。")
    return ConversationHandler.END
    
async def invalid_settings_option(update: Update, context: CallbackContext):
    """处理无效的设置选项"""
    await update.message.reply_text("无效选项，请重试。")
    return SETTINGS_MENU

# --- 配置文件管理 (备份/恢复) ---
@super_admin_only
async def backup_config_command(update: Update, context: CallbackContext):
    """备份所有.json文件到一个zip压缩包中"""
    chat_id = update.effective_chat.id
    try:
        json_files = glob.glob('*.json')
        if not json_files:
            await update.message.reply_text("未找到任何 .json 文件进行备份。")
            return

        # 创建一个临时文件来存储zip
        with tempfile.NamedTemporaryFile(prefix='backup_', suffix='.zip', delete=False) as tmp_zip_file:
            zip_filename = tmp_zip_file.name

        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file in json_files:
                zf.write(file)
        
        await context.bot.send_document(
            chat_id=chat_id,
            document=open(zip_filename, 'rb'),
            filename=f"bot_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            caption=f"✅ 成功备份 {len(json_files)} 个配置文件。"
        )
        os.remove(zip_filename) # 发送后删除临时文件

    except Exception as e:
        logger.error(f"备份失败: {e}", exc_info=True)
        await update.message.reply_text(f"⛔️ 备份过程中发生错误: {e}")

RECEIVING_CONFIG = range(1)
async def receive_config_file_entry(update: Update, context: CallbackContext):
    if not is_super_admin(update.effective_user.id):
        await update.message.reply_text("⛔️ 抱歉，此为超级管理员专属功能。")
        return ConversationHandler.END
    await update.message.reply_text("请直接发送您的 `config.json` 或包含所有配置的 `.zip` 备份文件给我。")
    return RECEIVING_CONFIG

async def receive_config_file(update: Update, context: CallbackContext):
    """接收并处理用户上传的配置文件"""
    # 修复了 global 声明的语法
    global CONFIG
    global HISTORY
    
    document = update.message.document
    if not document:
        await update.message.reply_text("请发送文件。")
        return RECEIVING_CONFIG

    file_extension = os.path.splitext(document.file_name)[1].lower()
    
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, document.file_name)
            bot_file = await context.bot.get_file(document.file_id)
            await bot_file.download_to_drive(custom_path=file_path)

            if file_extension == '.json':
                # 处理单个JSON文件
                os.replace(file_path, CONFIG_FILE)
                CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
                await update.message.reply_text("✅ `config.json` 已成功更新。机器人将自动重启以应用新配置。")

            elif file_extension == '.zip':
                # 处理ZIP备份文件
                with zipfile.ZipFile(file_path, 'r') as zf:
                    json_files_in_zip = [f for f in zf.namelist() if f.endswith('.json')]
                    if not json_files_in_zip:
                        await update.message.reply_text("⛔️ Zip文件中未找到任何 .json 配置文件。")
                        return RECEIVING_CONFIG
                    
                    zf.extractall('.') # 解压到当前目录
                
                # 重新加载配置
                CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
                HISTORY = load_json_file(HISTORY_FILE, {})

                await update.message.reply_text(
                    f"✅ 已从zip文件中成功恢复 {len(json_files_in_zip)} 个文件。机器人将自动重启。"
                )
            else:
                await update.message.reply_text("⛔️ 不支持的文件类型。请发送 `.json` 或 `.zip` 文件。")
                return RECEIVING_CONFIG

        # 触发重启
        asyncio.create_task(context.application.shutdown())
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"恢复配置失败: {e}", exc_info=True)
        await update.message.reply_text(f"⛔️ 处理文件时出错: {e}")
        return ConversationHandler.END

async def cancel_receive_config(update: Update, context: CallbackContext):
    """取消接收文件"""
    await update.message.reply_text("已取消操作。")
    return ConversationHandler.END
    
# 其他被省略的命令（addadmin, deladmin, 等）将在这里
# 为了简洁，我们仅实现核心逻辑
# ...
@super_admin_only
async def add_admin_command(update: Update, context: CallbackContext):
    try:
        user_id = int(context.args[0])
        if user_id not in CONFIG['admins']:
            CONFIG['admins'].append(user_id)
            save_json_file(CONFIG_FILE, CONFIG)
            await update.message.reply_text(f"✅ 管理员 {user_id} 添加成功。")
        else:
            await update.message.reply_text(f"ℹ️ 用户 {user_id} 已经是管理员。")
    except (IndexError, ValueError):
        await update.message.reply_text("用法: /addadmin <user_id>")

@super_admin_only
async def del_admin_command(update: Update, context: CallbackContext):
    try:
        user_id = int(context.args[0])
        if user_id in CONFIG['admins']:
            # 超级管理员不能被删除
            if CONFIG['admins'].index(user_id) == 0:
                await update.message.reply_text("⛔️ 不能删除超级管理员。")
                return
            CONFIG['admins'].remove(user_id)
            save_json_file(CONFIG_FILE, CONFIG)
            await update.message.reply_text(f"✅ 管理员 {user_id} 删除成功。")
        else:
            await update.message.reply_text(f"ℹ️ 用户 {user_id} 不是管理员。")
    except (IndexError, ValueError):
        await update.message.reply_text("用法: /deladmin <user_id>")

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
        
        error_str = str(error)
        if "[820031]" in error_str:
            logger.warning(f"Key [#{key_num}] F点余额不足...");
            continue
        if "[45022]" in error_str:
            logger.warning(f"Key [#{key_num}] 今日请求次数已达上限...");
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

async def async_scanner_orchestrator(scan_targets, concurrency, timeout, progress_callback=None):
    semaphore = asyncio.Semaphore(concurrency)
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

    try: msg.edit_text("1/3: 正在解析和加载目标...")
    except (BadRequest, RetryAfter, TimedOut): pass
    
    try:
        with open(cached_item['cache']['file_path'], 'r', encoding='utf-8') as f:
            raw_targets = [line.strip() for line in f if line.strip()]
    except Exception as e:
        try: msg.edit_text(f"❌ 读取缓存文件失败: {e}")
        except (BadRequest, RetryAfter, TimedOut): pass
        return
        
    scan_targets = []
    scan_type_text = ""
    if mode == 'tcping':
        scan_type_text = "TCP存活扫描"
        for t in raw_targets:
            try:
                # Handle URLs with schema
                if t.startswith('http://') or t.startswith('https://'):
                    parsed_url = urlparse(t)
                    hostname = parsed_url.hostname
                    port = parsed_url.port
                    if port is None:
                        port = 443 if parsed_url.scheme == 'https' else 80
                    if hostname:
                        # Strip brackets from IPv6 hostnames for socket connection
                        hostname = hostname.strip("[]")
                        scan_targets.append((hostname, port))
                    continue
                
                # Handle IPv6 in brackets like [ipv6]:port
                match = re.match(r'\[([a-fA-F0-9:]+)\]:(\d+)', t)
                if match:
                    scan_targets.append((match.group(1), int(match.group(2))))
                    continue

                # Handle host:port (IPv4 or domain)
                host, port_str = t.rsplit(':', 1)
                if host and port_str:
                    scan_targets.append((host, int(port_str)))
            except (ValueError, IndexError):
                logger.warning(f"无法解析扫描目标: {t}, 已跳过。")
                continue

    elif mode == 'subnet':
        scan_type_text = "子网扫描"
        subnets_to_ports = {}
        for line in raw_targets:
            try:
                ip_str, port_str = line.strip().split(':'); port = int(port_str)
                # Basic check for IPv4 before splitting
                if '.' in ip_str and len(ip_str.split('.')) == 4:
                    subnet = ".".join(ip_str.split('.')[:3])
                    if subnet not in subnets_to_ports: subnets_to_ports[subnet] = set()
                    subnets_to_ports[subnet].add(port)
                else:
                    logger.warning(f"子网扫描跳过非IPv4目标: {line}")
            except ValueError:
                logger.warning(f"子网扫描无法解析行: {line}")
                continue
        for subnet, ports in subnets_to_ports.items():
            for i in range(1, 255):
                for port in ports:
                    scan_targets.append((f"{subnet}.{i}", port))

    if not scan_targets:
        try: msg.edit_text("🤷‍♀️ 未能从文件中解析出任何有效的目标。请检查文件内容格式。")
        except (BadRequest, RetryAfter, TimedOut): pass
        return
        
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

        initial_message = f"2/3: 已加载 {len(scan_targets)} 个有效目标，开始异步{scan_type_text} (并发: {concurrency}, 超时: {timeout}s)..."
        try:
            msg.edit_text(initial_message)
        except (BadRequest, RetryAfter, TimedOut):
            pass

        return await async_scanner_orchestrator(scan_targets, concurrency, timeout, progress_callback)

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
                    f"{escape_markdown_v2(progress_bar)} {i}/{total_codes}\n"
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
        msg.edit_text(f"✅ 分片扫描完成\!\n总计发现 *{final_count}* 条唯一数据。\n正在生成并发送文件\.\.\.", parse_mode=ParseMode.MARKDOWN_V2)
        
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
        try:
            msg.edit_text(f"✅ 深度追溯完成！共 {len(unique_results)} 条。{termination_reason}\n正在发送文件...")
        except (BadRequest, RetryAfter, TimedOut): pass
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
            # 修改点：将 ( ) 改为 \( \)
            update.message.reply_text(f"⚠️ 任务已存在 \(ID: `{task_id}`\)", parse_mode=ParseMode.MARKDOWN_V2)
            return
            
        MONITOR_TASKS[task_id] = {
            "query": query_text,
            "chat_id": update.effective_chat.id,
            "added_at": int(time.time()),
            "last_run": 0,
            "interval": 3600, # 初始1小时
            "status": "active",
            "unnotified_count": 0, # 新增：未通知计数器
            "notification_threshold": 5000 # 新增：通知阈值
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

            msg.append(f"📡 `{tid}`: *{escape_markdown_v2(task['query'][:25] + '...')}*")
            msg.append(f"   📦 库存: *{count}* \\| ⏱ 上次: {last_run_str} \\| ⏳ 频率: {escape_markdown_v2(dur)}")
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
    """自适应监控雷达核心逻辑 (v2)"""
    job_context = context.job.context
    task_id = job_context.get('task_id')
    
    if task_id not in MONITOR_TASKS: return
    task = MONITOR_TASKS[task_id]
    
    query_text = task['query']
    os.makedirs(MONITOR_DATA_DIR, exist_ok=True)
    db_file = os.path.join(MONITOR_DATA_DIR, f"{task_id}.txt")
    
    # 1. 载入本地数据库哈希
    known_hashes = set()
    if os.path.exists(db_file):
        try:
            with open(db_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line: known_hashes.add(hashlib.md5(line.encode()).hexdigest())
        except Exception as e:
            logger.error(f"读取监控数据库失败: {e}")

    # 2. 执行数据收集 (由“探测”改为“收集”)
    fetch_func = lambda k, kl, ps: fetch_fofa_data(k, query_text, page=1, page_size=5000, fields="host", proxy_session=ps)
    data, _, _, _, _, error = execute_query_with_fallback(fetch_func)
    
    new_data_lines = []
    if not error and data and data.get('results'):
        results = data.get('results')
        for item in results:
            line_str = item[0] if isinstance(item, list) else str(item)
            line_str = line_str.strip()
            if not line_str: continue
            h = hashlib.md5(line_str.encode()).hexdigest()
            if h not in known_hashes:
                new_data_lines.append(line_str)
                known_hashes.add(h) # 在会话中也添加，防止单次查询内重复
                
    # 3. 智能调频与通知
    num_new_found = len(new_data_lines)
    current_interval = task.get('interval', 3600)
    unnotified_count = task.get('unnotified_count', 0)
    notification_threshold = task.get('notification_threshold', 5000)

    if num_new_found > 0:
        # 发现新目标，写入数据库
        with open(db_file, 'a', encoding='utf-8') as f:
            f.write("\n".join(new_data_lines) + "\n")
        
        unnotified_count += num_new_found
        
        # 检查是否达到通知阈值
        if unnotified_count >= notification_threshold:
            try:
                chat_id = task.get('chat_id')
                if chat_id:
                    notif_text = (
                        f"📡 *监控雷达命中* \\(Task: `{task_id}`\\)\n"
                        f"查询: `{escape_markdown_v2(query_text[:30])}`\\.\\.\\.\n"
                        f"发现 *{unnotified_count}* 个新目标\!\n"
                        f"已沉淀至本地库，可使用 `/monitor get {task_id}` 提取\\."
                    )
                    context.bot.send_message(chat_id, notif_text, parse_mode=ParseMode.MARKDOWN_V2)
                    unnotified_count = 0 # 重置计数器
            except Exception as e:
                logger.error(f"发送监控通知失败: {e}")

        # 智能调频：根据本次发现数量调整下次间隔
        if num_new_found < 100: # 少量发现，说明不是爆发期
            new_interval = min(43200, int(current_interval * 1.2)) # 稍微延长
        else: # 大量发现，说明是爆发期
            new_interval = max(600, int(current_interval * 0.7)) # 缩短间隔
    else:
        # 无新数据，进入冷却，延长间隔
        new_interval = min(43200, int(current_interval * 1.5))

    # 更新任务状态
    task['last_run'] = int(time.time())
    task['interval'] = new_interval
    task['unnotified_count'] = unnotified_count
    save_monitor_tasks()
    
    # 4. 安排下一次运行 (加入抖动，并处理 RuntimeError)
    jitter = random.randint(int(-new_interval * 0.1), int(new_interval * 0.1))
    next_run_delay = new_interval + jitter
    
    try:
        context.job_queue.run_once(run_monitor_execution_job, next_run_delay, context={"task_id": task_id}, name=f"monitor_{task_id}")
    except RuntimeError as e:
        logger.warning(f"无法安排新的监控任务 (可能正在关闭): {e}")

# --- 核心命令处理 ---
def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    welcome_text = f'👋 欢迎, {user.first_name}！\n请选择一个操作:'
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
        
        msg_text = (
            f"{success_message}\n"
            f"检测到大量结果 \\({total_size}条\\)\\。由于单次查询上限 \\(10,000\\)，您可以：\n\n"
            f"1️⃣ *前1万*：仅下载最近的1万条\\。\n"
            f"2️⃣ *分片下载*：按国家自动拆分，尽可能通过积少成多突破1万条限制 \\(消耗更多请求\\)\\。\n"
            f"3️⃣ *深度追溯*：按时间回溯 \\(需高等级Key\\)\\。"
        )
        msg.edit_text(msg_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
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
        if d.get('title'): port_info.append(f"  \- *标题:* `{escape_markdown_v2(d.get('title'))}`")
        if d.get('server'): port_info.append(f"  \- *服务:* `{escape_markdown_v2(d.get('server'))}`")
        if d.get('icp'): port_info.append(f"  \- *ICP:* `{escape_markdown_v2(d.get('icp'))}`")
        if d.get('jarm'): port_info.append(f"  \- *JARM:* `{escape_markdown_v2(d.get('jarm'))}`")
        cert_str = d.get('cert', '{}')
        try:
            cert_info = json.loads(cert_str) if isinstance(cert_str, str) and cert_str.startswith('{') else {}
            if cert_info.get('issuer', {}).get('CN'): port_info.append(f"  \- *证书颁发者:* `{escape_markdown_v2(cert_info['issuer']['CN'])}`")
            if cert_info.get('subject', {}).get('CN'): port_info.append(f"  \- *证书使用者:* `{escape_markdown_v2(cert_info['subject']['CN'])}`")
        except json.JSONDecodeError:
            pass
        if d.get('header'): port_info.append(f"  \- *Header:* ```\n{d.get('header')}\n```")
        if d.get('banner'): port_info.append(f"  \- *Banner:* ```\n{d.get('banner')}\n```")
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
        # 修改点：将所有的 - 改为 \-
        if port_info.get('product'): port_str += f"\n  \- *产品:* `{escape_markdown_v2(port_info.get('product'))}`"
        if port_info.get('title'): port_str += f"\n  \- *标题:* `{escape_markdown_v2(port_info.get('title'))}`"
        if port_info.get('jarm'): port_str += f"\n  \- *JARM:* `{escape_markdown_v2(port_info.get('jarm'))}`"
        if port_info.get('banner'): port_str += f"\n  \- *Banner:* ```\n{port_info.get('banner')}\n```"        
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
@super_admin_only
def backup_config_command(update: Update, context: CallbackContext):
    if update.callback_query:
        update.callback_query.answer()
    chat_id = update.effective_chat.id
    backup_filename = f"fofabot_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    
    json_files = glob.glob('*.json')
    if not json_files:
        context.bot.send_message(chat_id, "🤷‍♀️ 未找到任何 \\.json 配置文件可以备份。")
        return
        
    msg = context.bot.send_message(chat_id, f"📦 正在打包所有 {len(json_files)} 个 \\.json 配置文件...")
    
    try:
        with zipfile.ZipFile(backup_filename, 'w', zipfile.ZIP_DEFLATED) as zf:
            for f in json_files:
                zf.write(f)
        
        msg.edit_text("✅ 打包完成，正在发送备份文件...")
        send_file_safely(context, chat_id, backup_filename, caption=f"FofaBot 完整配置备份({len(json_files)}个文件)")
        upload_and_send_links(context, chat_id, backup_filename)
        os.remove(backup_filename)
        
    except Exception as e:
        logger.error(f"创建备份失败: {e}")
        msg.edit_text(f"❌ 创建备份压缩文件时出错: {escape_markdown_v2(str(e))}", parse_mode=ParseMode.MARKDOWN_V2)

@super_admin_only
def restore_config_command(update: Update, context: CallbackContext):
    update.message.reply_text("请发送您的 `config.json` 或 `.zip` 格式的备份文件。")
    return RESTORE_STATE_GET_FILE
def receive_config_file(update: Update, context: CallbackContext):
    global CONFIG
    doc = update.message.document
    file_name = doc.file_name.lower()
    
    # 恢复 .zip 备份
    if file_name.endswith('.zip'):
        msg = update.message.reply_text("解压并恢复 ZIP 备份中...")
        zip_path = os.path.join(FOFA_CACHE_DIR, doc.file_name)
        doc.get_file().download(custom_path=zip_path)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                if 'config.json' not in zf.namelist():
                    msg.edit_text("❌ 压缩包中缺少 `config.json`，恢复失败。")
                    return ConversationHandler.END
                
                zf.extractall('.')
            
            os.remove(zip_path)
            CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
            msg.edit_text("✅ 已从ZIP成功恢复所有配置文件。机器人将自动重启。")
            shutdown_command(update, context, restart=True)
            
        except Exception as e:
            logger.error(f"恢复ZIP备份时出错: {e}")
            msg.edit_text(f"❌ 恢复备份失败: {escape_markdown_v2(str(e))}", parse_mode=ParseMode.MARKDOWN_V2)

        return ConversationHandler.END
    
    # 恢复单个 config.json
    elif file_name == 'config.json':
        doc.get_file().download(custom_path=CONFIG_FILE)
        CONFIG = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
        update.message.reply_text("✅ 配置文件已恢复。机器人将自动重启。")
        shutdown_command(update, context, restart=True)
        return ConversationHandler.END

    else:
        update.message.reply_text("❌ 文件格式错误，请上传 `config.json` 或 `.zip` 备份文件。")
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

@super_admin_only
def shutdown_command(update: Update, context: CallbackContext, restart=False):
    message = "🤖 机器人正在重启..." if restart else "🤖 机器人正在关闭..."
    update.message.reply_text(message)
    logger.info(f"Shutdown/Restart initiated by user {update.effective_user.id}")
    
    # v10.9 FIX: Use OS signals for a truly robust and deadlock-free shutdown.
    # This sends a SIGINT signal (like Ctrl+C) to the bot's own process,
    # which updater.idle() is designed to catch gracefully.
    threading.Thread(target=lambda: (time.sleep(1), os.kill(os.getpid(), signal.SIGINT))).start()

@super_admin_only
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
@super_admin_only
def settings_command(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("🔑 API 管理", callback_data='settings_api'), InlineKeyboardButton("✨ 预设管理", callback_data='settings_preset')],
        [InlineKeyboardButton("🌐 代理池管理", callback_data='settings_proxypool'), InlineKeyboardButton("📡 监控设置", callback_data='settings_monitor')],
        [InlineKeyboardButton("📤 上传接口设置", callback_data='settings_upload'), InlineKeyboardButton("👨‍💼 管理员设置", callback_data='settings_admin')],
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
    if menu == 'monitor': return show_monitor_menu(update, context)
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
    if not update.message or not update.message.text:
        return SETTINGS_STATE_GET_UPDATE_URL
        
    url = update.message.text.strip()
    if url.lower().startswith('http'): 
        CONFIG['update_url'] = url
        save_config()
        update.message.reply_text("✅ 更新URL已设置。")
    else: 
        update.message.reply_text("❌ 无效的URL格式。")
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
    links_status = "✅ 显示" if CONFIG.get("show_download_links", True) else "❌ 隐藏"
    
    text = (f"📤 *上传接口设置*\n\n"
            f"此功能可将生成文件上传到您指定的服务器，并返回下载命令。\n\n"
            f"*API URL:* `{escape_markdown_v2(url)}`\n"
            f"*API Token:* `{token_status}`\n"
            f"*下载链接:* `{links_status}`")
    kbd = [
        [InlineKeyboardButton("✏️ 设置 URL", callback_data='upload_set_url'), InlineKeyboardButton("🔑 设置 Token", callback_data='upload_set_token')],
        [InlineKeyboardButton(f"🔗 切换链接显示", callback_data='upload_toggle_links')],
        [InlineKeyboardButton("🔙 返回", callback_data='upload_back')]
    ]
    query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(kbd), parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_UPLOAD_API_MENU

def upload_api_menu_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); action = query.data.split('_', 1)[1]
    
    if action == 'toggle_links':
        current_status = CONFIG.get("show_download_links", True)
        CONFIG["show_download_links"] = not current_status
        save_config()
        return show_upload_api_menu(update, context)
        
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

# --- Monitor Settings Menu ---
def show_monitor_menu(update: Update, context: CallbackContext):
    query = getattr(update, 'callback_query', None)
    
    msg = ["*📡 监控任务管理*"]
    
    tasks = {k: v for k, v in MONITOR_TASKS.items() if v.get('status') == 'active'}
    
    if not tasks:
        msg.append("\n_当前没有活跃的监控任务。_")
    else:
        for tid, task in tasks.items():
            data_file = os.path.join(MONITOR_DATA_DIR, f"{tid}.txt")
            count = 0
            if os.path.exists(data_file):
                try: 
                    with open(data_file, 'r', encoding='utf-8') as f: count = sum(1 for _ in f)
                except Exception: pass
            
            next_run_str = "未知"
            jobs = context.job_queue.get_jobs_by_name(f"monitor_{tid}")
            if jobs:
                # Use next_t for next run time
                next_run_dt = jobs[0].next_t
                if isinstance(next_run_dt, datetime):
                     next_run_str = next_run_dt.astimezone(tz.tzlocal()).strftime('%H:%M:%S')
                else: 
                     next_run_str = "计划中..."
            else:
                 last_run = task.get('last_run', 0)
                 if last_run == 0:
                     next_run_str = "首次运行"
                 else:
                     next_run_str = "已暂停" 

            threshold = task.get('notification_threshold', 5000)
            
            query_preview = task['query']
            if len(query_preview) > 25: query_preview = query_preview[:25] + '...'
            
            msg.append(f"\n📡 `{tid}`: *{escape_markdown_v2(query_preview)}*")
            msg.append(f"   📦 库存: *{count}* \| 通知阈值: *{threshold}*")
            msg.append(f"   ⏱ 下次运行: *{next_run_str}*")

    keyboard = [
        [
            InlineKeyboardButton("➕ 添加", callback_data='monitor_add'),
            InlineKeyboardButton("➖ 移除", callback_data='monitor_remove'),
            InlineKeyboardButton("⚙️ 配置", callback_data='monitor_config')
        ],
        [InlineKeyboardButton("🔙 返回", callback_data='monitor_back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if query:
        query.message.edit_text("\n".join(msg), reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
    elif update.message:
        update.message.reply_text("\n".join(msg), reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
        
    return SETTINGS_STATE_MONITOR_MENU

def monitor_menu_callback(update: Update, context: CallbackContext):
    query = update.callback_query; query.answer(); action = query.data.split('_')[1]
    if action == 'back': return settings_command(update, context)
    if action == 'add': 
        query.message.edit_text("请输入要添加的监控查询语句:"); 
        return SETTINGS_STATE_GET_MONITOR_QUERY_TO_ADD
    if action == 'remove': 
        query.message.edit_text("请输入要移除的监控任务ID:"); 
        return SETTINGS_STATE_GET_MONITOR_ID_TO_REMOVE
    if action == 'config':
        query.message.edit_text("请输入您想配置的监控任务ID:")
        return SETTINGS_STATE_GET_MONITOR_ID_TO_CONFIG

def get_monitor_query_to_add(update: Update, context: CallbackContext):
    query_text = update.message.text.strip()
    task_id = hashlib.md5(query_text.encode()).hexdigest()[:8]
    if task_id in MONITOR_TASKS:
        update.message.reply_text(f"⚠️ 任务已存在 (ID: `{task_id}`)", parse_mode=ParseMode.MARKDOWN_V2)
    else:
        MONITOR_TASKS[task_id] = {
            "query": query_text, "chat_id": update.effective_chat.id,
            "added_at": int(time.time()), "last_run": 0, "interval": 3600,
            "status": "active", "unnotified_count": 0, "notification_threshold": 5000
        }
        save_monitor_tasks()
        context.job_queue.run_once(run_monitor_execution_job, 1, context={"task_id": task_id}, name=f"monitor_{task_id}")
        update.message.reply_text(f"✅ 监控已添加，ID: `{task_id}`", parse_mode=ParseMode.MARKDOWN_V2)
    
    return show_monitor_menu(update, context)

def get_monitor_id_to_remove(update: Update, context: CallbackContext):
    tid = update.message.text.strip()
    if tid in MONITOR_TASKS:
        for job in context.job_queue.get_jobs_by_name(f"monitor_{tid}"):
            job.schedule_removal()
        del MONITOR_TASKS[tid]
        save_monitor_tasks()
        update.message.reply_text(f"🗑️ 任务 `{tid}` 已停止并移除。", parse_mode=ParseMode.MARKDOWN_V2)
    else:
        update.message.reply_text("❌ 任务ID不存在。")
    return show_monitor_menu(update, context)

def get_monitor_id_to_config(update: Update, context: CallbackContext):
    tid = update.message.text.strip()
    if tid not in MONITOR_TASKS:
        update.message.reply_text("❌ 任务ID不存在。请重新输入。")
        return SETTINGS_STATE_GET_MONITOR_ID_TO_CONFIG
    context.user_data['config_monitor_id'] = tid
    task = MONITOR_TASKS[tid]
    current_threshold = task.get('notification_threshold', 5000)
    update.message.reply_text(f"正在配置任务 `{tid}`。\n当前通知阈值为: *{current_threshold}*。\n\n请输入新的阈值 \(数字\):", parse_mode=ParseMode.MARKDOWN_V2)
    return SETTINGS_STATE_GET_MONITOR_THRESHOLD

def get_monitor_threshold(update: Update, context: CallbackContext):
    try:
        threshold = int(update.message.text.strip())
        if threshold < 0: raise ValueError
        tid = context.user_data.pop('config_monitor_id')
        MONITOR_TASKS[tid]['notification_threshold'] = threshold
        save_monitor_tasks()
        update.message.reply_text(f"✅ 任务 `{tid}` 的通知阈值已更新为 *{threshold}*。", parse_mode=ParseMode.MARKDOWN_V2)
    except (ValueError, KeyError):
        update.message.reply_text("❌ 无效输入。请输入一个非负整数。")
        return SETTINGS_STATE_GET_MONITOR_THRESHOLD
    return show_monitor_menu(update, context)


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
                        # 修改点：对 slice_desc 使用 escape_markdown_v2
                        msg.edit_text(
                            f"✂️ *正在剥离数据块:* `{escape_markdown_v2(slice_desc)}`\n"
                            f"📉 策略: 时间轴降维打击 (Time Trace)\n"
                            f"{prog_bar} 总数: {len(collected_results)}\n"
                            f"\\(本轮新增: {trace_count_added}\\)", # 建议：这里的括号也顺手转义一下，虽然不是必须
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


# --- /preview 命令 (v10.9.7) ---
def _build_preview_message(context: CallbackContext, page: int):
    """构建预览消息文本和按钮。"""
    results = context.user_data.get('preview_results', [])
    total_pages = context.user_data.get('preview_total_pages', 0)
    query_text = context.user_data.get('preview_query', 'N/A')

    if not results:
        return "没有可供预览的结果。", None

    start_index = (page - 1) * PREVIEW_PAGE_SIZE
    end_index = start_index + PREVIEW_PAGE_SIZE
    page_results = results[start_index:end_index]
    
    # [ip, port, title]
    message_parts = [f"📄 *预览: `{escape_markdown_v2(query_text)}`* \\(第 {page}/{total_pages} 页\\)\n"]
    for item in page_results:
        ip, port, title = item[0], item[1], item[2]
        title_str = escape_markdown_v2(title.strip()) if title else "_无标题_"
        # 修改点：将 - 修改为 \-
        message_parts.append(f"`{escape_markdown_v2(ip)}:{port}` \\- {title_str}")
    
    message = "\n".join(message_parts)

    keyboard_row = []
    if page > 1:
        keyboard_row.append(InlineKeyboardButton("⬅️ 上一页", callback_data="preview_prev"))
    keyboard_row.append(InlineKeyboardButton("❌ 关闭", callback_data="preview_close"))
    if page < total_pages:
        keyboard_row.append(InlineKeyboardButton("下一页 ➡️", callback_data="preview_next"))
    
    return message, InlineKeyboardMarkup([keyboard_row])

def preview_command(update: Update, context: CallbackContext) -> int:
    """/preview 和 /p 命令的入口点。"""
    if not context.args:
        update.message.reply_text("用法: `/preview <FOFA 查询语句>`\n此命令用于快速预览少量数据。", parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
        
    query_text = " ".join(context.args)
    msg = update.message.reply_text("⏳ 正在获取预览数据...")

    def query_logic(key, key_level, proxy_session):
        # 请求50条数据，字段为 ip, port, title
        return fetch_fofa_data(key, query_text, page=1, page_size=50, fields="ip,port,title", proxy_session=proxy_session)

    data, _, _, _, _, error = execute_query_with_fallback(query_logic, min_level=0)

    if error:
        msg.edit_text(f"❌ 预览失败: {error}")
        return ConversationHandler.END

    results = data.get('results', [])
    if not results:
        msg.edit_text("🤷‍♀️ 未找到任何结果。")
        return ConversationHandler.END

    context.user_data['preview_results'] = results
    context.user_data['preview_query'] = query_text
    context.user_data['preview_page'] = 1
    total_pages = (len(results) - 1) // PREVIEW_PAGE_SIZE + 1
    context.user_data['preview_total_pages'] = total_pages

    message_text, reply_markup = _build_preview_message(context, page=1)
    
    msg.edit_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN_V2
    )

    return PREVIEW_STATE_PAGINATE

def preview_page_callback(update: Update, context: CallbackContext):
    """处理预览翻页按钮。"""
    query = update.callback_query
    query.answer()
    
    action = query.data.split('_')[1]
    
    current_page = context.user_data.get('preview_page', 1)
    
    if action == "close":
        query.message.edit_text("预览已关闭。")
        context.user_data.clear()
        return ConversationHandler.END
        
    elif action == "next":
        new_page = current_page + 1
    elif action == "prev":
        new_page = current_page - 1
    else:
        return PREVIEW_STATE_PAGINATE
        
    total_pages = context.user_data.get('preview_total_pages', 0)
    if not 1 <= new_page <= total_pages:
        return PREVIEW_STATE_PAGINATE

    context.user_data['preview_page'] = new_page
    
    message_text, reply_markup = _build_preview_message(context, page=new_page)
    
    try:
        query.edit_message_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.error(f"编辑预览消息时出错: {e}")

    return PREVIEW_STATE_PAGINATE

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
        BotCommand("preview", "📄 快速预览"),
        BotCommand("kkfofa", "🔍 资产搜索 (常规)"), BotCommand("allfofa", "🚚 资产搜索 (海量)"),
        BotCommand("host", "📦 主机详查 (智能)"), BotCommand("lowhost", "🔬 主机速查 (聚合)"),
        BotCommand("stats", "📊 全局聚合统计"), BotCommand("batchfind", "📂 批量智能分析 (Excel)"),
        BotCommand("batch", "📤 批量自定义导出 (交互式)"), BotCommand("batchcheckapi", "🔑 批量验证API Key"),
        BotCommand("check", "🩺 系统自检"), BotCommand("settings", "⚙️ 设置菜单"),
        BotCommand("history", "🕰️ 查询历史"), BotCommand("import", "🖇️ 导入旧缓存"),
        BotCommand("backup", "📤 备份配置"), BotCommand("restore", "📥 恢复配置"),
        BotCommand("update", "🔄 在线更新脚本"), BotCommand("getlog", "📄 获取日志"),
        BotCommand("shutdown", "🔌 关闭机器人"), BotCommand("stop", "🛑 停止任务"),
        BotCommand("monitor", "📡 监控雷达 (添加/列表/删除)"), BotCommand("cancel", "❌ 取消操作")
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
            
            # 监控设置状态
            SETTINGS_STATE_MONITOR_MENU: [CallbackQueryHandler(monitor_menu_callback, pattern=r"^monitor_")],
            SETTINGS_STATE_GET_MONITOR_QUERY_TO_ADD: [MessageHandler(Filters.text & ~Filters.command, get_monitor_query_to_add)],
            SETTINGS_STATE_GET_MONITOR_ID_TO_REMOVE: [MessageHandler(Filters.text & ~Filters.command, get_monitor_id_to_remove)],
            SETTINGS_STATE_GET_MONITOR_ID_TO_CONFIG: [MessageHandler(Filters.text & ~Filters.command, get_monitor_id_to_config)],
            SETTINGS_STATE_GET_MONITOR_THRESHOLD: [MessageHandler(Filters.text & ~Filters.command, get_monitor_threshold)],

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
    
    # 新增预览功能的会话处理器
    preview_conv = ConversationHandler(
        entry_points=[CommandHandler("preview", preview_command)],
        states={
            PREVIEW_STATE_PAGINATE: [CallbackQueryHandler(preview_page_callback, pattern=r"^preview_")]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        conversation_timeout=300
    )

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
    dispatcher.add_handler(settings_conv); dispatcher.add_handler(query_conv); dispatcher.add_handler(batch_conv); dispatcher.add_handler(import_conv); dispatcher.add_handler(stats_conv); dispatcher.add_handler(batchfind_conv); dispatcher.add_handler(restore_conv); dispatcher.add_handler(scan_conv); dispatcher.add_handler(batch_check_api_conv); dispatcher.add_handler(preview_conv)
    
    logger.info(f"🚀 Fofa Bot v10.9 (稳定版) 已启动...")
    updater.start_polling()
    updater.idle()
    logger.info("Bot has been shut down gracefully.")

if __name__ == "__main__":
    main()
