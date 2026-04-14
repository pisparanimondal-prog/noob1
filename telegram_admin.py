"""
Telegram Admin Bot
Manages the OTP bot (multiple API sources) via Telegram commands and inline buttons.
All admin actions happen inside Telegram — no web page needed.
"""
import json
import os
import uuid
import logging
import threading
import time
import requests
import html as _html
from datetime import datetime

import sys
import telebot
from telebot import types

BOT_DIR = os.path.dirname(__file__)
if BOT_DIR not in sys.path:
    sys.path.insert(0, BOT_DIR)
CONFIG_PATH = os.path.join(BOT_DIR, 'config.json')
ACTIVITY_LOG_PATH = os.path.join(BOT_DIR, 'activity_log.json')
BOT_STATUS_PATH = os.path.join(BOT_DIR, 'bot_status.json')

log = logging.getLogger('TelegramAdmin')

# Per-user conversation state  {chat_id: {'step': str, 'data': dict}}
_states = {}

# API IDs that have already been tested this session (one test per panel)
_tested_apis = set()
_state_lock = threading.Lock()


# ─── Config helpers ──────────────────────────────────────────────────────────

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def save_config(cfg):
    with open(CONFIG_PATH, 'w') as f:
        json.dump(cfg, f, indent=2)


def load_activity():
    if not os.path.exists(ACTIVITY_LOG_PATH):
        return []
    try:
        with open(ACTIVITY_LOG_PATH) as f:
            return json.load(f)
    except Exception:
        return []


def load_status():
    if not os.path.exists(BOT_STATUS_PATH):
        return {}
    try:
        with open(BOT_STATUS_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


# ─── Auth helpers ─────────────────────────────────────────────────────────────

def is_admin(chat_id):
    cfg = load_config()
    admins = cfg['settings'].get('admin_ids', [])
    if not admins:
        return True  # open access until admins are configured
    return int(chat_id) in [int(a) for a in admins]


# ─── Keyboard helpers ─────────────────────────────────────────────────────────

def _btn(text, *, cb=None, url=None, style=None, copy=None):
    b = {'text': text}
    if cb:
        b['callback_data'] = cb
    if url:
        b['url'] = url
    if style:
        b['style'] = style
    if copy:
        b['copy_text'] = {'text': copy}
    return b


def _jkb(rows):
    return json.dumps({'inline_keyboard': rows})


# ─── Keyboard builders ────────────────────────────────────────────────────────

def main_menu_kb():
    s = _get_btn_styles()
    return _jkb([
        [_btn("📡  My APIs",     cb="menu:apis",     style=s.get('info') or None),   _btn("➕  Add API",     cb="menu:add",  style=s.get('add') or None)],
        [_btn("📊  Stats",       cb="menu:stats",    style=s.get('info') or None),   _btn("📜  Recent OTPs", cb="menu:recent", style=s.get('info') or None)],
        [_btn("⚙️  Settings",   cb="menu:settings", style=s.get('info') or None),   _btn("📨  Send Test",  cb="menu:test", style=s.get('test') or None)],
        [_btn("📶  API Status",  cb="menu:status",   style=s.get('info') or None)],
    ])


def back_kb(target="main"):
    s = _get_btn_styles()
    return _jkb([[_btn("🔙  Back", cb=f"back:{target}", style=s.get('back') or None)]])


def api_list_kb(apis, statuses):
    s = _get_btn_styles()
    rows = []
    for api in apis:
        enabled = api.get('enabled', False)
        toggle_label = "⏸ Disable" if enabled else "▶️ Enable"
        toggle_style = s.get('disable') or None if enabled else s.get('enable') or None
        rows.append([_btn(api['name'], cb=f"api:info:{api['id']}", style=s.get('info') or None)])
        rows.append([
            _btn(toggle_label,  cb=f"api:toggle:{api['id']}", style=toggle_style),
            _btn("✏️ Edit",     cb=f"api:edit:{api['id']}",   style=s.get('edit') or None),
            _btn("🗑 Delete",   cb=f"api:delete:{api['id']}", style=s.get('delete') or None),
        ])
    rows.append([_btn("🔙  Back", cb="back:main", style=s.get('back') or None)])
    return _jkb(rows)


def confirm_delete_kb(api_id):
    s = _get_btn_styles()
    return _jkb([[
        _btn("✅  Yes, delete", cb=f"api:confirm_delete:{api_id}", style=s.get('delete') or None),
        _btn("❌  Cancel",      cb="menu:apis",                    style=s.get('cancel') or None),
    ], [_btn("🔙  Back", cb="menu:apis", style=s.get('back') or None)]])


def settings_kb():
    s = _get_btn_styles()
    return _jkb([
        [_btn("🤖  Change Bot Token",         cb="set:token",    style=s.get('edit') or None)],
        [_btn("💬  Manage OTP Chat IDs",      cb="set:chatid",   style=s.get('edit') or None)],
        [_btn("⏱  Change Polling Interval",   cb="set:interval", style=s.get('edit') or None)],
        [_btn("🔗  Bot & Channel Links",      cb="set:links",    style=s.get('edit') or None)],
        [_btn("🎨  Button Colors",            cb="set:btnstyle", style=s.get('test') or None)],
        [_btn("🔙  Back",                     cb="back:main",    style=s.get('back') or None)],
    ])


# Style → human-readable label
_STYLE_LABEL = {
    'success': '🟢 Green',
    'primary': '🔵 Blue',
    'danger':  '🔴 Red',
    '':        '⚪ Default',
}

# Internal key → display name
_BTN_NAME = {
    # OTP message buttons
    'copy':     '📋 Copy OTP',
    'bot_link': '🤖 Bot Link',
    'channel':  '📢 Channel',
    # Admin action buttons
    'delete':   '🗑 Delete',
    'disable':  '⏸ Disable',
    'enable':   '▶️ Enable',
    'add':      '➕ Add',
    'cancel':   '❌ Cancel',
    'edit':     '✏️ Edit',
    'back':     '🔙 Back',
    'test':     '🧪 Test / Re-Login',
    'info':     '📡 View / Info',
}

# Grouped for the color-picker overview
_BTN_GROUPS = [
    ('📨 OTP Message Buttons', ('copy', 'bot_link', 'channel')),
    ('🛠 Admin Action Buttons', ('delete', 'disable', 'enable', 'add', 'cancel')),
    ('🔧 Navigation & Other',  ('edit', 'back', 'test', 'info')),
]


def _get_btn_styles():
    cfg = load_config()
    defaults = {
        'copy': 'success', 'bot_link': 'primary', 'channel': 'primary',
        'delete': 'danger', 'disable': 'danger',
        'enable': 'success', 'add': 'success', 'cancel': '',
        'edit': '', 'back': '', 'test': 'primary', 'info': '',
    }
    return {**defaults, **cfg.get('button_styles', {})}


def _bstyle(key):
    """Return the configured style string for a button key, or None (= default theme)."""
    s = _get_btn_styles().get(key, '')
    return s if s else None


def btnstyle_overview_text(styles):
    lines = ["🎨 <b>Button Colors</b>\n"]
    for group_title, keys in _BTN_GROUPS:
        lines.append(f"<b>{group_title}</b>")
        for key in keys:
            label = _STYLE_LABEL.get(styles.get(key, ''), '⚪ Default')
            lines.append(f"  {_BTN_NAME[key]}  →  <b>{label}</b>")
        lines.append("")
    lines.append("Tap a button below to change its color:")
    return "\n".join(lines)


def btnstyle_overview_kb(styles):
    s = _get_btn_styles()
    rows = []
    for _group_title, keys in _BTN_GROUPS:
        for key in keys:
            st = styles.get(key, '')
            label = _STYLE_LABEL.get(st, '⚪ Default')
            rows.append([_btn(f"{_BTN_NAME[key]}  [{label}]", cb=f"btnstyle:pick:{key}", style=st or None)])
    rows.append([_btn("🔙  Back", cb="menu:settings", style=s.get('back') or None)])
    return _jkb(rows)


def btnstyle_pick_kb(btn_key):
    s = _get_btn_styles()
    return _jkb([
        [
            _btn("🟢 Green",   cb=f"btnstyle:set:{btn_key}:success", style="success"),
            _btn("🔵 Blue",    cb=f"btnstyle:set:{btn_key}:primary", style="primary"),
        ],
        [
            _btn("🔴 Red",     cb=f"btnstyle:set:{btn_key}:danger",  style="danger"),
            _btn("⚪ Default", cb=f"btnstyle:set:{btn_key}:"),
        ],
        [_btn("🔙  Back", cb="set:btnstyle", style=s.get('back') or None)],
    ])


def chatids_menu_text(chat_ids):
    if not chat_ids:
        return "💬 <b>OTP Chat IDs</b>\n\n<i>No chat IDs configured yet.</i>"
    lines = "\n".join(f"  {i+1}. <code>{cid}</code>" for i, cid in enumerate(chat_ids))
    return f"💬 <b>OTP Chat IDs</b>\n\n{lines}\n\nTap 🗑 to remove an ID or ➕ to add a new one."


def chatids_menu_kb(chat_ids):
    s = _get_btn_styles()
    rows = []
    for i, cid in enumerate(chat_ids):
        short = cid if len(cid) <= 18 else cid[:15] + '...'
        rows.append([
            _btn(f"📋 {short}", cb=f"chatid:copy:{i}", style=s.get('info') or None),
            _btn("🗑 Delete",   cb=f"chatid:del:{i}",  style=s.get('delete') or None),
        ])
    rows.append([_btn("➕ Add Chat ID", cb="chatid:add",      style=s.get('add') or None)])
    rows.append([_btn("🔙 Back",        cb="menu:settings",   style=s.get('back') or None)])
    return _jkb(rows)


def edit_api_kb(api_id):
    s = _get_btn_styles()
    return _jkb([
        [_btn("🔄  Re-Login (Auto)",  cb=f"api:relogin:{api_id}",      style=s.get('test') or None)],
        [_btn("🧪  Test SMS",         cb=f"api:testsms:{api_id}",      style=s.get('test') or None)],
        [_btn("✏️  Edit Name",        cb=f"edit:name:{api_id}",        style=s.get('edit') or None)],
        [_btn("👤🔒  Edit Username & Password", cb=f"edit:credentials:{api_id}", style=s.get('edit') or None)],
        [_btn("🍪  Edit PHPSESSID",   cb=f"edit:sessid:{api_id}",      style=s.get('edit') or None)],
        [_btn("🔑  Edit Sesskey",     cb=f"edit:sesskey:{api_id}",     style=s.get('edit') or None)],
        [_btn("🌐  Edit Referer",     cb=f"edit:referer:{api_id}",     style=s.get('edit') or None)],
        [_btn("🔗  Edit Data URL",    cb=f"edit:url:{api_id}",         style=s.get('edit') or None)],
        [_btn("⏱  Edit Poll Interval", cb=f"edit:interval:{api_id}",  style=s.get('edit') or None)],
        [_btn("🔙  Back to APIs",     cb="menu:apis",                  style=s.get('back') or None)],
    ])


# ─── Text builders ────────────────────────────────────────────────────────────

def build_main_text():
    cfg = load_config()
    apis = cfg.get('apis', [])
    st = load_status()
    active = sum(1 for a in apis if a.get('enabled') and st.get(a['id'], {}).get('status') == 'active')
    errors = sum(1 for a in apis if st.get(a['id'], {}).get('status') == 'error')
    total_sent = sum(st.get(a['id'], {}).get('otps_sent', 0) for a in apis)
    return (
        "🤖 <b>OTP Bot Admin Panel</b>\n\n"
        f"📡 APIs configured: <b>{len(apis)}</b>\n"
        f"🟢 Active: <b>{active}</b>  |  🔴 Errors: <b>{errors}</b>\n"
        f"🔑 Total OTPs sent: <b>{total_sent}</b>\n\n"
        "Choose an action below:"
    )


def build_api_list_text(apis, statuses):
    if not apis:
        return "📡 <b>No APIs configured yet.</b>\n\nUse <b>➕ Add API</b> to add your first one."
    lines = ["📡 <b>API Sources</b>\n"]
    for i, api in enumerate(apis, 1):
        st = statuses.get(api['id'], {})
        if not api.get('enabled'):
            status_str = "⚪ Disabled"
        elif st.get('status') == 'active':
            status_str = "🟢 Active"
        elif st.get('status') == 'error':
            err = st.get('error', '')[:60]
            status_str = f"🔴 Error: {err}"
        else:
            status_str = "🟡 Starting..."
        sent = st.get('otps_sent', 0)
        last = st.get('last_check', '—')
        if last != '—':
            last = last[:19].replace('T', ' ')
        url_display = api['url'] if len(api['url']) <= 50 else api['url'][:50] + '...'
        lines.append(
            f"<b>{i}. {api['name']}</b>\n"
            f"   Status: {status_str}\n"
            f"   OTPs sent: <b>{sent}</b> | Last: {last}\n"
            f"   URL: <code>{url_display}</code>"
        )
    return "\n\n".join(lines)


def build_stats_text():
    cfg = load_config()
    apis = cfg.get('apis', [])
    statuses = load_status()
    activity = load_activity()
    total_sent = sum(statuses.get(a['id'], {}).get('otps_sent', 0) for a in apis)
    lines = [
        "📊 <b>Bot Statistics</b>\n",
        f"🔌 Total APIs: <b>{len(apis)}</b>",
        f"🟢 Active: <b>{sum(1 for a in apis if a.get('enabled') and statuses.get(a['id'], {}).get('status') == 'active')}</b>",
        f"🔴 Errors: <b>{sum(1 for a in apis if statuses.get(a['id'], {}).get('status') == 'error')}</b>",
        f"🔑 OTPs sent (session): <b>{total_sent}</b>",
        f"📜 Activity log size: <b>{len(activity)}</b>",
        "\n<b>Per-API breakdown:</b>",
    ]
    for api in apis:
        st = statuses.get(api['id'], {})
        sent = st.get('otps_sent', 0)
        status_icon = "🟢" if api.get('enabled') and st.get('status') == 'active' else "🔴" if st.get('status') == 'error' else "⚪"
        lines.append(f"  {status_icon} {api['name']}: <b>{sent}</b> OTPs")
    return "\n".join(lines)


def build_api_status_text():
    cfg = load_config()
    apis = cfg.get('apis', [])
    statuses = load_status()
    now = datetime.now().strftime('%H:%M:%S')
    lines = [f"📶 <b>API Status</b>  <i>(as of {now})</i>\n"]
    if not apis:
        lines.append("⚠️ <i>No APIs configured yet.\nUse ➕ Add API from the main menu to get started.</i>")
        return "\n\n".join(lines)
    for api in apis:
        st = statuses.get(api['id'], {})
        if not api.get('enabled'):
            icon = "⚪"
            status_label = "Disabled"
            bar = "░░░░░░░░░░"
        elif st.get('status') == 'active':
            icon = "🟢"
            status_label = "Active ✅"
            bar = "▓▓▓▓▓▓▓▓▓▓"
        elif st.get('status') == 'error':
            icon = "🔴"
            status_label = "Error ❌"
            bar = "▒▒▒▒▒▒▒▒▒▒"
        else:
            icon = "🟡"
            status_label = "Starting..."
            bar = "░░░▓░░░▓░░"
        sent = st.get('otps_sent', 0)
        last = st.get('last_check', '—')
        if last and last != '—':
            last = last[11:19]
        err = _html.escape((st.get('error') or '')[:100])
        err_line = f"\n   ⚠️ <i>{err}</i>" if err and st.get('status') == 'error' else ''
        lines.append(
            f"{icon} <b>{_html.escape(api['name'])}</b>\n"
            f"   {bar}\n"
            f"   Status: <b>{status_label}</b>  |  OTPs: <b>{sent}</b>\n"
            f"   Last check: <code>{last}</code>{err_line}"
        )
    return "\n\n".join(lines)


def status_kb():
    s = _get_btn_styles()
    return _jkb([
        [_btn("🔄 Refresh", cb="menu:status", style=s.get('test') or None), _btn("🔙 Back", cb="back:main", style=s.get('back') or None)],
    ])


def build_recent_text():
    activity = load_activity()[:15]
    if not activity:
        return "📜 <b>No OTPs recorded yet.</b>\n\nThe bot is running and will log here once an OTP is detected."
    lines = ["📜 <b>Recent OTPs (last 15)</b>\n"]
    for e in activity:
        ts = e.get('timestamp', '')[:19].replace('T', ' ')
        lines.append(
            f"⏰ {ts}\n"
            f"📞 <code>{e.get('number', '?')}</code>  |  🔑 <b>{e.get('otp_code', '?')}</b>\n"
            f"📱 {e.get('service', '?')}  |  {e.get('country', '?')}\n"
            f"🔌 {e.get('api_name', '?')}"
        )
    return "\n\n".join(lines)


# ─── State machine ────────────────────────────────────────────────────────────

def set_state(chat_id, step, data=None):
    with _state_lock:
        _states[chat_id] = {'step': step, 'data': data or {}}


def get_state(chat_id):
    with _state_lock:
        return _states.get(chat_id)


def clear_state(chat_id):
    with _state_lock:
        _states.pop(chat_id, None)


# ─── Bot setup ────────────────────────────────────────────────────────────────

def create_bot():
    cfg = load_config()
    bot = telebot.TeleBot(cfg['telegram']['bot_token'], parse_mode='HTML')

    # ── /start  /menu ────────────────────────────────────────────────────────

    def _deny_msg(msg):
        bot.send_message(msg.chat.id, "⛔ <b>Access Denied</b>\n\nYou are not authorised to use this bot.")

    def _ack(call, text='', alert=False):
        """Immediately acknowledge a callback query. Must be first call in every handler."""
        try:
            bot.answer_callback_query(call.id, text, show_alert=alert)
        except Exception:
            pass

    def _deny_cb(call):
        bot.answer_callback_query(call.id, "⛔ Access Denied", show_alert=True)

    @bot.message_handler(commands=['start', 'menu'])
    def cmd_start(msg):
        if not is_admin(msg.from_user.id):
            return _deny_msg(msg)
        clear_state(msg.chat.id)
        bot.send_message(msg.chat.id, build_main_text(), reply_markup=main_menu_kb())

    @bot.message_handler(commands=['apis'])
    def cmd_apis(msg):
        if not is_admin(msg.from_user.id):
            return _deny_msg(msg)
        clear_state(msg.chat.id)
        cfg = load_config()
        apis = cfg.get('apis', [])
        st = load_status()
        bot.send_message(msg.chat.id, build_api_list_text(apis, st), reply_markup=api_list_kb(apis, st))

    @bot.message_handler(commands=['stats'])
    def cmd_stats(msg):
        if not is_admin(msg.from_user.id):
            return _deny_msg(msg)
        clear_state(msg.chat.id)
        bot.send_message(msg.chat.id, build_stats_text(), reply_markup=back_kb("main"))

    @bot.message_handler(commands=['add'])
    def cmd_add(msg):
        if not is_admin(msg.from_user.id):
            return _deny_msg(msg)
        _start_add_flow(bot, msg.chat.id)

    @bot.message_handler(commands=['recent'])
    def cmd_recent(msg):
        if not is_admin(msg.from_user.id):
            return _deny_msg(msg)
        clear_state(msg.chat.id)
        bot.send_message(msg.chat.id, build_recent_text(), reply_markup=back_kb("main"))

    @bot.message_handler(commands=['status'])
    def cmd_status(msg):
        if not is_admin(msg.from_user.id):
            return _deny_msg(msg)
        clear_state(msg.chat.id)
        bot.send_message(msg.chat.id, build_api_status_text(), reply_markup=status_kb())

    @bot.message_handler(commands=['test'])
    def cmd_test(msg):
        if not is_admin(msg.from_user.id):
            return _deny_msg(msg)
        clear_state(msg.chat.id)
        ok, count = _send_test_message()
        if ok:
            bot.send_message(msg.chat.id,
                f"✅ <b>Test message sent!</b>\n\nDelivered to <b>{count}</b> chat ID(s) successfully.",
                reply_markup=back_kb("main"))
        else:
            bot.send_message(msg.chat.id,
                "🔴 <b>Test message failed.</b>\n\nCheck your bot token and chat IDs in Settings.",
                reply_markup=back_kb("main"))

    # ── Inline button callbacks ───────────────────────────────────────────────

    @bot.callback_query_handler(func=lambda c: c.data.startswith('menu:'))
    def cb_menu(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        _ack(call)
        action = call.data.split(':')[1]
        cid = call.message.chat.id
        mid = call.message.message_id
        clear_state(cid)

        if action == 'apis':
            cfg = load_config()
            apis = cfg.get('apis', [])
            st = load_status()
            log.info(f"[cb_menu:apis] {len(apis)} API(s) found for user {call.from_user.id}")
            text = build_api_list_text(apis, st)
            kb   = api_list_kb(apis, st)
            try:
                bot.edit_message_text(text, cid, mid, parse_mode='HTML', reply_markup=kb)
            except Exception as e:
                log.warning(f"[cb_menu:apis] edit failed ({e}), sending new message")
                try:
                    bot.send_message(cid, text, parse_mode='HTML', reply_markup=kb)
                except Exception as e2:
                    log.error(f"[cb_menu:apis] send_message also failed: {e2}")

        elif action == 'add':
            _start_add_flow(bot, cid)

        elif action == 'stats':
            try:
                bot.edit_message_text(build_stats_text(), cid, mid, reply_markup=back_kb("main"))
            except Exception:
                bot.send_message(cid, build_stats_text(), reply_markup=back_kb("main"))

        elif action == 'recent':
            try:
                bot.edit_message_text(build_recent_text(), cid, mid, reply_markup=back_kb("main"))
            except Exception:
                bot.send_message(cid, build_recent_text(), reply_markup=back_kb("main"))

        elif action == 'settings':
            cfg = load_config()
            stt = cfg.get('settings', {})
            text = (
                "⚙️ <b>Settings</b>\n\n"
                f"🤖 Bot Token: <code>{cfg['telegram']['bot_token'][:20]}...</code>\n"
                f"💬 OTP Chat IDs:\n" +
                "\n".join(f"  • <code>{cid}</code>" for cid in (cfg['telegram'].get('chat_ids') or [cfg['telegram'].get('chat_id', 'Not set')])) +
                f"\n⏱ Global Poll Interval: <b>{stt.get('polling_interval', 1)}s</b> (default for all panels)\n"
                f"🤖 Bot Link: <code>{stt.get('bot_link_url', 'Not set')}</code>\n"
                f"📢 Channel: <code>{stt.get('channel_link_url', 'Not set')}</code>\n\n"
                "Tap a button to change a setting:"
            )
            try:
                bot.edit_message_text(text, cid, mid, reply_markup=settings_kb())
            except Exception:
                bot.send_message(cid, text, reply_markup=settings_kb())

        elif action == 'status':
            text = build_api_status_text()
            kb = status_kb()
            try:
                bot.edit_message_text(text, cid, mid, reply_markup=kb)
            except Exception:
                bot.send_message(cid, text, reply_markup=kb)

        elif action == 'test':
            ok, count = _send_test_message()
            if ok:
                bot.send_message(cid,
                    f"✅ <b>Test message sent!</b>\n\nDelivered to <b>{count}</b> chat ID(s) successfully.",
                    reply_markup=back_kb("main"))
            else:
                bot.send_message(cid,
                    "🔴 <b>Test message failed.</b>\n\nCheck your bot token and chat IDs in Settings.",
                    reply_markup=back_kb("main"))

    @bot.callback_query_handler(func=lambda c: c.data.startswith('back:'))
    def cb_back(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        _ack(call)
        target = call.data.split(':')[1]
        cid = call.message.chat.id
        mid = call.message.message_id
        clear_state(cid)
        log.info(f"[cb_back:{target}] user {call.from_user.id}")
        if target == 'main':
            text = build_main_text()
            kb   = main_menu_kb()
            try:
                bot.edit_message_text(text, cid, mid, parse_mode='HTML', reply_markup=kb)
            except Exception as e:
                log.warning(f"[cb_back:main] edit failed ({e}), sending new message")
                try:
                    bot.send_message(cid, text, parse_mode='HTML', reply_markup=kb)
                except Exception as e2:
                    log.error(f"[cb_back:main] send also failed: {e2}")
        elif target == 'apis':
            cfg  = load_config()
            apis = cfg.get('apis', [])
            st   = load_status()
            text = build_api_list_text(apis, st)
            kb   = api_list_kb(apis, st)
            try:
                bot.edit_message_text(text, cid, mid, parse_mode='HTML', reply_markup=kb)
            except Exception as e:
                log.warning(f"[cb_back:apis] edit failed ({e}), sending new message")
                try:
                    bot.send_message(cid, text, parse_mode='HTML', reply_markup=kb)
                except Exception as e2:
                    log.error(f"[cb_back:apis] send also failed: {e2}")

    @bot.callback_query_handler(func=lambda c: c.data.startswith('api:'))
    def cb_api(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        parts = call.data.split(':')
        action = parts[1]
        api_id = parts[2] if len(parts) > 2 else None
        cid = call.message.chat.id
        mid = call.message.message_id
        cfg = load_config()

        _ack(call)

        if action == 'info':
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if not api:
                return
            st = load_status().get(api_id, {})
            status_str = "🟢 Active" if api.get('enabled') and st.get('status') == 'active' else \
                         "🔴 Error" if st.get('status') == 'error' else \
                         "⚪ Disabled"
            sesskey_val = api.get('sesskey', '')
            sesskey_line = f"🔑 Sesskey: <code>{_html.escape(sesskey_val)}</code>\n" if sesskey_val else ""
            cfg_global = load_config()
            global_interval = cfg_global.get('settings', {}).get('polling_interval', 1)
            panel_interval = api.get('polling_interval')
            if panel_interval:
                interval_line = f"⏱ Interval: <b>{panel_interval}s</b> (panel override)\n"
            else:
                interval_line = f"⏱ Interval: <b>{global_interval}s</b> (global default)\n"
            text = (
                f"📡 <b>{_html.escape(api['name'])}</b>\n\n"
                f"Status: {status_str}\n"
                f"OTPs sent: <b>{st.get('otps_sent', 0)}</b>\n"
                f"{interval_line}"
                f"URL: <code>{_html.escape(api['url'])}</code>\n"
                f"PHPSESSID: <code>{_html.escape(api.get('cookies', {}).get('PHPSESSID', 'N/A'))}</code>\n"
                f"{sesskey_line}"
                f"Referer: <code>{_html.escape(api.get('headers', {}).get('Referer', 'N/A'))}</code>\n"
                f"Enabled: {'✅ Yes' if api.get('enabled') else '❌ No'}"
            )
            enabled = api.get('enabled', False)
            toggle = "⏸ Disable" if enabled else "▶️ Enable"
            _bs = _get_btn_styles()
            toggle_style = _bs.get('disable') or None if enabled else _bs.get('enable') or None
            kb = _jkb([
                [
                    _btn(toggle,      cb=f"api:toggle:{api_id}", style=toggle_style),
                    _btn("✏️ Edit",   cb=f"api:edit:{api_id}"),
                    _btn("🗑 Delete", cb=f"api:delete:{api_id}", style=_bs.get('delete') or None),
                ],
                [_btn("🔙 Back", cb="menu:apis")],
            ])
            try:
                bot.edit_message_text(text, cid, mid, reply_markup=kb)
            except Exception:
                bot.send_message(cid, text, reply_markup=kb)

        elif action == 'toggle':
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if api:
                api['enabled'] = not api.get('enabled', True)
                save_config(cfg)
                state = "enabled ✅" if api['enabled'] else "disabled ⏸"
                _ack(call, f"{api['name']} {state}")
            cfg2 = load_config()
            st = load_status()
            try:
                bot.edit_message_text(build_api_list_text(cfg2.get('apis', []), st), cid, mid,
                                      reply_markup=api_list_kb(cfg2.get('apis', []), st))
            except Exception:
                bot.send_message(cid, build_api_list_text(cfg2.get('apis', []), st),
                                 reply_markup=api_list_kb(cfg2.get('apis', []), st))

        elif action == 'edit':
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if not api:
                return
            text = f"✏️ <b>Edit: {_html.escape(api['name'])}</b>\n\nWhat would you like to change?"
            try:
                bot.edit_message_text(text, cid, mid, reply_markup=edit_api_kb(api_id))
            except Exception:
                bot.send_message(cid, text, reply_markup=edit_api_kb(api_id))

        elif action == 'delete':
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            name = _html.escape(api['name']) if api else api_id
            text = f"🗑 <b>Delete \"{name}\"?</b>\n\nThis cannot be undone."
            try:
                bot.edit_message_text(text, cid, mid, reply_markup=confirm_delete_kb(api_id))
            except Exception:
                bot.send_message(cid, text, reply_markup=confirm_delete_kb(api_id))

        elif action == 'confirm_delete':
            cfg['apis'] = [a for a in cfg.get('apis', []) if a['id'] != api_id]
            save_config(cfg)
            _ack(call, "API deleted ✅")
            cfg2 = load_config()
            st = load_status()
            try:
                bot.edit_message_text(build_api_list_text(cfg2.get('apis', []), st), cid, mid,
                                      reply_markup=api_list_kb(cfg2.get('apis', []), st))
            except Exception:
                bot.send_message(cid, build_api_list_text(cfg2.get('apis', []), st),
                                 reply_markup=api_list_kb(cfg2.get('apis', []), st))

        elif action == 'relogin':
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if not api:
                return
            _ack(call)
            stored_user = api.get('username', '').strip()
            stored_pass = api.get('password', '').strip()
            if stored_user and stored_pass:
                # Credentials already saved — trigger auto-login immediately
                wait_msg = bot.send_message(cid,
                    f"🔄 <b>Re-Login: {_html.escape(api['name'])}</b>\n\n"
                    "⏳ Logging in automatically with saved credentials…",
                    parse_mode='HTML'
                )
                from urllib.parse import urlparse as _up
                _base = api.get('base_url', '')
                if not _base:
                    _ref = api.get('headers', {}).get('Referer', '')
                    _p = _up(_ref)
                    _base = f"{_p.scheme}://{_p.netloc}{_p.path.split('/agent')[0]}"
                def _do_immediate_relogin(api_id=api_id, api_name=api['name'],
                                          base_url=_base,
                                          username=stored_user, password=stored_pass,
                                          cid=cid, mid=wait_msg.message_id):
                    lines = []

                    def progress(msg):
                        lines.append(msg)
                        preview = "\n".join(f"  {l}" for l in lines[-6:])
                        try:
                            bot.edit_message_text(
                                f"🔄 <b>Re-Login: {_html.escape(api_name)}</b>\n\n"
                                f"<code>{preview}</code>",
                                cid, mid, parse_mode='HTML'
                            )
                        except Exception:
                            pass

                    try:
                        from panel_login import auto_login_panel
                        result = auto_login_panel(
                            base_url=base_url,
                            username=username,
                            password=password,
                            progress_cb=progress,
                        )
                        cfg2 = load_config()
                        api2 = next((a for a in cfg2.get('apis', []) if a['id'] == api_id), None)
                        if api2:
                            api2.setdefault('cookies', {})['PHPSESSID'] = result['phpsessid']
                            if result.get('sesskey'):
                                api2['sesskey'] = result['sesskey']
                            save_config(cfg2)
                        log.info(f'Re-login updated credentials for API: {api_name} ({api_id})')
                        sesskey_line = (
                            f"🔑 Sesskey: <code>{_html.escape(result['sesskey'])}</code>\n"
                            if result.get('sesskey') else ""
                        )
                        bot.edit_message_text(
                            f"✅ <b>Re-Login Successful!</b>\n\n"
                            f"API: <b>{_html.escape(api_name)}</b>\n"
                            f"🍪 New PHPSESSID: <code>{_html.escape(result['phpsessid'])}</code>\n"
                            f"{sesskey_line}"
                            "Polling will resume automatically.",
                            cid, mid, parse_mode='HTML',
                            reply_markup=_jkb([[_btn("✏️ Edit API", cb=f"api:edit:{api_id}"),
                                               _btn("🔙 Back", cb="menu:apis")]])
                        )
                    except Exception as e:
                        bot.edit_message_text(
                            f"❌ <b>Re-Login Failed</b>\n\n"
                            f"API: <b>{_html.escape(api_name)}</b>\n"
                            f"Error: {_html.escape(str(e)[:300])}",
                            cid, mid, parse_mode='HTML',
                            reply_markup=_jkb([[_btn("🔄 Try Again", cb=f"api:relogin:{api_id}"),
                                               _btn("🔙 Back", cb="menu:apis")]])
                        )
                threading.Thread(target=_do_immediate_relogin, daemon=True).start()
            else:
                # No stored credentials — ask for them
                set_state(cid, 'relogin_username', {'api_id': api_id})
                bot.send_message(cid,
                    f"🔄 <b>Re-Login: {_html.escape(api['name'])}</b>\n\n"
                    "👤 Send your <b>panel username</b>:",
                    parse_mode='HTML', reply_markup=_cancel_kb()
                )

        elif action == 'test':
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if not api:
                return
            bot.send_message(cid, f"🔄 Testing connection to <b>{_html.escape(api['name'])}</b>...", parse_mode='HTML')
            ok, status_code, detail = _test_api_connection(api)
            back_kb_test = _jkb([[_btn("✏️ Edit API", cb=f"api:edit:{api_id}"), _btn("🔙 Back", cb="menu:apis")]])
            bot.send_message(cid,
                f"{'✅' if ok else '❌'} <b>Connection Test: {_html.escape(api['name'])}</b>\n\n"
                f"{detail}\n\n"
                f"{'HTTP ' + str(status_code) if status_code else ''}\n"
                f"🔗 URL: <code>{_html.escape(api['url'])}</code>\n"
                f"🍪 PHPSESSID: <code>{_html.escape(api.get('cookies', {}).get('PHPSESSID', 'N/A'))}</code>",
                parse_mode='HTML',
                reply_markup=back_kb_test
            )

        elif action == 'testsms':
            _ack(call)
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if not api:
                return
            wait_msg = bot.send_message(cid,
                f"🧪 Fetching last SMS from <b>{_html.escape(api['name'])}</b>…",
                parse_mode='HTML')
            def _do_test_sms():
                try:
                    from otp_bot import fetch_latest_otp, extract_otp_code, detect_service, send_telegram_message, _col
                    sess = requests.Session()
                    sess.verify = False
                    sess.headers.update(api.get('headers', {}))
                    sess.cookies.update(api.get('cookies', {}))
                    data = fetch_latest_otp(api, sess)
                    rows = data.get('aaData', [])
                    if not rows:
                        bot.edit_message_text(
                            "⚠️ <b>No SMS records found in the panel.</b>\n\nNothing to forward.",
                            cid, wait_msg.message_id, parse_mode='HTML',
                            reply_markup=_jkb([[_btn("✏️ Edit API", cb=f"api:edit:{api_id}")]]))
                        return
                    latest = rows[0]
                    number   = _col(latest, 2) or 'Unknown'
                    svc_raw  = _col(latest, 3) or 'Unknown'
                    raw_text = _col(latest, 5) or ''
                    otp_code = extract_otp_code(raw_text) or '——'
                    svc_key  = detect_service(svc_raw, raw_text)
                    ok = send_telegram_message(cfg, number, svc_raw, raw_text, otp_code, api['name'])
                    status_line = "✅ Forwarded to group." if ok else "❌ Failed to send to group."
                    bot.edit_message_text(
                        f"🧪 <b>Test SMS — {_html.escape(api['name'])}</b>\n\n"
                        f"📱 Number: <code>{_html.escape(str(number))}</code>\n"
                        f"🔖 Service: <b>{_html.escape(svc_key)}</b>\n"
                        f"🔑 OTP: <code>{_html.escape(str(otp_code))}</code>\n"
                        f"💬 Message: <i>{_html.escape(raw_text[:200])}</i>\n\n"
                        f"{status_line}",
                        cid, wait_msg.message_id, parse_mode='HTML',
                        reply_markup=_jkb([[_btn("✏️ Edit API", cb=f"api:edit:{api_id}"),
                                            _btn("🔙 Back", cb="menu:apis")]]))
                except Exception as e:
                    bot.edit_message_text(
                        f"❌ <b>Test SMS failed</b>\n\n{_html.escape(str(e))}",
                        cid, wait_msg.message_id, parse_mode='HTML',
                        reply_markup=_jkb([[_btn("✏️ Edit API", cb=f"api:edit:{api_id}"),
                                            _btn("🔙 Back", cb="menu:apis")]]))
            threading.Thread(target=_do_test_sms, daemon=True).start()

    # ── Edit API field callbacks ──────────────────────────────────────────────

    @bot.callback_query_handler(func=lambda c: c.data.startswith('edit:'))
    def cb_edit(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        parts = call.data.split(':')
        field = parts[1]
        api_id = parts[2]
        cid = call.message.chat.id
        prompts = {
            'name':     "✏️ Send the <b>new name</b> for this API:",
            'url':      "🔗 Send the <b>new data endpoint URL</b>:\n\n<i>e.g. http://51.68.180.239/ints/agent/res/data_smscdr.php</i>",
            'referer':  "🌐 Send the <b>new Referer URL</b>:\n\n<i>e.g. http://51.68.180.239/ints/agent/SMSCDRReports</i>",
            'sessid':   "🍪 Send the <b>new PHPSESSID</b> cookie value:\n\n<i>Copy from browser DevTools → Application → Cookies</i>",
            'sesskey':  "🔑 Send the <b>new Sesskey</b> value:\n\n<i>Copy from the request URL — the <code>sesskey=...</code> parameter</i>",
            'interval': "⏱ Send the <b>polling interval</b> in seconds for this panel:\n\n<i>e.g. <code>0.5</code> = very fast, <code>1</code> = default, <code>5</code> = relaxed\nSend <code>0</code> to use the global default.</i>",
        }
        _ack(call)
        if field == 'credentials':
            set_state(cid, 'edit_api_credentials_username', {'api_id': api_id})
            bot.send_message(cid, "👤 <b>Step 1/2 — Username</b>\n\nSend the <b>new username</b>:",
                             parse_mode='HTML', reply_markup=_jkb([[_btn("❌ Cancel", cb="cancel")]]))
        else:
            set_state(cid, f'edit_api_{field}', {'api_id': api_id})
            bot.send_message(cid, prompts.get(field, "Send new value:"),
                             reply_markup=_jkb([[_btn("❌ Cancel", cb="cancel")]]))

    # ── Settings callbacks ────────────────────────────────────────────────────

    @bot.callback_query_handler(func=lambda c: c.data.startswith('set:'))
    def cb_settings(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        _ack(call)
        action = call.data.split(':')[1]
        cid = call.message.chat.id
        mid = call.message.message_id

        if action == 'chatid':
            cfg = load_config()
            chat_ids = cfg['telegram'].get('chat_ids') or []
            try:
                bot.edit_message_text(chatids_menu_text(chat_ids), cid, mid,
                                      reply_markup=chatids_menu_kb(chat_ids))
            except Exception:
                bot.send_message(cid, chatids_menu_text(chat_ids),
                                 reply_markup=chatids_menu_kb(chat_ids))
            return

        if action == 'btnstyle':
            styles = _get_btn_styles()
            try:
                bot.edit_message_text(btnstyle_overview_text(styles), cid, mid,
                                      reply_markup=btnstyle_overview_kb(styles))
            except Exception:
                bot.send_message(cid, btnstyle_overview_text(styles),
                                 reply_markup=btnstyle_overview_kb(styles))
            return

        if action == 'links':
            cfg = load_config()
            stt = cfg.get('settings', {})
            set_state(cid, 'settings_links_bot')
            bot.send_message(cid,
                "🔗 <b>Step 1/2 — Bot Link</b>\n\n"
                f"Current: <code>{stt.get('bot_link_url', 'Not set')}</code>\n\n"
                "Send the new <b>bot link URL</b> (e.g. <code>https://t.me/YourBot</code>):",
                parse_mode='HTML',
                reply_markup=_jkb([[_btn("❌ Cancel", cb="cancel")]]))
            return

        prompts = {
            'token':    "🤖 Send the <b>new Telegram Bot Token</b>:",
            'interval': "⏱ Send the <b>global default polling interval</b> in seconds:\n\n<i>e.g. <code>0.5</code> = very fast, <code>5</code> = relaxed. Default: <code>1</code>\nPanels with their own interval override this value.</i>",
        }
        set_state(cid, f'settings_{action}')
        bot.send_message(cid, prompts[action],
                         reply_markup=_jkb([[_btn("❌ Cancel", cb="cancel")]]))

    # ── Button style picker callbacks ─────────────────────────────────────────

    @bot.callback_query_handler(func=lambda c: c.data.startswith('btnstyle:'))
    def cb_btnstyle(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        _ack(call)
        parts = call.data.split(':')
        action = parts[1]
        cid = call.message.chat.id
        mid = call.message.message_id

        if action == 'pick':
            btn_key = parts[2]
            name = _BTN_NAME.get(btn_key, btn_key)
            styles = _get_btn_styles()
            current = _STYLE_LABEL.get(styles.get(btn_key, ''), '⚪ Default')
            try:
                bot.edit_message_text(
                    f"🎨 <b>Choose color for {name}</b>\n\nCurrent: <b>{current}</b>\n\nSelect the new button color:",
                    cid, mid, reply_markup=btnstyle_pick_kb(btn_key))
            except Exception:
                bot.send_message(cid,
                    f"🎨 <b>Choose color for {name}</b>\n\nCurrent: <b>{current}</b>\n\nSelect the new button color:",
                    reply_markup=btnstyle_pick_kb(btn_key))

        elif action == 'set':
            btn_key = parts[2]
            new_style = parts[3] if len(parts) > 3 else ''
            cfg = load_config()
            cfg.setdefault('button_styles', {})
            cfg['button_styles'][btn_key] = new_style
            save_config(cfg)
            styles = _get_btn_styles()
            label = _STYLE_LABEL.get(new_style, '⚪ Default')
            name = _BTN_NAME.get(btn_key, btn_key)
            _ack(call, f"✅ {name} → {label}")
            try:
                bot.edit_message_text(btnstyle_overview_text(styles), cid, mid,
                                      reply_markup=btnstyle_overview_kb(styles))
            except Exception:
                bot.send_message(cid, btnstyle_overview_text(styles),
                                 reply_markup=btnstyle_overview_kb(styles))

    # ── Chat ID manage callbacks ──────────────────────────────────────────────

    @bot.callback_query_handler(func=lambda c: c.data.startswith('chatid:'))
    def cb_chatid(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        _ack(call)
        parts = call.data.split(':')
        action = parts[1]
        cid = call.message.chat.id
        mid = call.message.message_id
        cfg = load_config()
        chat_ids = cfg['telegram'].get('chat_ids') or []

        if action == 'del':
            idx = int(parts[2])
            if 0 <= idx < len(chat_ids):
                removed = chat_ids.pop(idx)
                cfg['telegram']['chat_ids'] = chat_ids
                cfg['telegram'].pop('chat_id', None)
                save_config(cfg)
                _ack(call, f"🗑 Removed {removed}")
            else:
                _ack(call, "ID not found.")
            try:
                bot.edit_message_text(chatids_menu_text(chat_ids), cid, mid,
                                      reply_markup=chatids_menu_kb(chat_ids))
            except Exception:
                bot.send_message(cid, chatids_menu_text(chat_ids),
                                 reply_markup=chatids_menu_kb(chat_ids))

        elif action == 'add':
            set_state(cid, 'settings_chatid_add')
            bot.send_message(cid,
                "➕ <b>Add Chat ID(s)</b>\n\n"
                "<i>Send one or more IDs (one per line or comma-separated).\n"
                "They will be added to the existing list.</i>",
                reply_markup=_jkb([[_btn("❌ Cancel", cb="cancel")]])
            )

        elif action == 'copy':
            idx = int(parts[2])
            if 0 <= idx < len(chat_ids):
                _ack(call, f"ID: {chat_ids[idx]}", alert=True)
            else:
                _ack(call)

    # ── Text message handler (state machine) ──────────────────────────────────

    @bot.message_handler(func=lambda m: True, content_types=['text'])
    def handle_text(msg):
        if not is_admin(msg.from_user.id):
            return
        cid = msg.chat.id
        text = msg.text.strip()
        state = get_state(cid)

        if not state:
            bot.send_message(cid, "Use /menu to open the admin panel.", reply_markup=main_menu_kb())
            return

        step = state['step']
        data = state['data']

        # ── Add API flow (auto-login) ─────────────────────────────────────────

        if step == 'add_url':
            data['base_url'] = text.strip()
            set_state(cid, 'add_username', data)
            bot.send_message(cid,
                "👤 <b>Step 2/3 — Username</b>\n\n"
                "Send your <b>panel username</b>:",
                parse_mode='HTML',
                reply_markup=_cancel_kb()
            )

        elif step == 'add_username':
            data['username'] = text.strip()
            set_state(cid, 'add_password', data)
            bot.send_message(cid,
                "🔒 <b>Step 3/3 — Password</b>\n\n"
                "Send your <b>panel password</b>:",
                parse_mode='HTML',
                reply_markup=_cancel_kb()
            )

        elif step == 'add_password':
            data['password'] = text.strip()
            clear_state(cid)
            _start_auto_login(bot, cid, data)

        # ── Re-login flow ────────────────────────────────────────────────────

        elif step == 'relogin_username':
            data['username'] = text.strip()
            set_state(cid, 'relogin_password', data)
            bot.send_message(cid,
                "🔒 Send your <b>panel password</b>:",
                parse_mode='HTML', reply_markup=_cancel_kb()
            )

        elif step == 'relogin_password':
            data['password'] = text.strip()
            clear_state(cid)
            _start_relogin(bot, cid, data)

        # ── Edit credentials (username + password) two-step flow ─────────────

        elif step == 'edit_api_credentials_username':
            data['new_username'] = text.strip()
            set_state(cid, 'edit_api_credentials_password', data)
            bot.send_message(cid,
                "🔒 <b>Step 2/2 — Password</b>\n\nSend the <b>new password</b>:",
                parse_mode='HTML', reply_markup=_cancel_kb()
            )

        elif step == 'edit_api_credentials_password':
            api_id = data.get('api_id')
            cfg = load_config()
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if api:
                api['username'] = data.get('new_username', '')
                api['password'] = text.strip()
                save_config(cfg)
                clear_state(cid)
                bot.send_message(cid, "✅ Username & password updated!", reply_markup=edit_api_kb(api_id))
            else:
                clear_state(cid)
                bot.send_message(cid, "❌ API not found.", reply_markup=main_menu_kb())

        # ── Edit API field flow ───────────────────────────────────────────────

        elif step.startswith('edit_api_'):
            field = step.replace('edit_api_', '')
            api_id = data.get('api_id')
            cfg = load_config()
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if api:
                if field == 'name':
                    api['name'] = text.strip()
                elif field == 'url':
                    api['url'] = text.strip()
                elif field == 'referer':
                    api.setdefault('headers', {})['Referer'] = text.strip()
                elif field == 'sessid':
                    api.setdefault('cookies', {})['PHPSESSID'] = text.strip()
                elif field == 'sesskey':
                    api['sesskey'] = text.strip()
                elif field == 'username':
                    api['username'] = text.strip()
                elif field == 'password':
                    api['password'] = text.strip()
                elif field == 'interval':
                    try:
                        val = float(text.strip())
                        if val <= 0:
                            # 0 or negative = remove override, use global default
                            api.pop('polling_interval', None)
                            cfg_global = load_config()['settings'].get('polling_interval', 1)
                            save_config(cfg)
                            clear_state(cid)
                            bot.send_message(
                                cid,
                                f"✅ Polling interval reset to global default (<b>{cfg_global}s</b>).",
                                parse_mode='HTML', reply_markup=edit_api_kb(api_id))
                            return
                        else:
                            api['polling_interval'] = val
                    except ValueError:
                        bot.send_message(cid,
                            "❌ Please send a number, e.g. <code>1</code> or <code>0.5</code>",
                            parse_mode='HTML')
                        return
                save_config(cfg)
                clear_state(cid)
                if field == 'interval':
                    val = api.get('polling_interval', 1)
                    bot.send_message(
                        cid,
                        f"✅ Polling interval set to <b>{val}s</b> for this panel.",
                        parse_mode='HTML', reply_markup=edit_api_kb(api_id))
                else:
                    bot.send_message(cid, f"✅ Updated successfully!", reply_markup=edit_api_kb(api_id))
            else:
                clear_state(cid)
                bot.send_message(cid, "❌ API not found.", reply_markup=main_menu_kb())

        # ── Settings flow ─────────────────────────────────────────────────────

        elif step == 'settings_token':
            cfg = load_config()
            cfg['telegram']['bot_token'] = text
            save_config(cfg)
            clear_state(cid)
            bot.send_message(cid,
                "✅ <b>Bot token updated!</b>\n\n"
                "⚠️ Restart the bot for the new token to take effect.",
                reply_markup=settings_kb()
            )

        elif step == 'settings_chatid_add':
            cfg = load_config()
            existing = cfg['telegram'].get('chat_ids') or []
            new_ids = [i.strip() for i in text.replace(',', '\n').splitlines() if i.strip()]
            merged = existing + [i for i in new_ids if i not in existing]
            cfg['telegram']['chat_ids'] = merged
            cfg['telegram'].pop('chat_id', None)
            save_config(cfg)
            clear_state(cid)
            bot.send_message(cid,
                chatids_menu_text(merged),
                reply_markup=chatids_menu_kb(merged)
            )

        elif step == 'settings_interval':
            try:
                val = float(text)
                cfg = load_config()
                cfg['settings']['polling_interval'] = val
                save_config(cfg)
                clear_state(cid)
                bot.send_message(cid, f"✅ <b>Polling interval set to {val}s</b>", reply_markup=settings_kb())
            except ValueError:
                bot.send_message(cid, "❌ Please send a number, e.g. <code>1</code> or <code>0.5</code>")

        elif step == 'settings_links_bot':
            cfg = load_config()
            stt = cfg.get('settings', {})
            set_state(cid, 'settings_links_channel', {'bot_link_url': text.strip()})
            bot.send_message(cid,
                "📢 <b>Step 2/2 — Channel Link</b>\n\n"
                f"Current: <code>{stt.get('channel_link_url', 'Not set')}</code>\n\n"
                "Send the new <b>channel link URL</b> (e.g. <code>https://t.me/YourChannel</code>):",
                parse_mode='HTML', reply_markup=_cancel_kb()
            )

        elif step == 'settings_links_channel':
            cfg = load_config()
            cfg.setdefault('settings', {})['bot_link_url'] = data.get('bot_link_url', '')
            cfg['settings']['channel_link_url'] = text.strip()
            save_config(cfg)
            clear_state(cid)
            bot.send_message(cid,
                f"✅ <b>Links updated!</b>\n\n"
                f"🤖 Bot: <code>{cfg['settings']['bot_link_url']}</code>\n"
                f"📢 Channel: <code>{cfg['settings']['channel_link_url']}</code>",
                parse_mode='HTML', reply_markup=settings_kb()
            )

        else:
            clear_state(cid)
            bot.send_message(cid, "Use /menu to start.", reply_markup=main_menu_kb())

    # ── Universal cancel — deletes the prompt message ─────────────────────────

    @bot.callback_query_handler(func=lambda c: c.data == "cancel")
    def cb_cancel(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        _ack(call)
        cid = call.message.chat.id
        mid = call.message.message_id
        clear_state(cid)
        try:
            bot.delete_message(cid, mid)
        except Exception:
            pass

    # ── Add-flow callback shortcuts ───────────────────────────────────────────

    @bot.callback_query_handler(func=lambda c: c.data.startswith('add:'))
    def cb_add(call):
        if not is_admin(call.from_user.id):
            return _deny_cb(call)
        action = call.data.split(':')[1]
        cid = call.message.chat.id
        state = get_state(cid)

        _ack(call)

    return bot


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _cancel_kb():
    s = _get_btn_styles()
    return _jkb([[_btn("❌ Cancel", cb="cancel", style=s.get('cancel') or None)]])


def _send_test_message():
    cfg = load_config()
    token = cfg['telegram']['bot_token']
    chat_ids = cfg['telegram'].get('chat_ids') or [cfg['telegram'].get('chat_id')]
    chat_ids = [cid for cid in chat_ids if cid]

    text = (
        "🟢 🇺🇸 <b>USA</b> | 📱 WA | <code>+1234••6789</code>  ⏰ 12:00\n\n"
        "🔔 <b>Test OTP Message</b>\n\n"
        "This is a test message sent from your OTP Bot admin panel.\n"
        "If you see this, your bot is configured correctly! ✅\n\n"
        "🔑 Sample OTP: <b>123456</b>"
    )
    styles = _get_btn_styles()
    def _s(key):
        v = styles.get(key, '')
        return {'style': v} if v else {}
    inline_keyboard = {
        'inline_keyboard': [
            [{**{'text': '• • • • • •', 'copy_text': {'text': '123456'}}, **_s('copy')}],
            [
                {**{'text': '🤖 Bot Link', 'url': load_config().get('settings', {}).get('bot_link_url', 'https://t.me/YourBot')}, **_s('bot_link')},
                {**{'text': '📢 Channel',  'url': load_config().get('settings', {}).get('channel_link_url', 'https://t.me/YourChannel')}, **_s('channel')},
            ],
        ]
    }

    url = f'https://api.telegram.org/bot{token}/sendMessage'
    success_count = 0
    for cid in chat_ids:
        try:
            r = requests.post(url, data={
                'chat_id': cid,
                'text': text,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True,
                'reply_markup': json.dumps(inline_keyboard),
            }, timeout=10)
            if r.ok:
                success_count += 1
        except Exception:
            pass
    return success_count > 0, success_count


def _test_api_connection(api_cfg):
    """Make a live test request to an API. Returns (ok, http_status, detail_str)."""
    import requests as _req
    import urllib3
    urllib3.disable_warnings()
    today = datetime.now().strftime('%Y-%m-%d')
    ts = str(int(time.time() * 1000))
    params = {
        'fdate1': f'{today} 00:00:00',
        'fdate2': f'{today} 23:59:59',
        'fg': '0', 'sEcho': '1', 'iColumns': '9',
        'sColumns': ',,,,,,,,',
        'iDisplayStart': '0', 'iDisplayLength': '5', '_': ts,
    }
    try:
        sesskey = api_cfg.get('sesskey', '')
        if sesskey:
            params['sesskey'] = sesskey
        sess = _req.Session()
        phpsessid = api_cfg.get('cookies', {}).get('PHPSESSID', '')
        if phpsessid:
            sess.cookies.set('PHPSESSID', phpsessid)
        sess.headers.update(api_cfg.get('headers', {}))
        sess.verify = False
        r = sess.get(api_cfg['url'], params=params, timeout=12)
        code = r.status_code
        if r.ok:
            try:
                data = r.json()
                records = len(data.get('aaData', []))
                total = data.get('iTotalRecords', '?')
                return True, code, (
                    f"✅ <b>Connection successful!</b>\n"
                    f"Records returned: <b>{records}</b>\n"
                    f"Total records today: <b>{total}</b>\n"
                    f"PHPSESSID is <b>valid ✅</b>"
                )
            except Exception:
                snippet = _html.escape(r.text[:200])
                return True, code, f"✅ HTTP {code} OK — Response (not JSON):\n<code>{snippet}</code>"
        elif code == 302 or code == 301:
            return False, code, (
                "⚠️ <b>Session expired / Login redirect.</b>\n"
                "Your PHPSESSID is invalid or expired.\n"
                "Please update it from Edit API → Edit PHPSESSID."
            )
        elif code == 403:
            return False, code, (
                "🔒 <b>Access forbidden (403).</b>\n"
                "PHPSESSID may be wrong or expired."
            )
        elif code == 503:
            retry_after = r.headers.get('Retry-After', '')
            if retry_after:
                try:
                    wait_mins = int(retry_after) // 60
                    wait_secs = int(retry_after) % 60
                    wait_str = f"{wait_mins}m {wait_secs}s" if wait_mins else f"{wait_secs}s"
                except Exception:
                    wait_str = f"{retry_after}s"
                return False, code, (
                    f"⚡ <b>Rate limited by server (503).</b>\n"
                    f"The SMS panel is throttling too-frequent requests.\n"
                    f"Server asks to retry after: <b>{wait_str}</b>\n\n"
                    f"The bot now automatically respects this wait time.\n"
                    f"If this persists, you can increase the polling interval via Settings."
                )
            return False, code, (
                "🔴 <b>Server unavailable (503).</b>\n"
                "The SMS panel server is temporarily down.\n"
                "This is an external issue — try again later."
            )
        else:
            return False, code, f"❌ <b>HTTP {code}: {_html.escape(r.reason or 'Error')}</b>"
    except _req.exceptions.ConnectionError:
        return False, 0, (
            "🔴 <b>Connection refused.</b>\n"
            "The server is unreachable — check the IP/URL."
        )
    except _req.exceptions.Timeout:
        return False, 0, (
            "⏱ <b>Request timed out (12s).</b>\n"
            "Server is not responding."
        )
    except Exception as e:
        return False, 0, f"❌ <b>Error:</b> {_html.escape(str(e)[:150])}"


def _start_add_flow(bot, cid):
    set_state(cid, 'add_url', {})
    bot.send_message(cid,
        "➕ <b>Add New API — Step 1/3</b>\n\n"
        "Send the <b>panel base URL</b>:\n\n"
        "Example:\n"
        "<code>http://51.68.180.239/ints</code>",
        parse_mode='HTML',
        reply_markup=_cancel_kb()
    )


def _start_auto_login(bot, cid, data):
    """Kick off the Selenium login in a background thread, sending live progress."""
    status_msg = bot.send_message(
        cid,
        "🔐 <b>Logging into the panel…</b>\n\n"
        "<i>Starting virtual display and browser — this takes 30–60 seconds.</i>",
        parse_mode='HTML'
    )

    def _run():
        lines = []

        def progress(msg):
            lines.append(msg)
            preview = "\n".join(f"  {l}" for l in lines[-6:])
            try:
                bot.edit_message_text(
                    f"🔐 <b>Logging in…</b>\n\n<code>{preview}</code>",
                    cid, status_msg.message_id, parse_mode='HTML'
                )
            except Exception:
                pass

        try:
            from panel_login import auto_login_panel
            base_url = data.get('base_url', '').strip().rstrip('/')
            if not base_url.startswith(('http://', 'https://')):
                base_url = 'http://' + base_url
            result = auto_login_panel(
                base_url=base_url,
                username=data.get('username', ''),
                password=data.get('password', ''),
                progress_cb=progress,
            )
            result['username'] = data.get('username', '')
            result['password'] = data.get('password', '')
            _finish_auto_login(bot, cid, status_msg.message_id, result)
        except Exception as e:
            log.error(f'Auto-login failed: {e}')
            _sty = _get_btn_styles()
            kb = _jkb([[
                _btn("🔄 Try Again", cb="menu:add", style=_sty.get('add') or None),
                _btn("🏠 Main Menu", cb="back:main"),
            ]])
            try:
                bot.edit_message_text(
                    f"❌ <b>Login failed</b>\n\n{_html.escape(str(e))}",
                    cid, status_msg.message_id,
                    parse_mode='HTML', reply_markup=kb
                )
            except Exception:
                bot.send_message(cid, f"❌ <b>Login failed:</b>\n{_html.escape(str(e))}",
                                 parse_mode='HTML', reply_markup=kb)

    threading.Thread(target=_run, daemon=True).start()


def _finish_auto_login(bot, cid, edit_mid, result):
    """Store the extracted credentials and confirm to the user."""
    cfg = load_config()
    new_api = {
        'id': f'api_{uuid.uuid4().hex[:8]}',
        'name': result['name'],
        'enabled': True,
        'url': result['data_url'],
        'sesskey': result.get('sesskey', ''),
        'cookies': {'PHPSESSID': result['phpsessid']},
        'username': result.get('username', ''),
        'password': result.get('password', ''),
        'base_url': result.get('base_url', ''),
        'headers': {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': result['referer'],
            'User-Agent': (
                'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36'
            ),
            'X-Requested-With': 'XMLHttpRequest',
        },
    }
    cfg.setdefault('apis', []).append(new_api)
    save_config(cfg)
    log.info(f"Auto-login API added: {new_api['name']} ({new_api['id']})")

    sesskey_line = (
        f"🔑 Sesskey: <code>{_html.escape(result['sesskey'])}</code>\n"
        if result.get('sesskey') else
        "🔑 Sesskey: <i>not found — update via Edit API if needed</i>\n"
    )
    _sty = _get_btn_styles()
    kb = _jkb([
        [_btn("📡 View APIs", cb="menu:apis"),
         _btn("➕ Add Another", cb="menu:add", style=_sty.get('add') or None)],
        [_btn("🏠 Main Menu", cb="back:main")],
    ])
    text = (
        f"✅ <b>API \"{_html.escape(new_api['name'])}\" added!</b>\n\n"
        f"🔗 Endpoint: <code>{_html.escape(new_api['url'])}</code>\n"
        f"🌐 Referer: <code>{_html.escape(new_api['headers']['Referer'])}</code>\n"
        f"🍪 PHPSESSID: <code>{_html.escape(new_api['cookies']['PHPSESSID'])}</code>\n"
        f"{sesskey_line}\n"
        f"Bot will start polling within ~10 seconds."
    )
    try:
        bot.edit_message_text(text, cid, edit_mid, parse_mode='HTML', reply_markup=kb)
    except Exception:
        bot.send_message(cid, text, parse_mode='HTML', reply_markup=kb)


def _start_relogin(bot, cid, data):
    """Re-run auto-login for an existing API and update its stored credentials."""
    api_id = data.get('api_id')
    cfg = load_config()
    api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
    if not api:
        bot.send_message(cid, "❌ API not found.", reply_markup=main_menu_kb())
        return

    # Derive base_url from the stored referer (strip /agent/... suffix)
    referer = api.get('headers', {}).get('Referer', '')
    from urllib.parse import urlparse
    p = urlparse(referer)
    # base_url = scheme://host/ints  (everything up to /agent)
    path_parts = p.path.split('/agent')[0]
    base_url = f"{p.scheme}://{p.netloc}{path_parts}"

    status_msg = bot.send_message(
        cid,
        f"🔄 <b>Re-logging into {_html.escape(api['name'])}…</b>\n\n"
        "<i>Starting virtual display and browser — this takes 30–60 seconds.</i>",
        parse_mode='HTML'
    )

    def _run():
        lines = []

        def progress(msg):
            lines.append(msg)
            preview = "\n".join(f"  {l}" for l in lines[-6:])
            try:
                bot.edit_message_text(
                    f"🔄 <b>Re-logging in…</b>\n\n<code>{preview}</code>",
                    cid, status_msg.message_id, parse_mode='HTML'
                )
            except Exception:
                pass

        try:
            from panel_login import auto_login_panel
            result = auto_login_panel(
                base_url=base_url,
                username=data.get('username', ''),
                password=data.get('password', ''),
                progress_cb=progress,
            )
            result['username'] = data.get('username', '')
            result['password'] = data.get('password', '')
            result['base_url'] = base_url
            _finish_relogin(bot, cid, status_msg.message_id, api_id, result)
        except Exception as e:
            log.error(f'Re-login failed: {e}')
            kb = _jkb([[_btn("✏️ Edit API", cb=f"api:edit:{api_id}"),
                        _btn("🔙 Back", cb="menu:apis")]])
            try:
                bot.edit_message_text(
                    f"❌ <b>Re-login failed</b>\n\n{_html.escape(str(e))}",
                    cid, status_msg.message_id, parse_mode='HTML', reply_markup=kb
                )
            except Exception:
                bot.send_message(cid, f"❌ <b>Re-login failed:</b>\n{_html.escape(str(e))}",
                                 parse_mode='HTML', reply_markup=kb)

    threading.Thread(target=_run, daemon=True).start()


def _finish_relogin(bot, cid, edit_mid, api_id, result):
    """Update existing API's PHPSESSID and sesskey after successful re-login."""
    cfg = load_config()
    api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
    if not api:
        bot.send_message(cid, "❌ API not found after re-login.")
        return

    api.setdefault('cookies', {})['PHPSESSID'] = result['phpsessid']
    if result.get('sesskey'):
        api['sesskey'] = result['sesskey']
    if result.get('username'):
        api['username'] = result['username']
    if result.get('password'):
        api['password'] = result['password']
    if result.get('base_url'):
        api['base_url'] = result['base_url']
    save_config(cfg)
    log.info(f"Re-login updated credentials for API: {api['name']} ({api_id})")

    sesskey_line = (
        f"🔑 Sesskey: <code>{_html.escape(result['sesskey'])}</code>\n"
        if result.get('sesskey') else
        "🔑 Sesskey: <i>unchanged</i>\n"
    )
    kb = _jkb([
        [_btn("📡 View APIs", cb="menu:apis"),
         _btn("✏️ Edit API", cb=f"api:edit:{api_id}")],
        [_btn("🏠 Main Menu", cb="back:main")],
    ])
    text = (
        f"✅ <b>Session refreshed: {_html.escape(api['name'])}</b>\n\n"
        f"🍪 New PHPSESSID: <code>{_html.escape(result['phpsessid'])}</code>\n"
        f"{sesskey_line}\n"
        f"Bot will resume polling within ~10 seconds."
    )
    try:
        bot.edit_message_text(text, cid, edit_mid, parse_mode='HTML', reply_markup=kb)
    except Exception:
        bot.send_message(cid, text, parse_mode='HTML', reply_markup=kb)


# ─── Entry point ──────────────────────────────────────────────────────────────

def start_admin_bot():
    import time as _time
    log.info("Telegram admin bot starting...")
    _conflict_notified = False
    while True:
        try:
            bot = create_bot()
            _conflict_notified = False
            log.info("Admin bot polling started")
            bot.infinity_polling(timeout=20, long_polling_timeout=10, skip_pending=True)
        except Exception as e:
            err_str = str(e)
            # 409 Conflict = another bot instance is running with the same token
            if '409' in err_str or 'Conflict' in err_str:
                log.error(f"Admin bot 409 Conflict — another instance is running with this token. Waiting 30s before retry.")
                if not _conflict_notified:
                    _conflict_notified = True
                    try:
                        cfg = load_config()
                        token = cfg['telegram']['bot_token']
                        admins = cfg.get('settings', {}).get('admin_ids', [])
                        for admin_id in admins:
                            try:
                                import requests as _req
                                _req.post(
                                    f'https://api.telegram.org/bot{token}/sendMessage',
                                    json={
                                        'chat_id': admin_id,
                                        'text': (
                                            '⚠️ <b>Bot Conflict Detected</b>\n\n'
                                            'Another bot instance is running with the same token.\n'
                                            'Please stop all other running instances of this bot '
                                            'to restore admin panel functionality.'
                                        ),
                                        'parse_mode': 'HTML',
                                    },
                                    timeout=10,
                                )
                            except Exception:
                                pass
                    except Exception:
                        pass
                _time.sleep(30)
            else:
                log.error(f"Admin bot crashed: {e} — restarting in 5s")
                _time.sleep(5)
