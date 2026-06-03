"""
claude-bot — Telegram remote control for Claude Code (clone of opencode-bot).

Architecture:
- Claude Agent SDK replaces the OpenCode server + SSE.
- Each prompt runs a ClaudeSDKClient in a background asyncio.Task; its streamed
  events drive a live status message and, on completion, the final reply.
- Conversation continuity via resume=<claude_session_id> (Claude persists state
  on disk). Session discovery uses the SDK's native list_sessions().
- Single-admin model: only TELEGRAM_ADMIN_ID may use the bot.
"""

import os
import time
import shutil
import asyncio
import logging
from pathlib import Path
from collections import deque

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.error import BadRequest, RetryAfter

import claude_agent_sdk as sdk

import db
import md2tgv2
import transcription as grok_stt
import claude_client as cc

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
load_dotenv(Path(__file__).parent.parent / ".env")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("claude-bot")

TOKEN     = os.environ["TELEGRAM_BOT_TOKEN"]
ADMIN_ID  = int(os.environ["TELEGRAM_ADMIN_ID"])
WORKSPACE = Path(os.getenv("DEFAULT_WORKSPACE", "~/proyectos")).expanduser()
DEFAULT_PERMISSION_MODE = os.getenv("PERMISSION_MODE", "bypassPermissions")
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", "1800"))
BOT_DIR = str(Path(__file__).parent.parent.resolve())
TMP_DIR = Path("/tmp/claude-bot-media")
RESTART_FLAG = Path("/tmp/claude-bot-restarting.flag")

MCP_SERVER = cc.build_mcp_server()

# Module-level state (single admin → globals are fine)
APP: Application | None = None
PERMISSION_MODE = DEFAULT_PERMISSION_MODE

STATUSES: dict = {}        # skey -> status dict
RUNNING: dict = {}         # skey -> {"client", "task", "directory"}
QUEUES: dict = {}          # skey -> deque[{"text","directory","model"}]
MSG2SESS: dict = {}        # bot message_id -> {"skey","directory"}
KNOWN_SID: dict = {}       # skey -> real claude session id once known
KEYSTORE: dict = {}        # int -> str   (compress long strings for callback_data)
PENDING_PERMS: dict = {}   # qid -> asyncio.Future
PENDING_Q: dict = {}       # qid -> {"future", "options": [str]}
SEND_MODE = {"on": False, "target": None, "pending_text": None}
MKDIR_PENDING: dict = {}   # {"path","msg_id"}

STATUS_INTERVAL = 10
STATUS_THROTTLE = 3
MSG_TRACK_LIMIT = 200
MD_FILE_THRESHOLD = 6000


# --------------------------------------------------------------------------- #
# Auth + key store
# --------------------------------------------------------------------------- #
def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id != ADMIN_ID:
            return
        return await func(update, ctx)
    return wrapper


def _key(value: str) -> int:
    for k, v in KEYSTORE.items():
        if v == value:
            return k
    k = len(KEYSTORE)
    KEYSTORE[k] = value
    return k


def _val(k: int) -> str:
    return KEYSTORE.get(k, "")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _skey(directory: str, claude_session_id: str | None) -> str:
    return claude_session_id if claude_session_id else f"new::{directory}"


def _resume_for(skey: str) -> str | None:
    if skey and not skey.startswith("new::"):
        return skey
    return KNOWN_SID.get(skey)


def _session_label(s) -> str:
    sid = getattr(s, "session_id", "")
    if sid:
        meta = db.get_session_meta(sid)
        if meta and meta.get("title"):
            return meta["title"]
    return (getattr(s, "custom_title", None) or getattr(s, "summary", None)
            or getattr(s, "first_prompt", None) or sid[:8]
            or "sesión")


def _list_sessions(directory: str | None = None) -> list:
    try:
        return sdk.list_sessions(directory=directory)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"list_sessions failed: {exc}")
        return []


def _format_elapsed(seconds: float) -> str:
    return f"{int(seconds // 60):02d}:{int(seconds % 60):02d}"


async def _delete_msg(bot, msg_id: int):
    try:
        await bot.delete_message(chat_id=ADMIN_ID, message_id=msg_id)
    except Exception:  # noqa: BLE001
        pass


def _track_msg(message_id: int, skey: str, directory: str):
    MSG2SESS[message_id] = {"skey": skey, "directory": directory}
    while len(MSG2SESS) > MSG_TRACK_LIMIT:
        MSG2SESS.pop(next(iter(MSG2SESS)))


def _active_model() -> str:
    active = db.get_active()
    return (active or {}).get("model") or cc.DEFAULT_MODEL


# --------------------------------------------------------------------------- #
# Live status (fed by the SDK stream)
# --------------------------------------------------------------------------- #
def _build_status_text(st: dict) -> str:
    icons = {"busy": "🔴", "thinking": "🤔", "idle": "🟢", "error": "❌", "pending": "⚪"}
    state = st.get("state", "busy")
    icon = icons.get(state, "⚪")
    labels = {"busy": "TRABAJANDO", "thinking": "PENSANDO", "idle": "OK",
              "error": "ERROR", "pending": "ESPERANDO"}
    cwd_name = Path(st.get("directory", "")).name or "?"
    model = st.get("model") or "?"
    elapsed = _format_elapsed(time.time() - st.get("start_time", time.time()))

    lines = [
        f"{icon} *{labels.get(state, state.upper())}* | 📂 `{cwd_name}`",
        f"🧩 `{model}` | ⏱ `{elapsed}`",
    ]
    files = st.get("files_edited", set())
    if files:
        fs = ", ".join(f"`{f}`" for f in list(files)[:4])
        if len(files) > 4:
            fs += f" +{len(files)-4}"
        lines.append(f"📝 {fs}")
    if st.get("tool"):
        lines.append(f"🔧 `{st['tool']}`")
    else:
        tools = list(dict.fromkeys(st.get("tools_seen", [])))
        if tools:
            lines.append("⚡ " + " · ".join(f"`{t}`" for t in tools[-5:]))
    if state == "thinking" and st.get("reasoning_text"):
        snip = st["reasoning_text"][-200:].replace("`", "'").replace("*", "")
        lines.append(f"💭 _{snip}_")
    toks = st.get("tokens_input", 0) + st.get("tokens_output", 0)
    if toks:
        lines.append(f"🔢 `{toks}` tok")
    lines.append("\n_Pulsa_ /esc _para cancelar_")
    return "\n".join(lines)


async def _update_status(skey: str, force: bool = False):
    st = STATUSES.get(skey)
    if not st or not st.get("msg_id"):
        return
    now = time.time()
    if not force and (now - st.get("last_update_time", 0)) < STATUS_THROTTLE:
        return
    st["last_update_time"] = now
    try:
        await APP.bot.edit_message_text(
            chat_id=ADMIN_ID, message_id=st["msg_id"],
            text=_build_status_text(st), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")]]),
        )
    except BadRequest:
        pass
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)


async def _heartbeat(ctx: ContextTypes.DEFAULT_TYPE):
    for skey in list(STATUSES.keys()):
        await _update_status(skey, force=False)


def _ensure_heartbeat():
    if APP.job_queue and not APP.job_queue.get_jobs_by_name("hb"):
        APP.job_queue.run_repeating(_heartbeat, interval=STATUS_INTERVAL,
                                    first=STATUS_INTERVAL, name="hb")


def _start_status(skey: str, directory: str, msg_id: int, model: str):
    STATUSES[skey] = {
        "msg_id": msg_id, "directory": directory, "model": model,
        "state": "pending", "tool": None, "tools_seen": [], "files_edited": set(),
        "reasoning_text": None, "start_time": time.time(),
        "last_update_time": time.time(), "tokens_input": 0, "tokens_output": 0,
        "cost": 0.0,
    }
    _ensure_heartbeat()


# --------------------------------------------------------------------------- #
# Final reply
# --------------------------------------------------------------------------- #
async def _send_reply(skey: str, directory: str, st: dict, final: dict | None):
    cwd_name = Path(directory).name or "?"
    model = st.get("model") or "?"
    elapsed = _format_elapsed(time.time() - st.get("start_time", time.time()))
    cost = (final or {}).get("cost", 0.0)
    files = st.get("files_edited", set())

    session_id = _resume_for(skey)
    session_title = None
    if session_id:
        meta = db.get_session_meta(session_id)
        session_title = (meta or {}).get("title")
    
    header = f"✅ `{cwd_name}` | 🧩 `{model}` | ⏱ `{elapsed}`"
    if cost:
        header += f" | 💲`{cost:.4f}`"
    if session_title:
        truncated_title = session_title[:25] + ("..." if len(session_title) > 25 else "")
        header += f"\n📌 `{truncated_title}`"
    if files:
        names = list(files)[:3]
        header += " 📝 " + ", ".join(f"`{f}`" for f in names)
        if len(files) > 3:
            header += f" +{len(files)-3}"

    text = (final or {}).get("text", "") or ""
    if not text:
        sent = await APP.bot.send_message(ADMIN_ID, f"{md2tgv2.convert(header)}\n_Listo\\._",
                                          parse_mode="MarkdownV2")
        _track_msg(sent.message_id, skey, directory)
        return

    if len(text) > MD_FILE_THRESHOLD:
        import io
        f = io.BytesIO(text.encode("utf-8"))
        f.name = "respuesta.md"
        try:
            sent = await APP.bot.send_document(ADMIN_ID, document=f,
                                               caption=md2tgv2.convert(header),
                                               parse_mode="MarkdownV2")
            _track_msg(sent.message_id, skey, directory)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"send respuesta.md failed: {exc}")
        return

    chunks = [text[i:i+3800] for i in range(0, len(text), 3800)]
    last = None
    for i, chunk in enumerate(chunks):
        body = (f"{md2tgv2.convert(header)}\n" if i == 0 else "") + md2tgv2.convert(chunk)
        try:
            last = await APP.bot.send_message(ADMIN_ID, body, parse_mode="MarkdownV2")
        except BadRequest:
            plain = (f"{header}\n" if i == 0 else "") + chunk
            try:
                last = await APP.bot.send_message(ADMIN_ID, plain)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"send chunk failed: {exc}")
    if last:
        _track_msg(last.message_id, skey, directory)


async def _finish(skey: str, directory: str, final: dict | None):
    st = STATUSES.pop(skey, None)
    RUNNING.pop(skey, None)
    if APP.job_queue and not STATUSES:
        for j in APP.job_queue.get_jobs_by_name("hb"):
            j.schedule_removal()
    if st and st.get("msg_id"):
        await _delete_msg(APP.bot, st["msg_id"])
    if st:
        await _send_reply(skey, directory, st, final)
    await _drain_queue(skey, directory)


async def _drain_queue(skey: str, directory: str):
    q = QUEUES.get(skey)
    if not q:
        return
    item = q.popleft()
    if not q:
        QUEUES.pop(skey, None)
    await _dispatch(item["directory"], skey, item["model"], item["text"])


# --------------------------------------------------------------------------- #
# Task runner
# --------------------------------------------------------------------------- #
async def _run_task(skey: str, directory: str, prompt: str,
                    resume_sid: str | None, model: str):
    st = STATUSES.get(skey)
    final = None
    title = prompt.strip().replace("\n", " ")[:40]
    can_use_tool = _can_use_tool if PERMISSION_MODE != "bypassPermissions" else None
    try:
        async for ev in cc.run(prompt, directory, model, resume_sid,
                               PERMISSION_MODE, can_use_tool, MCP_SERVER):
            t = ev["type"]
            if t == "client":
                if skey in RUNNING:
                    RUNNING[skey]["client"] = ev["client"]
            elif t == "session":
                sid = ev["session_id"]
                KNOWN_SID[skey] = sid
                db.remember_session(sid, directory, model, title)
                active = db.get_active()
                if active and active.get("directory") == directory \
                        and not active.get("claude_session_id"):
                    db.update_active_session_id(sid)
            elif t == "text":
                if st:
                    st["state"] = "busy"
                await _update_status(skey)
            elif t == "thinking":
                if st:
                    st["state"] = "thinking"
                    st["reasoning_text"] = ev["text"]
                await _update_status(skey)
            elif t == "tool":
                if st:
                    st["state"] = "busy"
                    st["tool"] = ev["name"]
                    if ev["name"] not in st["tools_seen"]:
                        st["tools_seen"].append(ev["name"])
                    if ev["name"] in cc.EDIT_TOOLS:
                        fp = (ev["input"].get("file_path") or ev["input"].get("path")
                              or ev["input"].get("notebook_path"))
                        if fp:
                            st["files_edited"].add(Path(fp).name)
                await _update_status(skey, force=True)
            elif t == "usage":
                if st:
                    st["tokens_input"] = ev["input"]
                    st["tokens_output"] = ev["output"]
            elif t == "result":
                final = ev
                if st:
                    st["cost"] = ev.get("cost", 0.0)
            elif t == "error":
                await APP.bot.send_message(ADMIN_ID, f"❌ Error: {ev['message'][:1500]}")
    except asyncio.CancelledError:
        logger.info(f"task {skey} cancelled")
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error(f"run_task error: {exc}", exc_info=True)
        await APP.bot.send_message(ADMIN_ID, f"❌ Error inesperado: {exc}")
    finally:
        await _finish(skey, directory, final)


async def _dispatch(directory: str, skey: str, model: str, text: str):
    """Send a prompt: create the status message and launch the task (or queue)."""
    if skey in STATUSES:  # busy → queue
        QUEUES.setdefault(skey, deque()).append(
            {"text": text, "directory": directory, "model": model})
        pos = len(QUEUES[skey])
        await APP.bot.send_message(
            ADMIN_ID,
            f"⏳ `{Path(directory).name}` ocupado. En cola (posición {pos}).",
            parse_mode="Markdown")
        return

    # Reserve the slot synchronously before any await to prevent a second
    # message slipping through the STATUSES check during the Telegram round-trip.
    STATUSES[skey] = {"state": "reserving"}

    sent = await APP.bot.send_message(
        ADMIN_ID,
        f"⚪ *ESPERANDO* | 📂 `{Path(directory).name}`\n"
        f"🧩 `{model}` | ⏱ `00:00`\n\n_Pulsa_ /esc _para cancelar_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancelar", callback_data="abort:")]]))
    _start_status(skey, directory, sent.message_id, model)
    _track_msg(sent.message_id, skey, directory)
    RUNNING[skey] = {"client": None, "directory": directory}

    resume_sid = _resume_for(skey)
    task = asyncio.create_task(_run_task(skey, directory, text, resume_sid, model))
    RUNNING[skey]["task"] = task


# --------------------------------------------------------------------------- #
# Folder browser  (/open)
# --------------------------------------------------------------------------- #
PAGE = 8


def _folder_kbd(path: Path, page: int):
    try:
        entries = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    except PermissionError:
        entries = []
    dirs = [e for e in entries if e.is_dir() and not e.name.startswith(".")]
    files = [e for e in entries if e.is_file()]
    all_e = dirs + files
    total = max(1, (len(all_e) + PAGE - 1) // PAGE)
    page = max(0, min(page, total - 1))
    chunk = all_e[page*PAGE:(page+1)*PAGE]

    pk = _key(str(path))
    btns = []
    for e in chunk:
        icon = "📁" if e.is_dir() else "📄"
        btns.append([InlineKeyboardButton(f"{icon} {e.name}",
                                          callback_data=f"ob:{_key(str(e))}:0")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"ob:{pk}:{page-1}"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"ob:{pk}:{page+1}"))
    if nav:
        btns.append(nav)
    btns.append([InlineKeyboardButton("✅ Abrir aquí", callback_data=f"os:{pk}")])
    btns.append([InlineKeyboardButton("📁 Nueva carpeta", callback_data=f"mkdir:{pk}")])
    if path.parent != path:
        btns.append([InlineKeyboardButton("⬆ Subir", callback_data=f"ob:{_key(str(path.parent))}:0")])
    return f"📂 `{path}`  _{page+1}/{total}_", InlineKeyboardMarkup(btns)


@admin_only
async def cmd_open(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _clear_send_mode()
    txt, kbd = _folder_kbd(WORKSPACE, 0)
    await update.message.reply_text(txt, reply_markup=kbd, parse_mode="Markdown")


async def cb_ob(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, pk, pg = q.data.split(":")
    path = Path(_val(int(pk)))
    if path.is_file():
        path = path.parent
    txt, kbd = _folder_kbd(path, int(pg))
    await q.edit_message_text(txt, reply_markup=kbd, parse_mode="Markdown")


async def cb_mkdir(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    path = _val(int(q.data.split(":")[1]))
    MKDIR_PENDING.clear()
    MKDIR_PENDING.update({"path": path, "msg_id": q.message.message_id})
    await q.edit_message_text(f"📁 Nueva carpeta en `{Path(path).name}`\n\nEscribe el nombre:",
                              parse_mode="Markdown")


async def cb_os(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Folder chosen → existing sessions picker or model picker."""
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    sessions = _list_sessions(directory=cwd)
    if sessions:
        await _show_session_picker(q, cwd, sessions)
    else:
        await _show_model_picker(q, cwd)


async def _show_session_picker(q, cwd: str, sessions: list, mode: str = "activate"):
    pk = _key(cwd)
    if mode == "send":
        new_cb = f"sendnew:{pk}"
        cur_sid = (SEND_MODE.get("target") or {}).get("skey")
        title = f"📤 `{Path(cwd).name}` — {len(sessions)} sesión(es) (destino)"
        sel = lambda sid: f"sendsess:{_key(sid)}:{pk}"
        dele = lambda sid: f"senddel:{_key(sid)}:{pk}"
    else:
        new_cb = f"newsess:{pk}"
        cur_sid = (db.get_active() or {}).get("claude_session_id")
        title = f"📂 `{Path(cwd).name}` — {len(sessions)} sesión(es)"
        sel = lambda sid: f"actsess:{_key(sid)}:{pk}"
        dele = lambda sid: f"delsess:{_key(sid)}:{pk}"
    btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=new_cb)]]
    for s in sessions[:10]:
        sid = s.session_id
        mark = " ✅" if sid == cur_sid else ""
        btns.append([
            InlineKeyboardButton(f"{_session_label(s)[:28]}{mark}", callback_data=sel(sid)),
            InlineKeyboardButton("🗑", callback_data=dele(sid)),
        ])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await q.edit_message_text(
        title, reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def _show_model_picker(q, cwd: str | None):
    pk = _key(cwd) if cwd else -1
    btns = [[InlineKeyboardButton(("🧩 " + m), callback_data=f"setmodel:{pk}:{_key(m)}")]
            for m in cc.MODELS]
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    header = f"📂 `{Path(cwd).name}`\n" if cwd else ""
    await q.edit_message_text(f"{header}🧩 Elige modelo:",
                              reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_newsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    await _show_model_picker(q, cwd)


async def cb_setmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model chosen → either create a new active session (cwd given) or change /models."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    pk = int(parts[1])
    model = _val(int(parts[2]))

    if pk == -1:  # /models mode → change active session model
        active = db.get_active()
        if not active:
            await q.edit_message_text("⚠️ No hay sesión activa.")
            return
        db.set_active(active["directory"], active.get("claude_session_id"), model)
        if active.get("claude_session_id"):
            db.set_session_model(active["claude_session_id"], model)
        await q.edit_message_text(f"✅ Modelo `{model}` aplicado a la sesión activa.",
                                  parse_mode="Markdown")
        return

    cwd = _val(pk)
    db.set_active(cwd, None, model)  # new session, materializes on first prompt
    await q.edit_message_text(
        f"✅ Sesión nueva en `{Path(cwd).name}`\n🧩 `{model}`\n\nEnvía tu primer prompt.",
        parse_mode="Markdown")


async def cb_actsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    sid = _val(int(parts[1]))
    cwd = _val(int(parts[2])) if len(parts) > 2 else ""
    meta = db.get_session_meta(sid)
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    db.set_active(cwd, sid, model)
    await q.edit_message_text(f"✅ Sesión activa\n📂 `{Path(cwd).name}`\n🧩 `{model}`",
                              parse_mode="Markdown")


async def cb_delsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    sid = _val(int(parts[1]))
    cwd = _val(int(parts[2])) if len(parts) > 2 else ""
    try:
        sdk.delete_session(sid, directory=cwd or None)
    except Exception as exc:  # noqa: BLE001
        await q.edit_message_text(f"❌ Error: {exc}")
        return
    db.forget_session(sid)
    active = db.get_active()
    if active and active.get("claude_session_id") == sid:
        db.clear_active()
    sessions = _list_sessions(directory=cwd)
    if sessions:
        await _show_session_picker(q, cwd, sessions)
    else:
        db.set_active(cwd, None, cc.DEFAULT_MODEL)
        await q.edit_message_text(
            f"✅ Sesión borrada.\n"
            f"📌 Nueva sesión creada automáticamente en `{Path(cwd).name}`\n"
            f"🧩 `{cc.DEFAULT_MODEL}`",
            parse_mode="Markdown")


async def cb_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("❌ Cancelado.")


# --------------------------------------------------------------------------- #
# /sessions /projects /close
# --------------------------------------------------------------------------- #
def _group_by_dir(sessions: list) -> dict:
    by_dir: dict[str, list] = {}
    for s in sessions:
        d = getattr(s, "cwd", "") or ""
        if d:
            by_dir.setdefault(d, []).append(s)
    return by_dir


@admin_only
async def cmd_sessions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _clear_send_mode()
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await update.message.reply_text("No hay sesiones todavía. Usa /open.")
        return
    active_dir = (db.get_active() or {}).get("directory", "")
    btns = []
    for d in sorted(by_dir):
        mark = " ✅" if d == active_dir else ""
        btns.append([InlineKeyboardButton(
            f"📂 {Path(d).name}{mark} ({len(by_dir[d])})",
            callback_data=f"sesspick:{_key(d)}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text("¿De qué proyecto?",
                                    reply_markup=InlineKeyboardMarkup(btns))


async def cb_sesspick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    sessions = _list_sessions(directory=cwd)
    if sessions:
        await _show_session_picker(q, cwd, sessions)
    else:
        await q.edit_message_text(f"No quedan sesiones en `{Path(cwd).name}`.",
                                  parse_mode="Markdown")


@admin_only
async def cmd_projects(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await update.message.reply_text("No hay proyectos con sesiones. Usa /open.")
        return
    active_dir = (db.get_active() or {}).get("directory", "")
    lines = ["*Proyectos con sesiones*\n"]
    for d in sorted(by_dir):
        marker = " ◀ activo" if d == active_dir else ""
        lines.append(f"📂 *{Path(d).name}*{marker} — {len(by_dir[d])} sesión(es)")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


@admin_only
async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await update.message.reply_text("No hay proyectos con sesiones.")
        return
    btns = [[InlineKeyboardButton(f"📂 {Path(d).name} ({len(by_dir[d])})",
                                  callback_data=f"closedir:{_key(d)}")]
            for d in sorted(by_dir)]
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text("¿Qué proyecto cierro (borra sus sesiones)?",
                                    reply_markup=InlineKeyboardMarkup(btns))


async def cb_closedir(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    sessions = _list_sessions(directory=cwd)
    deleted = 0
    for s in sessions:
        try:
            sdk.delete_session(s.session_id, directory=cwd)
            db.forget_session(s.session_id)
            deleted += 1
        except Exception:  # noqa: BLE001
            pass
    active = db.get_active()
    if active and active.get("directory") == cwd:
        db.clear_active()
    await q.edit_message_text(f"✅ `{Path(cwd).name}` cerrado — {deleted} sesión(es) borradas.",
                              parse_mode="Markdown")


# --------------------------------------------------------------------------- #
# /models  /permisos
# --------------------------------------------------------------------------- #
@admin_only
async def cmd_models(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    cur = active.get("model") or cc.DEFAULT_MODEL
    btns = [[InlineKeyboardButton(("✅ " if m == cur else "🧩 ") + m,
                                  callback_data=f"setmodel:-1:{_key(m)}")]
            for m in cc.MODELS]
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text(f"🧩 Modelo actual: `{cur}`\nElige:",
                                    reply_markup=InlineKeyboardMarkup(btns),
                                    parse_mode="Markdown")


@admin_only
async def cmd_rename(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    sid = active.get("claude_session_id")
    if not sid:
        await update.message.reply_text(
            "⚠️ La sesión aún no existe (envía un primer prompt antes de renombrarla).")
        return
    title = " ".join(ctx.args).strip()
    if not title:
        await update.message.reply_text("Uso: `/rename <nuevo nombre>`", parse_mode="Markdown")
        return
    title = title[:60]
    db.set_session_title(sid, title)
    await update.message.reply_text(f"✅ Sesión renombrada: `{title}`", parse_mode="Markdown")


@admin_only
async def cmd_btw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Side question (à la /btw): sees the active session's context, no tools,
    doesn't touch its history (forks + deletes a throwaway session)."""
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    sid = active.get("claude_session_id")
    if not sid:
        await update.message.reply_text(
            "⚠️ La sesión activa aún no tiene contexto (envía un primer prompt).")
        return
    question = " ".join(ctx.args).strip()
    if not question:
        await update.message.reply_text(
            "Uso: `/btw <pregunta>`\nPregunta rápida sobre la sesión activa "
            "(ve el contexto, sin herramientas, no afecta al historial).",
            parse_mode="Markdown")
        return

    directory = active["directory"]
    model = active.get("model") or cc.DEFAULT_MODEL
    status = await update.message.reply_text("💬 _Pensando \\(btw\\)…_",
                                             parse_mode="MarkdownV2")
    answer = ""
    forked_sid = None
    try:
        async for ev in cc.ask_side(question, directory, model, sid):
            t = ev["type"]
            if t == "session":
                forked_sid = ev["session_id"]
            elif t == "text":
                answer += ev["text"]
            elif t == "result":
                forked_sid = forked_sid or ev.get("session_id")
                if not answer:
                    answer = ev.get("text", "")
            elif t == "error":
                answer = answer or f"❌ {ev['message'][:800]}"
    except Exception as exc:  # noqa: BLE001
        answer = f"❌ {exc}"

    # Drop the throwaway forked session so it never clutters the pickers.
    if forked_sid and forked_sid != sid:
        try:
            sdk.delete_session(forked_sid, directory=directory)
        except Exception:  # noqa: BLE001
            pass
        db.forget_session(forked_sid)

    out = "💬 *BTW* — no afecta al historial\n\n" + (answer or "(sin respuesta)")
    chunks = [out[i:i+3800] for i in range(0, len(out), 3800)] or [out]
    try:
        await status.edit_text(md2tgv2.convert(chunks[0]), parse_mode="MarkdownV2")
    except BadRequest:
        await status.edit_text(chunks[0])
    for c in chunks[1:]:
        try:
            await APP.bot.send_message(ADMIN_ID, md2tgv2.convert(c), parse_mode="MarkdownV2")
        except BadRequest:
            await APP.bot.send_message(ADMIN_ID, c)


PERM_MODES = ["bypassPermissions", "acceptEdits", "default", "plan"]


@admin_only
async def cmd_permisos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    btns = [[InlineKeyboardButton(("✅ " if m == PERMISSION_MODE else "") + m,
                                  callback_data=f"perm:{_key(m)}")]
            for m in PERM_MODES]
    await update.message.reply_text(
        f"🔐 Modo de permisos actual: `{PERMISSION_MODE}`\n\n"
        "• `bypassPermissions` — hace todo sin preguntar\n"
        "• `acceptEdits` — edita sin preguntar, pregunta comandos\n"
        "• `default` — pregunta (botones) en cada acción\n"
        "• `plan` — solo planifica, no toca nada\n\nElige:",
        reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_perm_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global PERMISSION_MODE
    q = update.callback_query
    await q.answer()
    PERMISSION_MODE = _val(int(q.data.split(":")[1]))
    await q.edit_message_text(f"🔐 Modo de permisos: `{PERMISSION_MODE}`",
                              parse_mode="Markdown")


# --------------------------------------------------------------------------- #
# /esc
# --------------------------------------------------------------------------- #
async def _abort(skey: str) -> str:
    entry = RUNNING.get(skey)
    if not entry:
        return "⚠️ No hay tarea en curso."
    client = entry.get("client")
    if client:
        try:
            await client.interrupt()
        except Exception:  # noqa: BLE001
            pass
    task = entry.get("task")
    if task and not task.done():
        task.cancel()
    return "🛑 Tarea cancelada."


@admin_only
async def cmd_esc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa.")
        return
    skey = _skey(active["directory"], active.get("claude_session_id"))
    await update.message.reply_text(await _abort(skey))


async def cb_abort(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    target = MSG2SESS.get(q.message.message_id)
    skey = target["skey"] if target else None
    if not skey:
        active = db.get_active()
        skey = _skey(active["directory"], active.get("claude_session_id")) if active else None
    msg = await _abort(skey) if skey else "⚠️ No hay tarea en curso."
    try:
        await q.edit_message_text(msg)
    except BadRequest:
        await APP.bot.send_message(ADMIN_ID, msg)


# --------------------------------------------------------------------------- #
# Permission + question bridges (used in non-bypass modes / ask_user tool)
# --------------------------------------------------------------------------- #
async def _can_use_tool(tool_name: str, input_data: dict, context):
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    qid = _key(f"perm-{len(PENDING_PERMS)}-{time.time()}")
    PENDING_PERMS[qid] = fut
    preview = str(input_data)[:120]
    btns = [
        [InlineKeyboardButton("✅ Permitir", callback_data=f"pa:{qid}:1"),
         InlineKeyboardButton("❌ Denegar", callback_data=f"pa:{qid}:0")],
    ]
    await APP.bot.send_message(
        ADMIN_ID,
        f"🔐 *Permiso*\nClaude quiere usar `{tool_name}`\n`{preview}`",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(btns))
    try:
        allow = await asyncio.wait_for(fut, timeout=600)
    except asyncio.TimeoutError:
        return sdk.PermissionResultDeny(message="Sin respuesta (timeout)")
    finally:
        PENDING_PERMS.pop(qid, None)
    if allow:
        return sdk.PermissionResultAllow(updated_input=input_data)
    return sdk.PermissionResultDeny(message="Denegado por el usuario")


async def cb_perm_answer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, qid_k, val = q.data.split(":")
    fut = PENDING_PERMS.get(int(qid_k))
    if fut and not fut.done():
        fut.set_result(val == "1")
    await q.edit_message_text("✅ Permitido" if val == "1" else "❌ Denegado")


async def _question_bridge(question: str, options: str) -> str:
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    qid = _key(f"q-{len(PENDING_Q)}-{time.time()}")
    opts = [o.strip() for o in options.split(",") if o.strip()] if options else []
    PENDING_Q[qid] = {"future": fut, "options": opts}
    btns = [[InlineKeyboardButton(o[:60], callback_data=f"qa:{qid}:{i}")]
            for i, o in enumerate(opts)]
    btns.append([InlineKeyboardButton("✏️ Responder por texto", callback_data=f"qc:{qid}")])
    await APP.bot.send_message(ADMIN_ID, f"❓ {question}",
                              reply_markup=InlineKeyboardMarkup(btns))
    try:
        return await asyncio.wait_for(fut, timeout=900)
    except asyncio.TimeoutError:
        return ""
    finally:
        PENDING_Q.pop(qid, None)


async def cb_q_answer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, qid_k, idx = q.data.split(":")
    data = PENDING_Q.get(int(qid_k))
    if not data:
        await q.edit_message_text("⚠️ Pregunta expirada.")
        return
    answer = data["options"][int(idx)] if int(idx) < len(data["options"]) else ""
    if not data["future"].done():
        data["future"].set_result(answer)
    await q.edit_message_text(f"✅ {answer}")


async def cb_q_custom(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    qid_k = int(q.data.split(":")[1])
    ctx.bot_data["q_custom_qid"] = qid_k
    await q.edit_message_text("✏️ Escribe tu respuesta (el próximo mensaje):")


# --------------------------------------------------------------------------- #
# /send mode
# --------------------------------------------------------------------------- #
def _clear_send_mode() -> bool:
    was = SEND_MODE["on"] or SEND_MODE["target"] is not None
    SEND_MODE.update({"on": False, "target": None, "pending_text": None})
    return was


@admin_only
async def cmd_multisesion(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    SEND_MODE["on"] = True
    by_dir = _group_by_dir(_list_sessions())
    btns = [[InlineKeyboardButton(f"📂 {Path(d).name} ({len(by_dir[d])})",
                                  callback_data=f"sendpick:{_key(d)}")]
            for d in sorted(by_dir)]
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text(
        "🔀 *Modo multisesión activo* — preguntaré el destino en *cada* mensaje "
        "(no cambia la sesión activa). Responde a un mensaje del bot para seguir "
        "esa sesión concreta.\n\n⚠️ Sigues en multisesión hasta que pongas "
        "/exitmulti.", reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown")


async def cb_sendpick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    sessions = _list_sessions(directory=cwd)
    await _show_session_picker(q, cwd, sessions, mode="send")


async def _send_to_target(q, cwd: str, skey: str, model: str, label: str):
    """Route one message to the chosen destination. The target is transient:
    after dispatch it is cleared so the next clean message asks again."""
    pending = SEND_MODE.pop("pending_text", None)
    SEND_MODE["pending_text"] = None
    if pending:
        SEND_MODE["target"] = None  # one message per pick → re-ask next time
        await q.edit_message_text(
            f"📤 Enviando a {label}…\n_🔀 Sigues en multisesión · /exitmulti para salir_",
            parse_mode="Markdown")
        await _dispatch(cwd, skey, model, pending)
    else:
        # /send invoked without text yet: hold this destination for the very
        # next message only (handle_text clears it once that message is sent).
        SEND_MODE["target"] = {"skey": skey, "directory": cwd, "model": model}
        await q.edit_message_text(
            f"📤 Destino: {label}\n🧩 `{model}`\nEscribe el mensaje.", parse_mode="Markdown")


async def cb_sendsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    sid = _val(int(parts[1]))
    cwd = _val(int(parts[2]))
    meta = db.get_session_meta(sid)
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    await _send_to_target(q, cwd, sid, model, f"`{Path(cwd).name}`")


async def cb_sendnew(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """➕ Nueva sesión in send mode → pick a model for the new target session."""
    q = update.callback_query
    await q.answer()
    pk = int(q.data.split(":")[1])
    cwd = _val(pk)
    btns = [[InlineKeyboardButton("🧩 " + m, callback_data=f"sendmodel:{pk}:{_key(m)}")]
            for m in cc.MODELS]
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await q.edit_message_text(
        f"📤 `{Path(cwd).name}` — nueva sesión\n🧩 Elige modelo:",
        reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_sendmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model chosen for a new send-target session (materializes on first prompt)."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    cwd = _val(int(parts[1]))
    model = _val(int(parts[2]))
    await _send_to_target(q, cwd, _skey(cwd, None), model,
                          f"nueva sesión en `{Path(cwd).name}`")


async def cb_senddel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete a session from the send picker, then re-render it."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    sid = _val(int(parts[1]))
    cwd = _val(int(parts[2])) if len(parts) > 2 else ""
    try:
        sdk.delete_session(sid, directory=cwd or None)
    except Exception as exc:  # noqa: BLE001
        await q.edit_message_text(f"❌ Error: {exc}")
        return
    db.forget_session(sid)
    if (SEND_MODE.get("target") or {}).get("skey") == sid:
        SEND_MODE["target"] = None
    await _show_session_picker(q, cwd, _list_sessions(directory=cwd), mode="send")


@admin_only
async def cmd_exitmulti(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if _clear_send_mode():
        await update.message.reply_text("✅ Modo multisesión desactivado. "
                                        "Vuelves a tu sesión activa.")
    else:
        await update.message.reply_text("No estabas en modo multisesión.")


# --------------------------------------------------------------------------- #
# Uploads + audio
# --------------------------------------------------------------------------- #
@admin_only
async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    file_id = file_name = None
    if msg.document:
        file_id, file_name = msg.document.file_id, msg.document.file_name or f"doc_{int(time.time())}"
    elif msg.photo:
        file_id, file_name = msg.photo[-1].file_id, f"photo_{int(time.time())}.jpg"
    elif msg.video:
        file_id, file_name = msg.video.file_id, msg.video.file_name or f"video_{int(time.time())}.mp4"
    if not file_id:
        return
    active = db.get_active()
    if not active:
        await msg.reply_text("❌ No hay sesión activa. Usa /open.")
        return
    cwd = active["directory"]
    save_path = Path(cwd) / file_name
    try:
        tg = await ctx.bot.get_file(file_id)
        await tg.download_to_drive(save_path)
    except Exception as exc:  # noqa: BLE001
        await msg.reply_text(f"❌ Error al guardar: {exc}")
        return
    await msg.reply_text(f"✅ `{file_name}` guardado en `{Path(cwd).name}`.",
                         parse_mode="Markdown")


@admin_only
async def handle_audio(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    file_id = file_name = None
    if msg.audio:
        file_id, file_name = msg.audio.file_id, msg.audio.file_name or f"audio_{int(time.time())}.mp3"
    elif msg.voice:
        file_id, file_name = msg.voice.file_id, f"voice_{int(time.time())}.ogg"
    if not file_id:
        return
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp = TMP_DIR / file_name
    try:
        tg = await ctx.bot.get_file(file_id)
        await tg.download_to_drive(tmp)
    except Exception as exc:  # noqa: BLE001
        await msg.reply_text(f"❌ Error al descargar audio: {exc}")
        return
    if not grok_stt.is_configured():
        await msg.reply_text("⚠️ Transcripción no disponible (falta XAI_API_KEY).")
        tmp.unlink(missing_ok=True)
        return
    status = await msg.reply_text("🎙️ Transcribiendo…")
    text = await grok_stt.transcribe(str(tmp))
    tmp.unlink(missing_ok=True)
    if not text:
        await status.edit_text("⚠️ No se pudo transcribir.")
        return
    active = db.get_active()
    if not active:
        await status.edit_text(f"🎙️ Transcripción (sin sesión activa):\n\n{text}")
        return
    await status.edit_text(f"🎙️ *Transcrito → enviando:*\n\n{text}", parse_mode="Markdown")
    skey = _skey(active["directory"], active.get("claude_session_id"))
    await _dispatch(active["directory"], skey, active.get("model") or cc.DEFAULT_MODEL, text)


# --------------------------------------------------------------------------- #
# Plain text → prompt
# --------------------------------------------------------------------------- #
@admin_only
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    # Pending mkdir name?
    if MKDIR_PENDING:
        parent = Path(MKDIR_PENDING["path"])
        new_dir = parent / text.strip()
        msg_id = MKDIR_PENDING.get("msg_id")
        MKDIR_PENDING.clear()
        try:
            new_dir.mkdir(parents=False, exist_ok=False)
        except Exception as exc:  # noqa: BLE001
            await update.message.reply_text(f"❌ {exc}")
            return
        txt, kbd = _folder_kbd(new_dir, 0)
        try:
            await ctx.bot.edit_message_text(chat_id=ADMIN_ID, message_id=msg_id,
                                            text=txt, reply_markup=kbd, parse_mode="Markdown")
        except Exception:  # noqa: BLE001
            await update.message.reply_text(txt, reply_markup=kbd, parse_mode="Markdown")
        await update.message.reply_text(f"✅ Carpeta `{text}` creada.", parse_mode="Markdown")
        return

    # Pending custom question answer?
    if "q_custom_qid" in ctx.bot_data:
        qid_k = ctx.bot_data.pop("q_custom_qid")
        data = PENDING_Q.get(qid_k)
        if data and not data["future"].done():
            data["future"].set_result(text)
            await update.message.reply_text("✅ Respuesta enviada a Claude.")
        else:
            await update.message.reply_text("⚠️ La pregunta expiró.")
        return

    # Resolve target: reply > send target > active
    reply = update.message.reply_to_message
    if reply and reply.from_user and reply.from_user.is_bot and reply.message_id in MSG2SESS:
        tgt = MSG2SESS[reply.message_id]
        directory, skey = tgt["directory"], tgt["skey"]
        meta = db.get_session_meta(_resume_for(skey) or "")
        model = (meta or {}).get("model") or _active_model()
    elif SEND_MODE["on"] and SEND_MODE["target"] is None:
        SEND_MODE["pending_text"] = text
        await cmd_multisesion(update, ctx)
        return
    elif SEND_MODE["target"]:
        t = SEND_MODE["target"]
        directory, skey, model = t["directory"], t["skey"], t["model"]
        SEND_MODE["target"] = None  # transient: next clean message re-asks
    else:
        active = db.get_active()
        if not active or not active.get("directory"):
            await update.message.reply_text("❌ No hay sesión activa. Usa /open.")
            return
        directory = active["directory"]
        skey = _skey(directory, active.get("claude_session_id"))
        model = active.get("model") or cc.DEFAULT_MODEL

    await _dispatch(directory, skey, model, text)


# --------------------------------------------------------------------------- #
# /start /help /restart
# --------------------------------------------------------------------------- #
HELP = (
    "*claude-bot* — Claude Code por Telegram\n\n"
    "/open — navegar carpetas, abrir proyecto / sesión\n"
    "/sessions — gestionar sesiones de un proyecto\n"
    "/projects — proyectos con sesiones\n"
    "/models — cambiar modelo (opus/sonnet/haiku)\n"
    "/rename — renombrar la sesión activa (`/rename mi nombre`)\n"
    "/btw — pregunta rápida sobre la sesión, sin tocar su historial\n"
    "/permisos — modo de permisos\n"
    "/multisesion — pregunta destino en cada mensaje\n"
    "/exitmulti — salir de multisesión\n"
    "/close — borrar sesiones de un proyecto\n"
    "/esc — cancelar la tarea en curso\n"
    "/restart — reiniciar el bot\n\n"
    "Responde a un mensaje del bot para continuar esa sesión concreta.\n"
    "Envía audio para dictar, o archivos para guardarlos en el proyecto."
)


@admin_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if active:
        cwd = Path(active["directory"]).name
        sid = active.get("claude_session_id") or "(nueva)"
        model = active.get("model") or cc.DEFAULT_MODEL
        head = (f"*Sesión activa*\n📂 `{cwd}`\n📦 `{sid[:12] if sid != '(nueva)' else sid}`\n"
                f"🧩 `{model}` | 🔐 `{PERMISSION_MODE}`\n\n")
    else:
        head = f"⚠️ Sin sesión activa · 🔐 `{PERMISSION_MODE}`\n\n"
    await update.message.reply_text(head + HELP, parse_mode="Markdown")


@admin_only
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP, parse_mode="Markdown")


@admin_only
async def cmd_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Reiniciando…")
    RESTART_FLAG.write_text(str(msg.message_id))
    import subprocess
    r = subprocess.run(["systemctl", "--user", "restart", "claude-bot.service"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        RESTART_FLAG.unlink(missing_ok=True)
        await msg.edit_text("⚠️ No hay servicio systemd `claude-bot.service`.\n"
                            "Reinícialo a mano con `./run.sh`.", parse_mode="Markdown")


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
def main():
    global APP
    db.init()
    cc.set_question_bridge(_question_bridge)

    asyncio.set_event_loop(asyncio.new_event_loop())  # Python 3.14 fix
    app = Application.builder().token(TOKEN).build()
    APP = app

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("sessions", cmd_sessions))
    app.add_handler(CommandHandler("projects", cmd_projects))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("models", cmd_models))
    app.add_handler(CommandHandler("rename", cmd_rename))
    app.add_handler(CommandHandler("btw", cmd_btw))
    app.add_handler(CommandHandler("permisos", cmd_permisos))
    app.add_handler(CommandHandler("multisesion", cmd_multisesion))
    app.add_handler(CommandHandler("exitmulti", cmd_exitmulti))
    app.add_handler(CommandHandler("esc", cmd_esc))
    app.add_handler(CommandHandler("restart", cmd_restart))

    app.add_handler(CallbackQueryHandler(cb_ob, pattern=r"^ob:"))
    app.add_handler(CallbackQueryHandler(cb_mkdir, pattern=r"^mkdir:"))
    app.add_handler(CallbackQueryHandler(cb_os, pattern=r"^os:"))
    app.add_handler(CallbackQueryHandler(cb_newsess, pattern=r"^newsess:"))
    app.add_handler(CallbackQueryHandler(cb_setmodel, pattern=r"^setmodel:"))
    app.add_handler(CallbackQueryHandler(cb_actsess, pattern=r"^actsess:"))
    app.add_handler(CallbackQueryHandler(cb_delsess, pattern=r"^delsess:"))
    app.add_handler(CallbackQueryHandler(cb_sesspick, pattern=r"^sesspick:"))
    app.add_handler(CallbackQueryHandler(cb_closedir, pattern=r"^closedir:"))
    app.add_handler(CallbackQueryHandler(cb_perm_mode, pattern=r"^perm:"))
    app.add_handler(CallbackQueryHandler(cb_perm_answer, pattern=r"^pa:"))
    app.add_handler(CallbackQueryHandler(cb_q_answer, pattern=r"^qa:"))
    app.add_handler(CallbackQueryHandler(cb_q_custom, pattern=r"^qc:"))
    app.add_handler(CallbackQueryHandler(cb_sendpick, pattern=r"^sendpick:"))
    app.add_handler(CallbackQueryHandler(cb_sendsess, pattern=r"^sendsess:"))
    app.add_handler(CallbackQueryHandler(cb_sendnew, pattern=r"^sendnew:"))
    app.add_handler(CallbackQueryHandler(cb_sendmodel, pattern=r"^sendmodel:"))
    app.add_handler(CallbackQueryHandler(cb_senddel, pattern=r"^senddel:"))
    app.add_handler(CallbackQueryHandler(cb_abort, pattern=r"^abort:"))
    app.add_handler(CallbackQueryHandler(cb_cancel, pattern=r"^cancel:"))

    app.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO | filters.VIDEO, handle_file))
    app.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    async def post_init(application: Application):
        await application.bot.set_my_commands([
            BotCommand("start", "Estado y menú"),
            BotCommand("open", "Abrir proyecto / sesión"),
            BotCommand("sessions", "Gestionar sesiones"),
            BotCommand("projects", "Proyectos con sesiones"),
            BotCommand("models", "Cambiar modelo"),
            BotCommand("rename", "Renombrar sesión activa"),
            BotCommand("btw", "Pregunta rápida (no afecta historial)"),
            BotCommand("permisos", "Modo de permisos"),
            BotCommand("multisesion", "Preguntar destino en cada mensaje"),
            BotCommand("exitmulti", "Salir de multisesión"),
            BotCommand("close", "Cerrar proyecto"),
            BotCommand("esc", "Cancelar tarea"),
            BotCommand("restart", "Reiniciar bot"),
        ])
        if RESTART_FLAG.exists():
            try:
                mid = int(RESTART_FLAG.read_text().strip())
                RESTART_FLAG.unlink(missing_ok=True)
                await application.bot.edit_message_text(chat_id=ADMIN_ID, message_id=mid,
                                                        text="✅ Bot reiniciado.")
            except Exception:  # noqa: BLE001
                RESTART_FLAG.unlink(missing_ok=True)

    app.post_init = post_init
    logger.info("claude-bot starting (permission_mode=%s, workspace=%s)",
                PERMISSION_MODE, WORKSPACE)
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
