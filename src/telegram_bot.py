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
from telegram.error import BadRequest, RetryAfter, NetworkError

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
SEND_MODE = {"on": False, "target": None, "pending_text": None,
             "oneshot": False, "oneshot_pre": False}
MKDIR_PENDING: dict = {}   # {"path","msg_id"}

STATUS_INTERVAL = 10
STATUS_THROTTLE = 3
MSG_TRACK_LIMIT = 200
MD_FILE_THRESHOLD = 6000
PENDING_FLOW_TTL = 300  # s — after this, a stale mkdir/question flow is ignored


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
    k = (max(KEYSTORE) + 1) if KEYSTORE else 0
    KEYSTORE[k] = value
    try:
        db.keystore_put(k, value)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"keystore_put failed: {exc}")
    return k


# Sentinel returned by _val for an int that isn't in the keystore (e.g. a
# button from before a wipe). Distinct from "" so callbacks can detect a
# stale/expired menu and tell the user to reopen it instead of acting on "".
KEY_MISSING = "\x00__missing__"


def _val(k: int) -> str:
    return KEYSTORE.get(k, KEY_MISSING)


def _vals(*ks: int) -> list[str] | None:
    """Resolve several keystore ints at once. Returns None if ANY is missing
    (stale callback), so callers can bail out with a single guard."""
    out = []
    for k in ks:
        v = KEYSTORE.get(k, KEY_MISSING)
        if v == KEY_MISSING:
            return None
        out.append(v)
    return out


async def _expired(q) -> None:
    """Tell the user a button no longer resolves (keystore lost its value,
    typically after a data wipe) and that they should reopen the menu."""
    try:
        await q.edit_message_text(
            "⚠️ Este menú ha caducado (el bot perdió su contexto). "
            "Vuelve a abrirlo con /open o el comando correspondiente.")
    except BadRequest:
        await APP.bot.send_message(
            ADMIN_ID,
            "⚠️ Ese botón ha caducado. Vuelve a abrir el menú con /open.")


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


def _find_session(sid: str, cwd: str):
    """Return the SDK session object for sid, or None."""
    for s in _list_sessions(directory=cwd):
        if s.session_id == sid:
            return s
    return None


def _context_bar(file_size: int) -> str:
    """Rough context-usage indicator from conversation JSONL file size."""
    est = file_size // 6
    pct = min(99, est * 100 // 200_000)
    icon = "🟢" if pct < 40 else ("🟡" if pct < 70 else ("🟠" if pct < 90 else "🔴"))
    kb = file_size / 1024
    size_str = f"{kb:.0f} KB" if kb < 1024 else f"{kb / 1024:.1f} MB"
    tip = " — considera sesión nueva" if pct >= 80 else ""
    return f"{icon} ctx ~{pct}% ({size_str}){tip}"


def _ctx_pct(tokens_input: int) -> tuple[str, int]:
    """Returns (inline indicator, pct) from real input token count. 200K window."""
    pct = min(99, tokens_input * 100 // 200_000)
    icon = "🟢" if pct < 40 else ("🟡" if pct < 70 else ("🟠" if pct < 90 else "🔴"))
    return f"{icon} ctx {pct}%", pct


def _session_card(s, meta: dict | None, cwd: str) -> str:
    """Multi-line session summary used in activation messages and restart notice."""
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    title = (_session_label(s).replace("`", "'").replace("*", "·"))[:50]
    branch = getattr(s, "git_branch", None)
    last_mod = getattr(s, "last_modified", None)
    file_size = getattr(s, "file_size", 0) or 0

    dir_line = f"📂 `{Path(cwd).name}`"
    if branch:
        dir_line += f"  🌿 `{branch}`"

    ago_str = ""
    if last_mod:
        ago = time.time() - last_mod / 1000
        if ago < 60:
            ago_str = "ahora mismo"
        elif ago < 3600:
            ago_str = f"hace {int(ago // 60)} min"
        elif ago < 86400:
            ago_str = f"hace {int(ago // 3600)} h"
        else:
            ago_str = f"hace {int(ago // 86400)} d"

    lines = [
        dir_line,
        f"🧩 `{model}`" + (f"  🕐 {ago_str}" if ago_str else ""),
        f"💬 {title}",
    ]
    if file_size:
        lines.append(_context_bar(file_size))
    return "\n".join(lines)


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


async def _safe_send(text: str, parse_mode: str | None = "MarkdownV2",
                     plain_fallback: str | None = None, **kwargs):
    """Send a message to the admin resiliently:
      - retries once on RetryAfter (flood control) and on transient NetworkError
      - on BadRequest (bad markdown) falls back to plain text
    Returns the sent Message, or None if it ultimately failed (always logged,
    never raised, so callers in a finally block don't lose their flow)."""
    for attempt in range(2):
        try:
            return await APP.bot.send_message(ADMIN_ID, text, parse_mode=parse_mode,
                                              **kwargs)
        except BadRequest:
            if plain_fallback is not None:
                try:
                    return await APP.bot.send_message(ADMIN_ID, plain_fallback,
                                                      **kwargs)
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"_safe_send plain fallback failed: {exc}")
                    return None
            logger.error("_safe_send BadRequest with no plain fallback")
            return None
        except RetryAfter as e:
            if attempt == 0:
                await asyncio.sleep(float(getattr(e, "retry_after", 1)) + 0.5)
                continue
            logger.error("_safe_send giving up after RetryAfter")
            return None
        except NetworkError as e:
            if attempt == 0:
                await asyncio.sleep(1.0)
                continue
            logger.error(f"_safe_send giving up after NetworkError: {e}")
            return None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"_safe_send unexpected error: {exc}")
            return None
    return None


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

    sess_label = (st.get("session_label") or "").replace("`", "'").replace("*", "·")
    label_line = f"💬 _{sess_label[:50]}_" if sess_label else ""
    lines = [
        f"{icon} *{labels.get(state, state.upper())}* | 📂 `{cwd_name}`",
        f"🧩 `{model}` | ⏱ `{elapsed}`",
    ]
    if label_line:
        lines.append(label_line)
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
    tok_in = st.get("tokens_input", 0)
    if tok_in:
        ctx_str, _ = _ctx_pct(tok_in)
        lines.append(ctx_str)
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
    except NetworkError:
        pass


async def _heartbeat(ctx: ContextTypes.DEFAULT_TYPE):
    for skey in list(STATUSES.keys()):
        await _update_status(skey, force=False)


def _ensure_heartbeat():
    if APP.job_queue and not APP.job_queue.get_jobs_by_name("hb"):
        APP.job_queue.run_repeating(_heartbeat, interval=STATUS_INTERVAL,
                                    first=STATUS_INTERVAL, name="hb")


def _start_status(skey: str, directory: str, msg_id: int, model: str,
                  session_label: str | None = None):
    STATUSES[skey] = {
        "msg_id": msg_id, "directory": directory, "model": model,
        "session_label": session_label,
        "state": "pending", "tool": None, "tools_seen": [], "files_edited": set(),
        "reasoning_text": None, "start_time": time.time(),
        "last_update_time": time.time(), "tokens_input": 0, "tokens_output": 0,
        "cost": 0.0,
    }
    _ensure_heartbeat()


# --------------------------------------------------------------------------- #
# Final reply
# --------------------------------------------------------------------------- #
async def _send_reply(skey: str, directory: str, st: dict, final: dict | None,
                      cancelled: bool = False, error_msg: str | None = None):
    cwd_name = Path(directory).name or "?"
    model = st.get("model") or "?"
    elapsed = _format_elapsed(time.time() - st.get("start_time", time.time()))
    cost = (final or {}).get("cost", 0.0)
    files = st.get("files_edited", set())

    # Outcome icon — never claim success when Claude errored or was cancelled.
    is_error = bool((final or {}).get("is_error"))
    subtype = (final or {}).get("subtype") or ""
    if cancelled:
        icon = "🛑"
    elif is_error or error_msg:
        icon = "❌"
    else:
        icon = "✅"

    session_id = _resume_for(skey)
    session_title = None
    if session_id:
        meta = db.get_session_meta(session_id)
        session_title = (meta or {}).get("title")

    tok_in = st.get("tokens_input", 0)
    header = f"{icon} `{cwd_name}` | 🧩 `{model}` | ⏱ `{elapsed}`"
    if tok_in:
        ctx_str, ctx_pct = _ctx_pct(tok_in)
        header += f" | {ctx_str}"
    else:
        ctx_pct = 0
    if cancelled:
        header += "\n🛑 _Cancelado por ti_"
    elif is_error:
        # Surface the CLI's structured error reason (e.g. error_max_turns).
        reason = {"error_max_turns": "límite de turnos alcanzado",
                  "error_during_execution": "error durante la ejecución"}.get(
                      subtype, subtype or "error")
        header += f"\n❌ _Claude terminó con error: {reason}_"
    elif error_msg:
        header += f"\n❌ _{error_msg[:200]}_"
    if session_title:
        truncated_title = session_title[:25] + ("..." if len(session_title) > 25 else "")
        header += f"\n📌 `{truncated_title}`"
    if ctx_pct >= 80:
        header += "\n⚠️ _Contexto casi lleno — considera abrir una sesión nueva_"
    if files:
        names = list(files)[:3]
        header += " 📝 " + ", ".join(f"`{f}`" for f in names)
        if len(files) > 3:
            header += f" +{len(files)-3}"

    text = (final or {}).get("text", "") or ""
    if not text:
        # No body — still confirm the outcome so the user always knows what happened.
        tail = "_Cancelado\\._" if cancelled else (
            "_Terminó con error\\._" if (is_error or error_msg) else "_Listo\\._")
        sent = await _safe_send(f"{md2tgv2.convert(header)}\n{tail}",
                                plain_fallback=f"{header}\n(sin texto)")
        if sent:
            _track_msg(sent.message_id, skey, directory)
        return

    if len(text) > MD_FILE_THRESHOLD:
        import io
        f = io.BytesIO(text.encode("utf-8"))
        f.name = "respuesta.md"
        for attempt in range(2):
            try:
                sent = await APP.bot.send_document(ADMIN_ID, document=f,
                                                   caption=md2tgv2.convert(header),
                                                   parse_mode="MarkdownV2")
                _track_msg(sent.message_id, skey, directory)
                return
            except RetryAfter as e:
                if attempt == 0:
                    await asyncio.sleep(float(getattr(e, "retry_after", 1)) + 0.5)
                    f.seek(0)
                    continue
                logger.error("send respuesta.md giving up after RetryAfter")
            except Exception as exc:  # noqa: BLE001
                logger.error(f"send respuesta.md failed: {exc}")
                break
        # Fallback: at least notify the user the body couldn't be delivered.
        await _safe_send(
            f"{md2tgv2.convert(header)}\n⚠️ _No se pudo enviar la respuesta completa "
            f"\\(ver logs\\)\\._",
            plain_fallback=f"{header}\n⚠️ No se pudo enviar la respuesta (ver logs).")
        return

    chunks = [text[i:i+3800] for i in range(0, len(text), 3800)]
    last = None
    for i, chunk in enumerate(chunks):
        body = (f"{md2tgv2.convert(header)}\n" if i == 0 else "") + md2tgv2.convert(chunk)
        plain = (f"{header}\n" if i == 0 else "") + chunk
        sent = await _safe_send(body, plain_fallback=plain)
        if sent:
            last = sent
    if last:
        _track_msg(last.message_id, skey, directory)
    else:
        # Every chunk failed to send → don't leave the user in the dark.
        await _safe_send("⚠️ _No se pudo enviar la respuesta \\(ver logs\\)\\._",
                         plain_fallback="⚠️ No se pudo enviar la respuesta (ver logs).")


async def _finish(skey: str, directory: str, final: dict | None,
                  cancelled: bool = False, error_msg: str | None = None):
    st = STATUSES.pop(skey, None)
    RUNNING.pop(skey, None)
    try:
        db.inflight_remove(skey)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"inflight_remove failed: {exc}")
    if APP.job_queue and not STATUSES:
        for j in APP.job_queue.get_jobs_by_name("hb"):
            j.schedule_removal()
    if st and st.get("msg_id"):
        await _delete_msg(APP.bot, st["msg_id"])
    if st:
        await _send_reply(skey, directory, st, final,
                          cancelled=cancelled, error_msg=error_msg)
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
    cancelled = False
    error_msg = None
    title = prompt.strip().replace("\n", " ")[:40]
    can_use_tool = _can_use_tool if PERMISSION_MODE != "bypassPermissions" else None

    async def _consume():
        nonlocal final, error_msg
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
                if st and not st.get("session_label"):
                    st["session_label"] = title
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
                error_msg = ev["message"]
                await _safe_send(f"❌ Error: {ev['message'][:1500]}", parse_mode=None)

    try:
        # Hard wall-clock cap so a hung CLI can't pin the session forever.
        await asyncio.wait_for(_consume(), timeout=TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning(f"task {skey} timed out after {TASK_TIMEOUT}s")
        error_msg = f"Tiempo límite agotado ({TASK_TIMEOUT}s) — tarea interrumpida."
        await _interrupt_client(skey)
    except asyncio.CancelledError:
        logger.info(f"task {skey} cancelled")
        cancelled = True
        # Swallow: we still want the finally to run _finish (drain queue, clean
        # up status). Re-raising would propagate Cancelled into the finally's
        # awaits and abort the drain.
    except Exception as exc:  # noqa: BLE001
        logger.error(f"run_task error: {exc}", exc_info=True)
        error_msg = f"Error inesperado: {exc}"
    finally:
        # Shield so a cancellation arriving during teardown can't abort the
        # final reply or the queue drain (otherwise queued prompts vanish).
        await asyncio.shield(
            _finish(skey, directory, final, cancelled=cancelled, error_msg=error_msg))


async def _dispatch(directory: str, skey: str, model: str, text: str):
    """Send a prompt: create the status message and launch the task (or queue)."""
    if skey in STATUSES:  # busy → queue
        QUEUES.setdefault(skey, deque()).append(
            {"text": text, "directory": directory, "model": model})
        pos = len(QUEUES[skey])
        await _safe_send(
            f"⏳ `{Path(directory).name}` ocupado. En cola (posición {pos}).",
            parse_mode="Markdown",
            plain_fallback=f"⏳ {Path(directory).name} ocupado. En cola (posición {pos}).")
        return

    # Reserve the slot synchronously before any await to prevent a second
    # message slipping through the STATUSES check during the Telegram round-trip.
    STATUSES[skey] = {"state": "reserving"}

    try:
        resume_sid = _resume_for(skey)
        sess_label = None
        if resume_sid:
            s = _find_session(resume_sid, directory)
            if s:
                sess_label = _session_label(s)

        sess_line = f"\n💬 `{sess_label[:40]}`" if sess_label else ""
        sent = await APP.bot.send_message(
            ADMIN_ID,
            f"⚪ *ESPERANDO* | 📂 `{Path(directory).name}`\n"
            f"🧩 `{model}` | ⏱ `00:00`{sess_line}\n"
            f"_Iniciando Claude Code…_\n\n_Pulsa_ /esc _para cancelar_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")]]))
    except Exception as exc:  # noqa: BLE001
        # Posting the status message failed (network/flood/etc.). Free the slot
        # so the session isn't wedged "busy" forever, and tell the user.
        STATUSES.pop(skey, None)
        logger.error(f"_dispatch failed to post status: {exc}", exc_info=True)
        await _safe_send(
            f"❌ No pude iniciar la tarea en `{Path(directory).name}` "
            f"\\(error de red\\)\\. Reintenta\\.",
            plain_fallback=f"❌ No pude iniciar la tarea en {Path(directory).name}. Reintenta.")
        return

    _start_status(skey, directory, sent.message_id, model, sess_label)
    _track_msg(sent.message_id, skey, directory)
    RUNNING[skey] = {"client": None, "directory": directory}
    # Record the in-flight task so a restart can detect it was interrupted.
    try:
        db.inflight_add(skey, directory, model, text, sent.message_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"inflight_add failed: {exc}")

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
    parts = q.data.split(":")
    if len(parts) < 3:
        await _expired(q)
        return
    _, pk, pg = parts
    val = _val(int(pk))
    if val == KEY_MISSING:
        await _expired(q)
        return
    path = Path(val)
    if path.is_file():
        path = path.parent
    txt, kbd = _folder_kbd(path, int(pg))
    await q.edit_message_text(txt, reply_markup=kbd, parse_mode="Markdown")


async def cb_mkdir(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    path = _val(int(q.data.split(":")[1]))
    if path == KEY_MISSING:
        await _expired(q)
        return
    MKDIR_PENDING.clear()
    MKDIR_PENDING.update({"path": path, "msg_id": q.message.message_id,
                          "ts": time.time()})
    await q.edit_message_text(f"📁 Nueva carpeta en `{Path(path).name}`\n\nEscribe el nombre:",
                              parse_mode="Markdown")


async def cb_os(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Folder chosen → existing sessions picker or model picker."""
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
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
    elif mode == "sessions":
        new_cb = f"newsess:{pk}"
        cur_sid = (db.get_active() or {}).get("claude_session_id")
        title = f"📂 `{Path(cwd).name}` — {len(sessions)} sesión(es)"
        if cur_sid:
            active_s = _find_session(cur_sid, cwd)
            if active_s:
                title += f"\n✅ {_session_label(active_s).replace('`', chr(39))[:50]}"
        sel = lambda sid: f"actsess:{_key(sid)}:{pk}"
        dele = lambda sid: f"delsess:{_key(sid)}:{pk}:s"  # :s → re-render in sessions mode
    else:  # activate (from /open)
        new_cb = f"newsess:{pk}"
        cur_sid = (db.get_active() or {}).get("claude_session_id")
        title = f"📂 `{Path(cwd).name}` — {len(sessions)} sesión(es)"
        if cur_sid:
            active_s = _find_session(cur_sid, cwd)
            if active_s:
                title += f"\n✅ {_session_label(active_s).replace('`', chr(39))[:50]}"
        sel = lambda sid: f"actsess:{_key(sid)}:{pk}"
        dele = lambda sid: f"delsess:{_key(sid)}:{pk}"
    btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=new_cb)]]
    for s in sessions[:10]:
        sid = s.session_id
        is_active = sid == cur_sid
        prefix = "✅ " if is_active else ""
        label = f"{prefix}{_session_label(s)[:26 if is_active else 28]}"
        btns.append([
            InlineKeyboardButton(label, callback_data=sel(sid)),
            InlineKeyboardButton("🗑", callback_data=dele(sid)),
        ])
    if mode in ("send", "sessions"):
        label_back = "🔙 Otro proyecto"
        cb_back = "sendback:" if mode == "send" else "sessback:"
        btns.append([InlineKeyboardButton(label_back, callback_data=cb_back)])
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
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    await _show_model_picker(q, cwd)


async def cb_setmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model chosen → either create a new active session (cwd given) or change /models."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    pk = int(parts[1])
    model = _val(int(parts[2]))
    if not model or model == KEY_MISSING:  # stale → would persist an empty model
        await _expired(q)
        return

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
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    KNOWN_SID.pop(_skey(cwd, None), None)  # force truly new session, not a resume
    db.set_active(cwd, None, model)  # new session, materializes on first prompt
    await q.edit_message_text(
        f"✅ Sesión nueva en `{Path(cwd).name}`\n🧩 `{model}`\n\nEnvía tu primer prompt.",
        parse_mode="Markdown")


async def cb_actsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0] or not vals[1]:
        await _expired(q)  # stale button → don't clobber the active pointer with ""
        return
    sid, cwd = vals
    meta = db.get_session_meta(sid)
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    db.set_active(cwd, sid, model)
    s = _find_session(sid, cwd)
    if s:
        await q.edit_message_text(f"✅ *Sesión activa*\n{_session_card(s, meta, cwd)}",
                                  parse_mode="Markdown")
    else:
        await q.edit_message_text(f"✅ Sesión activa\n📂 `{Path(cwd).name}`\n🧩 `{model}`",
                                  parse_mode="Markdown")


async def cb_delsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0]:
        await _expired(q)
        return
    sid, cwd = vals
    from_sessions = len(parts) > 3 and parts[3] == "s"
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
        await _show_session_picker(q, cwd, sessions,
                                   mode="sessions" if from_sessions else "activate")
    else:
        KNOWN_SID.pop(_skey(cwd, None), None)  # prevent stale resume of the deleted session
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
    active = db.get_active() or {}
    active_dir = active.get("directory", "")
    active_sid = active.get("claude_session_id", "")

    header = ""
    if active_dir and active_sid:
        s = _find_session(active_sid, active_dir)
        label = _session_label(s).replace("`", "'")[:40] if s else active_sid[:8]
        dir_name = Path(active_dir).name.replace("`", "'")
        header = f"✅ Activa: `{dir_name}` › {label}\n\n"

    btns = []
    for d in sorted(by_dir):
        mark = " ✅" if d == active_dir else ""
        btns.append([InlineKeyboardButton(
            f"📂 {Path(d).name}{mark} ({len(by_dir[d])})",
            callback_data=f"sesspick:{_key(d)}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text(
        f"{header}¿De qué proyecto?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(btns))


async def cb_sesspick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    sessions = _list_sessions(directory=cwd)
    if sessions:
        await _show_session_picker(q, cwd, sessions, mode="sessions")
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
    vals = _vals(int(q.data.split(":")[1]))
    if vals is None:
        await _expired(q)
        return
    cwd = vals[0]
    # Hard guard: never operate on an empty directory. _list_sessions("")
    # returns sessions for *every* project, so a stale/empty cwd here would
    # wipe unrelated projects' sessions.
    if not cwd:
        await _expired(q)
        return
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
        await _safe_send(md2tgv2.convert(c), plain_fallback=c)


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
    mode = _val(int(q.data.split(":")[1]))
    if not mode or mode == KEY_MISSING or mode not in PERM_MODES:
        await _expired(q)
        return
    PERMISSION_MODE = mode
    await q.edit_message_text(f"🔐 Modo de permisos: `{PERMISSION_MODE}`",
                              parse_mode="Markdown")


# --------------------------------------------------------------------------- #
# /esc
# --------------------------------------------------------------------------- #
async def _interrupt_client(skey: str) -> None:
    """Ask the running Claude client to interrupt (best-effort, never raises)."""
    entry = RUNNING.get(skey)
    if not entry:
        return
    client = entry.get("client")
    if client:
        try:
            await client.interrupt()
        except Exception:  # noqa: BLE001
            pass


async def _abort(skey: str) -> str:
    entry = RUNNING.get(skey)
    if not entry:
        return "⚠️ No hay tarea en curso."
    await _interrupt_client(skey)
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
    was = SEND_MODE["on"] or SEND_MODE["target"] is not None or SEND_MODE.get("oneshot", False)
    SEND_MODE.update({"on": False, "target": None, "pending_text": None,
                      "oneshot": False, "oneshot_pre": False})
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


@admin_only
async def cmd_send(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """One-shot send: pick a session, dispatch one message, restore prior mode."""
    SEND_MODE["oneshot"] = True
    SEND_MODE["oneshot_pre"] = SEND_MODE["on"]
    text_arg = " ".join(ctx.args) if ctx.args else None
    if text_arg:
        SEND_MODE["pending_text"] = text_arg
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        SEND_MODE["oneshot"] = False
        await update.message.reply_text("No hay sesiones todavía. Usa /open.")
        return
    btns = [[InlineKeyboardButton(f"📂 {Path(d).name} ({len(by_dir[d])})",
                                  callback_data=f"sendpick:{_key(d)}")]
            for d in sorted(by_dir)]
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text(
        "📤 *Envío único* — elige proyecto:",
        reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_sendpick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    sessions = _list_sessions(directory=cwd)
    await _show_session_picker(q, cwd, sessions, mode="send")


async def cb_sendback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Return to the project picker from the session picker (send mode)."""
    q = update.callback_query
    await q.answer()
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await q.edit_message_text("No hay sesiones todavía. Usa /open.")
        return
    btns = [[InlineKeyboardButton(f"📂 {Path(d).name} ({len(by_dir[d])})",
                                  callback_data=f"sendpick:{_key(d)}")]
            for d in sorted(by_dir)]
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await q.edit_message_text(
        "📤 Elige proyecto:", reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown")


async def cb_sessback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Return to the project picker from the session picker (/sessions mode)."""
    q = update.callback_query
    await q.answer()
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await q.edit_message_text("No hay sesiones todavía. Usa /open.")
        return
    active_dir = (db.get_active() or {}).get("directory", "")
    btns = []
    for d in sorted(by_dir):
        mark = " ✅" if d == active_dir else ""
        btns.append([InlineKeyboardButton(
            f"📂 {Path(d).name}{mark} ({len(by_dir[d])})",
            callback_data=f"sesspick:{_key(d)}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await q.edit_message_text(
        "¿De qué proyecto?", reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown")


async def _send_to_target(q, cwd: str, skey: str, model: str, label: str):
    """Route one message to the chosen destination. The target is transient:
    after dispatch it is cleared so the next clean message asks again."""
    oneshot = SEND_MODE.get("oneshot", False)
    oneshot_pre = SEND_MODE.get("oneshot_pre", False)

    pending = SEND_MODE.pop("pending_text", None)
    SEND_MODE["pending_text"] = None
    if pending:
        SEND_MODE["target"] = None
        if oneshot:
            SEND_MODE["oneshot"] = False
            SEND_MODE["on"] = oneshot_pre  # restore prior mode
            suffix = ("🔀 Sigues en multisesión · /exitmulti para salir"
                      if oneshot_pre else "🔙 Volviendo a sesión normal")
        else:
            suffix = "🔀 Sigues en multisesión · /exitmulti para salir"
        try:
            await q.edit_message_text(
                f"📤 Enviando a {label}…\n_{suffix}_", parse_mode="Markdown")
        except BadRequest:
            await q.edit_message_text(f"📤 Enviando…")
        await _dispatch(cwd, skey, model, pending)
    else:
        # No text yet — hold destination; handle_text dispatches on next message.
        target = {"skey": skey, "directory": cwd, "model": model}
        if oneshot:
            target["oneshot_pre"] = oneshot_pre  # carry flag for handle_text
            SEND_MODE["oneshot"] = False          # flag consumed, info in target
        SEND_MODE["target"] = target
        if oneshot:
            suffix = ("🔀 Multisesión activa" if oneshot_pre else "🔙 Vuelve a normal tras envío")
        else:
            suffix = "🔀 Sigues en multisesión"
        try:
            await q.edit_message_text(
                f"📤 Destino: {label}\n🧩 `{model}`\nEscribe el mensaje.\n_{suffix}_",
                parse_mode="Markdown")
        except BadRequest:
            await q.edit_message_text(f"📤 Destino seleccionado. Escribe el mensaje.")


async def cb_sendsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0] or not vals[1]:
        await _expired(q)
        return
    sid, cwd = vals
    meta = db.get_session_meta(sid)
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    s = _find_session(sid, cwd)
    sess_name = _session_label(s).replace("`", "'")[:35] if s else sid[:8]
    label = f"`{Path(cwd).name}` › {sess_name}"
    await _send_to_target(q, cwd, sid, model, label)


async def cb_sendnew(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """➕ Nueva sesión in send mode → pick a model for the new target session."""
    q = update.callback_query
    await q.answer()
    pk = int(q.data.split(":")[1])
    cwd = _val(pk)
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
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
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0] or not vals[1]:
        await _expired(q)
        return
    cwd, model = vals
    new_skey = _skey(cwd, None)
    KNOWN_SID.pop(new_skey, None)  # force truly new session, not a resume
    await _send_to_target(q, cwd, new_skey, model,
                          f"nueva sesión en `{Path(cwd).name}`")


async def cb_senddel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete a session from the send picker, then re-render it."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0]:
        await _expired(q)
        return
    sid, cwd = vals
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
    if not _clear_send_mode():
        await update.message.reply_text("No estabas en modo multisesión.")
        return
    active = db.get_active()
    if active and active.get("directory"):
        sid = active.get("claude_session_id")
        cwd_name = Path(active["directory"]).name
        model = active.get("model") or cc.DEFAULT_MODEL
        label = ""
        if sid:
            s = _find_session(sid, active["directory"])
            if s:
                label = f"\n💬 {_session_label(s).replace('`', chr(39))[:40]}"
        await update.message.reply_text(
            f"✅ Multisesión desactivada.\n"
            f"📂 `{cwd_name}` · 🧩 `{model}`{label}",
            parse_mode="Markdown")
    else:
        await update.message.reply_text(
            "✅ Multisesión desactivada. Sin sesión activa — usa /open.")


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
    # Sanitize: strip any directory components so a crafted name like "../x"
    # or "/etc/x" can't write outside the project folder.
    safe_name = Path(file_name).name or f"file_{int(time.time())}"
    save_path = Path(cwd) / safe_name
    # Anti-collision: never silently overwrite an existing file.
    if save_path.exists():
        stem, suffix = save_path.stem, save_path.suffix
        save_path = save_path.with_name(f"{stem}_{int(time.time())}{suffix}")
        safe_name = save_path.name
    try:
        tg = await ctx.bot.get_file(file_id)
        await tg.download_to_drive(save_path)
    except Exception as exc:  # noqa: BLE001
        await msg.reply_text(f"❌ Error al guardar: {exc}")
        return
    await msg.reply_text(f"✅ `{safe_name}` guardado en `{Path(cwd).name}`.",
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
        # Expire stale flows so a forgotten "new folder" prompt doesn't swallow
        # a later normal message as a folder name.
        if time.time() - MKDIR_PENDING.get("ts", 0) > PENDING_FLOW_TTL:
            MKDIR_PENDING.clear()
        else:
            parent = Path(MKDIR_PENDING["path"])
            # Sanitize: only a single path component, no traversal.
            name = Path(text.strip()).name
            msg_id = MKDIR_PENDING.get("msg_id")
            MKDIR_PENDING.clear()
            if not name:
                await update.message.reply_text("❌ Nombre de carpeta no válido.")
                return
            new_dir = parent / name
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
            await update.message.reply_text(f"✅ Carpeta `{name}` creada.", parse_mode="Markdown")
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
        SEND_MODE["target"] = None
        if "oneshot_pre" in t:
            SEND_MODE["on"] = t["oneshot_pre"]  # restore mode after one-shot send
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
    "/send — envío único a sesión específica (un tiro)\n"
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
        sid = active.get("claude_session_id")
        cwd = active["directory"]
        meta = db.get_session_meta(sid) if sid else None
        model = (meta or {}).get("model") or active.get("model") or cc.DEFAULT_MODEL
        s = _find_session(sid, cwd) if sid else None
        if s:
            head = (f"*Sesión activa*\n{_session_card(s, meta, cwd)}\n"
                    f"🔐 `{PERMISSION_MODE}`\n\n")
        else:
            label = "(nueva)" if not sid else sid[:12]
            head = (f"*Sesión activa*\n📂 `{Path(cwd).name}`\n"
                    f"🧩 `{model}` | 🔐 `{PERMISSION_MODE}`\n"
                    f"📦 `{label}`\n\n")
    else:
        head = f"⚠️ Sin sesión activa · 🔐 `{PERMISSION_MODE}`\n\n"
    await update.message.reply_text(head + HELP, parse_mode="Markdown")


@admin_only
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP, parse_mode="Markdown")


@admin_only
async def cmd_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    import subprocess
    service = "claude-bot.service"

    # Detect how the bot is running and pick the matching systemctl invocation.
    # Order: user unit (preferred — Claude's login lives in this user's
    # ~/.claude) → system unit via passwordless sudo. We probe with `cat`, which
    # finishes *before* anything kills us (a plain `restart` would get SIGTERM'd
    # mid-run inside our own cgroup and falsely report failure). `-n` on sudo so
    # it never blocks waiting for a password nobody can type from Telegram.
    def _unit_exists(cmd: list[str]) -> bool:
        try:
            return subprocess.run(cmd + ["cat", service],
                                  capture_output=True, text=True,
                                  timeout=10).returncode == 0
        except Exception:  # noqa: BLE001
            return False

    if _unit_exists(["systemctl", "--user"]):
        restart_cmd = ["systemd-run", "--user", "--collect",
                       "systemctl", "--user", "restart", service]
    elif _unit_exists(["sudo", "-n", "systemctl"]):
        # System unit reachable with passwordless sudo. Detach via a transient
        # *system* unit so the restart survives our own SIGTERM.
        restart_cmd = ["sudo", "-n", "systemd-run", "--collect",
                       "systemctl", "restart", service]
    else:
        await update.message.reply_text(
            "⚠️ No puedo reiniciar automáticamente.\n"
            "• Como servicio de usuario: `systemctl --user restart claude-bot`\n"
            "• Como servicio de sistema: `sudo systemctl restart claude-bot` "
            "(requiere sudo sin contraseña para hacerlo desde aquí)\n"
            "• A mano: `./run.sh`", parse_mode="Markdown")
        return

    msg = await update.message.reply_text("🔄 Reiniciando…")
    RESTART_FLAG.write_text(str(msg.message_id))
    # Fire-and-forget; the success message is shown by post_init via
    # RESTART_FLAG once we come back up.
    try:
        subprocess.Popen(restart_cmd)
    except Exception as exc:  # noqa: BLE001
        RESTART_FLAG.unlink(missing_ok=True)
        await _safe_send(f"❌ No pude lanzar el reinicio: `{exc}`",
                         plain_fallback=f"❌ No pude lanzar el reinicio: {exc}")


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    """Global error handler: nothing should fail silently. Log with traceback
    and tell the admin what blew up (so a stuck 'loading' button always gets a
    follow-up message)."""
    err = ctx.error
    if isinstance(err, asyncio.CancelledError):
        return
    logger.error("Unhandled error in handler", exc_info=err)
    # Surface a short, safe summary to the admin. Never raise from here.
    try:
        where = ""
        if isinstance(update, Update):
            if update.callback_query and update.callback_query.data:
                where = f" (botón `{update.callback_query.data}`)"
            elif update.effective_message and update.effective_message.text:
                where = f" (mensaje «{update.effective_message.text[:40]}»)"
        msg = f"⚠️ Fallo interno{where}:\n`{type(err).__name__}: {str(err)[:300]}`"
        await APP.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
    except Exception:  # noqa: BLE001
        try:
            await APP.bot.send_message(ADMIN_ID, "⚠️ Fallo interno (ver logs).")
        except Exception:  # noqa: BLE001
            pass


def main():
    global APP, KEYSTORE
    db.init()
    KEYSTORE = db.load_keystore()  # restore so pre-restart buttons still resolve
    cc.set_question_bridge(_question_bridge)

    asyncio.set_event_loop(asyncio.new_event_loop())  # Python 3.14 fix
    app = Application.builder().token(TOKEN).build()
    APP = app

    app.add_error_handler(on_error)

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
    app.add_handler(CommandHandler("send", cmd_send))
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
    app.add_handler(CallbackQueryHandler(cb_sendback, pattern=r"^sendback:"))
    app.add_handler(CallbackQueryHandler(cb_sessback, pattern=r"^sessback:"))
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
            BotCommand("send", "Envío único a sesión específica"),
            BotCommand("multisesion", "Preguntar destino en cada mensaje"),
            BotCommand("exitmulti", "Salir de multisesión"),
            BotCommand("close", "Cerrar proyecto"),
            BotCommand("esc", "Cancelar tarea"),
            BotCommand("restart", "Reiniciar bot"),
        ])

        # Orphan recovery: any in-flight task row means a prompt was running
        # when the process died (crash, redeploy, /restart). The Claude
        # subprocess is gone and its status message is frozen — clean it up and
        # tell the user, so nothing looks "stuck working" forever.
        try:
            orphans = db.inflight_all()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"inflight_all failed: {exc}")
            orphans = []
        if orphans:
            for o in orphans:
                if o.get("msg_id"):
                    await _delete_msg(application.bot, o["msg_id"])
            try:
                db.inflight_clear()
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"inflight_clear failed: {exc}")
            lines = ["⚠️ *El bot se reinició mientras trabajaba.*",
                     "Estas tareas se interrumpieron (no se perdió tu historial, "
                     "pero conviene revisarlas):"]
            for o in orphans[:10]:
                name = Path(o.get("directory", "")).name or "?"
                prm = (o.get("prompt") or "").replace("\n", " ")[:50]
                lines.append(f"• 📂 `{name}` — _{prm}_" if prm else f"• 📂 `{name}`")
            try:
                await application.bot.send_message(
                    ADMIN_ID, "\n".join(lines), parse_mode="Markdown")
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"orphan notice failed: {exc}")

        if RESTART_FLAG.exists():
            try:
                mid = int(RESTART_FLAG.read_text().strip())
                RESTART_FLAG.unlink(missing_ok=True)
                lines = ["✅ *Bot reiniciado*"]
                active = db.get_active()
                if active and active.get("claude_session_id"):
                    sid = active["claude_session_id"]
                    cwd = active["directory"]
                    meta = db.get_session_meta(sid)
                    s = _find_session(sid, cwd)
                    if s:
                        lines.append(f"\n*Sesión activa:*\n{_session_card(s, meta, cwd)}")
                    else:
                        model = (meta or {}).get("model") or cc.DEFAULT_MODEL
                        lines.append(f"\n📂 `{Path(cwd).name}` · 🧩 `{model}`")
                elif active:
                    cwd = active.get("directory", "")
                    model = active.get("model") or cc.DEFAULT_MODEL
                    lines.append(f"\n📂 `{Path(cwd).name}` · 🧩 `{model}` · (nueva sesión)")
                await application.bot.edit_message_text(
                    chat_id=ADMIN_ID, message_id=mid,
                    text="\n".join(lines), parse_mode="Markdown")
            except Exception:  # noqa: BLE001
                RESTART_FLAG.unlink(missing_ok=True)

    app.post_init = post_init
    logger.info("claude-bot starting (permission_mode=%s, workspace=%s)",
                PERMISSION_MODE, WORKSPACE)
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
