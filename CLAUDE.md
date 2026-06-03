# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-admin Telegram bot that drives **Claude Code** from your phone, via the
**Claude Agent SDK** (not the HTTP API — it reuses the local `claude` CLI login).
All user-facing text is **Spanish**; match that when adding messages/commands.

## Commands

```bash
# Setup
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env            # fill TELEGRAM_BOT_TOKEN + TELEGRAM_ADMIN_ID

# Run (foreground)
./run.sh                         # = .venv/bin/python src/telegram_bot.py

# Syntax check before deploying (there is NO test suite, NO linter configured)
.venv/bin/python -m py_compile src/telegram_bot.py src/claude_client.py src/db.py
```

The bot runs in production as a **systemd *user* service** (`claude-bot.service`,
under `~/.config/systemd/user/`). Manage it with `--user`; never `sudo` it (a user
unit is invisible to root's systemd):

```bash
systemctl --user restart claude-bot.service
journalctl --user -u claude-bot.service -f
```

To smoke-test SDK behavior against a real session, run a throwaway script with the
venv python from inside `src/` so imports resolve, e.g.
`cd src && ../.venv/bin/python - <<'PY' ... PY` (see git history for `ask_side` test).

## Architecture

Three layers, each requiring the others for context:

- **`src/telegram_bot.py`** — all Telegram I/O, commands, callbacks, live status,
  message routing. Holds the app together via module-level global dicts (single
  admin → globals are fine).
- **`src/claude_client.py`** — thin Agent SDK wrapper. `run()` is an async
  generator yielding **normalized events** (`session`/`text`/`thinking`/`tool`/
  `usage`/`result`/`error`) that the bot renders. Also defines the `ask_user` MCP
  tool and `ask_side()` (see `/btw` below).
- **`src/db.py`** — SQLite (`bot.db`). Stores **only** the active-session pointer and
  per-session metadata (model, custom title). The real conversation history is
  persisted on disk by Claude itself; session discovery uses the SDK's native
  `list_sessions()` / `resume=` / `delete_session()`. Do not try to duplicate
  conversation state here.

### Session model — the `skey` concept (central, easy to get wrong)

A session is identified by a **`skey`** string:
- `_skey(directory, claude_session_id)` returns the real `claude_session_id`, or
  `"new::{directory}"` for a session that hasn't sent its first prompt yet.
- A new session has **no** Claude session id until the first prompt materializes it;
  `KNOWN_SID[skey]` is filled in on the SDK `session` event, and `_resume_for(skey)`
  resolves the resume id. Until then, features needing context (rename, `/btw`)
  must guard on `active.get("claude_session_id")` being present.

All runtime state is keyed by `skey`: `STATUSES` (live status), `RUNNING`
(client+task for interruption), `QUEUES` (per-session prompt backlog when busy).

### Request lifecycle

`_dispatch()` reserves the `STATUSES[skey]` slot synchronously (before any await,
to avoid double-dispatch races), posts a status message, then launches `_run_task`
as an `asyncio.Task`. `_run_task` consumes `cc.run()` events, updates a live status
message (throttled + a `job_queue` heartbeat), and on completion `_finish()` deletes
the status, sends the final reply, and drains the queue. If a `skey` is already in
`STATUSES`, new prompts are **queued**, not run concurrently.

### Message routing (`handle_text`)

Resolution order for a plain text message: **reply-to a bot message** (via
`MSG2SESS`, continues that exact session) → **multisession target** → **active
session**. Replying to any bot message is how parallel conversations work.

### Callback data + KEYSTORE

Telegram limits `callback_data` to 64 bytes. `_key(str)→int` / `_val(int)→str`
(`KEYSTORE`) compress long values (paths, session ids, model names) into small ints
embedded in callbacks like `actsess:{k}:{pk}`. Always round-trip through these.

### Permission / question bridges

When `PERMISSION_MODE != bypassPermissions`, `_can_use_tool` posts inline
allow/deny buttons and blocks on an `asyncio.Future`. The `ask_user` MCP tool
(claude_client) routes Claude's questions to Telegram via `_question_bridge`. Both
are awaited Futures resolved by callback handlers.

## Notable behaviors / gotchas

- **`/btw`** (side question): `cc.ask_side()` **forks** the active session
  (`fork_session=True`, original history untouched), blocks all tools
  (`disallowed_tools`) so it answers from context only, then **deletes the throwaway
  fork** so it never clutters the session pickers.
- **`/multisesion` / `/exitmulti`**: an "always ask" mode — every clean (non-reply)
  message re-launches the project→session wizard; the chosen destination is
  **transient** (cleared after one dispatch), never sticky.
- **`/restart`**: must launch the restart **detached** via
  `systemd-run --user --collect systemctl --user restart claude-bot.service`. A plain
  `systemctl restart` from inside the bot lives in the service's own cgroup and gets
  SIGTERM'd mid-restart → false "service not found". Existence is checked first with
  `systemctl --user cat` (which finishes before any kill).
- Final replies > `MD_FILE_THRESHOLD` (~6000 chars) are sent as a `respuesta.md`
  document; otherwise chunked. All markdown goes through `md2tgv2.convert()` →
  MarkdownV2, with a plain-text fallback on `BadRequest`.

## Config (`.env`)

`TELEGRAM_BOT_TOKEN`, `TELEGRAM_ADMIN_ID` (required), `DEFAULT_WORKSPACE` (root of the
`/open` browser), `PERMISSION_MODE` (default `bypassPermissions`), `TASK_TIMEOUT`,
`XAI_API_KEY` (optional, for Grok voice-note transcription).
