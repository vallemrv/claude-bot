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
.venv/bin/python -m py_compile src/*.py
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

Three core layers, each requiring the others for context:

- **`src/telegram_bot.py`** — all Telegram I/O, commands, callbacks, live status,
  message routing. Holds the app together via module-level global dicts (single
  admin → globals are fine).
- **`src/claude_client.py`** — thin Agent SDK wrapper. `run()` is an async
  generator yielding **normalized events** (`session`/`text`/`thinking`/`tool`/
  `usage`/`result`/`error`) that the bot renders. Also defines the `ask_user` MCP
  tool and `ask_side()` (see `/btw` below). Per-session **effort** (`/esfuerzo`,
  stored in `db` on both `active.effort` and `session_meta.effort`) is forwarded to
  the SDK as the `effort` kwarg; Haiku has no configurable effort.
- **`src/db.py`** — SQLite (`bot.db`). Stores the active-session pointer,
  per-session metadata (model, custom title, effort), a **persisted callback keystore**
  (`keystore` table — so inline buttons survive restarts) and an **in-flight task
  ledger** (`inflight` table — for orphan detection after a restart). The real
  conversation history is persisted on disk by Claude itself; session discovery
  uses the SDK's native `list_sessions()` / `resume=` / `delete_session()`. Do not
  try to duplicate conversation state here. Connections use WAL + `busy_timeout`.

Plus supporting modules that carry real architectural weight:

- **`src/models_catalog.py`** — the **single source of truth** for the model picker.
  Calls Anthropic's `/v1/models` reusing the OAuth bearer the `claude` CLI already
  keeps in `~/.claude/.credentials.json` (no extra API key, no extra payment), keeps
  the newest entry per family (opus/sonnet/haiku/fable), and **caches the result in
  `db`** so the picker reflects what the current plan actually serves. `claude_client`
  re-exports its surface (`MODELS`, `DEFAULT_MODEL`, `cli_model()`, …); `/refreshmodels`
  (and the refresh button in `/models`) call `refresh()`.
- **`src/gitops.py`** — sync, subprocess-only git helpers (no deps) backing `/undo`,
  `/redo`, `/status`. See the snapshot/undo gotcha below.
- **`src/md2tgv2.py`** (Markdown→MarkdownV2) and **`src/transcription.py`** (Grok
  voice-note STT) are leaf utilities.

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
as an `asyncio.Task`. If posting the status fails, the slot is freed (so the session
isn't wedged "busy"). `_run_task` consumes `cc.run()` events under an
`asyncio.wait_for(TASK_TIMEOUT)` cap (a hung CLI is interrupted, not left forever),
updates a live status message (throttled + a `job_queue` heartbeat). The status line
for the current tool includes its key argument — `_tool_arg()` picks the salient field
per tool (`_TOOL_ARG_FIELDS`: e.g. `command` for Bash, `file_path` for edits, made
repo-relative, truncated) so the user sees `🔧 Edit: src/foo.py`, not a bare tool name.
On completion `_finish()` deletes the status, sends the final reply, and drains the
queue. `_finish` runs under `asyncio.shield` inside the `finally`, so a cancellation
(`/esc`) can't abort the queue drain. The reply header reflects the real outcome:
`✅` success / `❌` error (`result.is_error` + `subtype`) / `🛑` cancelled. If a `skey`
is already in `STATUSES`, new prompts are **queued**, not run concurrently. Each
in-flight task is recorded in the `inflight` table; on startup `post_init` detects
rows left by a crash/restart, deletes the orphaned status messages and warns the user.

### Message routing (`handle_text`)

Resolution order for a plain text message: **reply-to a bot message** (via
`MSG2SESS`, continues that exact session) → **multisession target** → **active
session**. Replying to any bot message is how parallel conversations work.

### Callback data + KEYSTORE

Telegram limits `callback_data` to 64 bytes. `_key(str)→int` / `_val(int)→str`
(`KEYSTORE`) compress long values (paths, session ids, model names) into small ints
embedded in callbacks like `actsess:{k}:{pk}`. Always round-trip through these. The
`KEYSTORE` is **persisted** (`keystore` table) and reloaded at startup so pre-restart
buttons still resolve. `_val` returns the `KEY_MISSING` sentinel (not `""`) for an
unknown int; destructive callbacks must guard on it (and on empty cwd/sid) and call
`_expired(q)` instead of acting — otherwise a stale button could wipe unrelated
sessions (`list_sessions("")` returns **all** projects) or clobber the active pointer.
Use `_vals(*ints)` to resolve several at once (returns `None` if any is missing).

### Robustness helpers

- `_safe_send()` is the resilient sender: retries on `RetryAfter`/`NetworkError`,
  falls back to plain text on `BadRequest`, and never raises (safe in `finally`).
  Prefer it over `APP.bot.send_message` for user-facing replies/notices.
- `on_error` is the global PTB error handler — nothing fails silently; the admin
  gets a short summary so a stuck "loading" button always has a follow-up.

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
- **`/restart`**: auto-detects whether the bot runs as a **user** unit
  (preferred — Claude's login lives in this user's `~/.claude`) or a **system**
  unit, and uses the matching `systemctl`. It probes with `systemctl --user cat`
  first, then `sudo -n systemctl cat` (`-n` so it never blocks on a password
  nobody can type from Telegram); `cat` finishes before any kill, unlike a plain
  `restart` which would get SIGTERM'd inside its own cgroup → false "not found".
  The actual restart is launched **detached** via `systemd-run … --collect` so it
  survives our SIGTERM. If neither mode is reachable it explains the options
  instead of failing silently. **Before relaunching it auto-updates the checkout**
  (`_git_pull_and_deps`): `git pull --ff-only` (never merges or clobbers uncommitted
  work — if it can't fast-forward it reports and restarts on the current code), and
  if the pull touched `requirements.txt` it reinstalls deps with the venv python;
  a failed `pip install` **aborts** the restart so the bot never relaunches into a
  broken env. So `/restart` always comes back on the newest code.
- Final replies that don't fit in a single Telegram message (> `MD_FILE_THRESHOLD`,
  ~3500 chars to leave room for the header + MarkdownV2 escaping) are sent as a
  `respuesta.md` document instead of being split across messages. Shorter replies
  go inline. All markdown goes through `md2tgv2.convert()` → MarkdownV2, with a
  plain-text fallback on `BadRequest`.
- **`/undo` · `/redo` · `/status`** (`gitops.py`): before each task that edits files,
  `_run_task` captures a `pre_snapshot` of the working tree via `git commit-tree` on a
  **throwaway index** (`GIT_INDEX_FILE`) — it never touches the real HEAD, index, or
  history, and keeps the object alive under `refs/bot-snapshots/`. `_finish` pushes that
  snapshot onto `UNDO_STACK[directory]` (capped at 50; a new task clears `REDO_STACK`).
  `/undo` restores via `read-tree` + `checkout-index -f -a` + `clean -fd`, moving the
  state across `UNDO_STACK`/`REDO_STACK`. `/status` shows the diff summary with a
  "Commit y push" button → `commit_push()` (commit is the critical step; a push with no
  remote / no upstream still reports success since the work is safely committed).
  Each snapshot is pinned under `refs/bot-snapshots/` so GC can't collect it while it's
  reachable from a stack. Once a snapshot leaves every stack — a read-only task whose
  upfront snapshot was never used, a redo entry invalidated by a new edit, the state
  you just landed on via undo/redo, or an entry past the 50-cap — `_finish`/`_do_undo`/
  `cmd_redo` call `gitops.drop_snapshot()` to delete its keep-ref so its objects become
  collectable. Best-effort: dropping a ref never fails the operation.
- **`ask_user` options is an ARRAY** (`claude_client` tool schema), never a CSV string —
  each element is exactly one button, so commas inside an answer don't shatter it.
  `_question_bridge` normalizes (still accepts a legacy comma-string), de-dups, and
  numbers buttons; long options are spelled out in full in the message body.

## Config (`.env`)

`TELEGRAM_BOT_TOKEN`, `TELEGRAM_ADMIN_ID` (required), `DEFAULT_WORKSPACE` (root of the
`/open` browser), `PERMISSION_MODE` (default `bypassPermissions`), `TASK_TIMEOUT`,
`XAI_API_KEY` (optional, for Grok voice-note transcription).
