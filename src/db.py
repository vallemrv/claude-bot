"""
SQLite persistence for claude-bot.

Claude persists the real conversation state on disk (~/.claude/projects/...),
so we only track:
  - the active session pointer (directory + claude_session_id + model)
  - per-session model preference, so reactivating a session restores its model

Session discovery (listing sessions per project) uses the Agent SDK's native
list_sessions(), so we don't duplicate that here.
"""

import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "bot.db"


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS active (
                id                INTEGER PRIMARY KEY CHECK (id = 1),
                directory         TEXT NOT NULL,
                claude_session_id TEXT,
                model             TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS session_meta (
                claude_session_id TEXT PRIMARY KEY,
                directory         TEXT NOT NULL,
                model             TEXT,
                title             TEXT,
                created_at        REAL
            )
        """)


# --------------------------------------------------------------------------- #
# Active session pointer
# --------------------------------------------------------------------------- #
def get_active() -> dict | None:
    """Return {directory, claude_session_id, model} or None."""
    with _conn() as con:
        row = con.execute(
            "SELECT directory, claude_session_id, model FROM active WHERE id = 1"
        ).fetchone()
        return dict(row) if row else None


def set_active(directory: str, claude_session_id: str | None, model: str | None) -> None:
    with _conn() as con:
        con.execute("""
            INSERT INTO active (id, directory, claude_session_id, model)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                directory = excluded.directory,
                claude_session_id = excluded.claude_session_id,
                model = excluded.model
        """, (directory, claude_session_id, model))


def update_active_session_id(claude_session_id: str) -> None:
    """Fill in the session id once Claude creates it on the first prompt."""
    with _conn() as con:
        con.execute(
            "UPDATE active SET claude_session_id = ? WHERE id = 1",
            (claude_session_id,),
        )


def clear_active() -> None:
    with _conn() as con:
        con.execute("DELETE FROM active WHERE id = 1")


# --------------------------------------------------------------------------- #
# Per-session metadata (model + title)
# --------------------------------------------------------------------------- #
def remember_session(claude_session_id: str, directory: str,
                     model: str | None, title: str | None) -> None:
    if not claude_session_id:
        return
    with _conn() as con:
        existing = con.execute(
            "SELECT title FROM session_meta WHERE claude_session_id = ?",
            (claude_session_id,),
        ).fetchone()
        # Keep the first title we recorded for this session.
        keep_title = (existing["title"] if existing and existing["title"] else title)
        con.execute("""
            INSERT INTO session_meta (claude_session_id, directory, model, title, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(claude_session_id) DO UPDATE SET
                directory = excluded.directory,
                model = excluded.model,
                title = ?
        """, (claude_session_id, directory, model, keep_title, time.time(), keep_title))


def get_session_meta(claude_session_id: str) -> dict | None:
    with _conn() as con:
        row = con.execute(
            "SELECT claude_session_id, directory, model, title FROM session_meta "
            "WHERE claude_session_id = ?",
            (claude_session_id,),
        ).fetchone()
        return dict(row) if row else None


def set_session_model(claude_session_id: str, model: str) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE session_meta SET model = ? WHERE claude_session_id = ?",
            (model, claude_session_id),
        )


def forget_session(claude_session_id: str) -> None:
    with _conn() as con:
        con.execute(
            "DELETE FROM session_meta WHERE claude_session_id = ?",
            (claude_session_id,),
        )
