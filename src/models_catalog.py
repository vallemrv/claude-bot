"""
Dynamic model catalog — single source of truth for the picker.

Calls Anthropic's /v1/models with the OAuth bearer the Claude Code CLI already
keeps in ~/.claude/.credentials.json (no extra config, no extra API key, no
extra payment). For each family (opus, sonnet, haiku, fable) keeps the most
recently created entry, so the picker always reflects what your current plan
actually serves.

Public surface mirrors the old claude_client constants:
  MODELS, DEFAULT_MODEL, MODEL_LABELS, CONTEXT_WINDOWS, DEFAULT_CONTEXT_WINDOW,
  cli_model(), context_window(), refresh().
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from pathlib import Path

import db

logger = logging.getLogger(__name__)

CREDS_PATH = Path.home() / ".claude" / ".credentials.json"
GLOBAL_CFG = Path.home() / ".claude.json"

# How long to trust a cached catalog before re-fetching at startup.
CACHE_TTL_SECONDS = 24 * 3600

# Families we offer in the picker. The order here drives the picker order.
FAMILIES = ("fable", "opus", "sonnet", "haiku")

# Friendly bot-side alias per family (what we store in DB and show in labels).
FAMILY_ALIAS = {
    "fable":  "fable-5",
    "opus":   "opus",
    "sonnet": "sonnet",
    "haiku":  "haiku",
}

# Decorative suffix per family for the picker label.
FAMILY_DECOR = {"fable": " ✨"}

# Snapshot — used only as a last-resort fallback if the API is unreachable AND
# the DB cache is empty. Kept tiny on purpose: we trust the live catalog.
_FALLBACK = [
    {"family": "fable",  "api_id": "claude-fable-5",    "display": "Claude Fable 5",   "ctx": 1_000_000},
    {"family": "opus",   "api_id": "claude-opus-4-8",   "display": "Claude Opus 4.8",  "ctx": 1_000_000},
    {"family": "sonnet", "api_id": "claude-sonnet-4-6", "display": "Claude Sonnet 4.6","ctx": 500_000},
    {"family": "haiku",  "api_id": "claude-haiku-4-5",  "display": "Claude Haiku 4.5", "ctx": 200_000},
]

DEFAULT_MODEL = "sonnet"
DEFAULT_CONTEXT_WINDOW = 200_000

# Populated by _rebuild_from_entries().
MODELS: list[str] = []
MODEL_LABELS: dict[str, str] = {}
CONTEXT_WINDOWS: dict[str, int] = {}
_CLI_BY_ALIAS: dict[str, str] = {}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _family_of(api_id: str) -> str | None:
    for fam in FAMILIES:
        if fam in api_id:
            return fam
    return None


def _label(display: str, ctx: int, family: str) -> str:
    # "Claude Opus 4.8" → "Opus 4.8 (1M) ✨"
    short = display.removeprefix("Claude ").strip()
    if ctx >= 1_000_000:
        tag = " (1M)"
    elif ctx >= 500_000:
        tag = f" ({ctx // 1000}K)"
    else:
        tag = ""
    return f"{short}{tag}{FAMILY_DECOR.get(family, '')}"


def _extra_usage_enabled() -> bool:
    """Mirrors the CLI's Tp7() gate: extra usage is on when cachedExtraUsageDisabledReason is null."""
    try:
        cfg = json.loads(GLOBAL_CFG.read_text())
    except Exception:  # noqa: BLE001
        return False
    return cfg.get("cachedExtraUsageDisabledReason", "missing") is None


def _adjust_ctx(family: str, api_ctx: int) -> int:
    """
    The /v1/models endpoint reports Sonnet's max_input_tokens as 1M, but the CLI
    gates the 1M window behind "Extra usage" (paid). When that gate is closed,
    Sonnet's *effective* window on a plain Max plan is 500K. Honor that so the
    ctx% indicator doesn't lie.
    """
    if family == "sonnet" and api_ctx >= 1_000_000 and not _extra_usage_enabled():
        return 500_000
    return api_ctx


def _rebuild_from_entries(entries: list[dict]) -> None:
    """Replace the module-level MODELS/MODEL_LABELS/CONTEXT_WINDOWS in-place."""
    by_family = {e["family"]: e for e in entries}
    new_models: list[str] = []
    new_labels: dict[str, str] = {}
    new_ctx: dict[str, int] = {}
    new_cli: dict[str, str] = {}
    for fam in FAMILIES:
        e = by_family.get(fam)
        if not e:
            continue
        alias = FAMILY_ALIAS[fam]
        ctx = _adjust_ctx(fam, int(e["ctx"]))
        new_models.append(alias)
        new_labels[alias] = _label(e["display"], ctx, fam)
        new_ctx[alias] = ctx
        new_cli[alias] = e["api_id"]

    MODELS.clear(); MODELS.extend(new_models)
    MODEL_LABELS.clear(); MODEL_LABELS.update(new_labels)
    CONTEXT_WINDOWS.clear(); CONTEXT_WINDOWS.update(new_ctx)
    _CLI_BY_ALIAS.clear(); _CLI_BY_ALIAS.update(new_cli)


# --------------------------------------------------------------------------- #
# Live fetch
# --------------------------------------------------------------------------- #
def _read_oauth_token() -> str | None:
    try:
        return json.loads(CREDS_PATH.read_text())["claudeAiOauth"]["accessToken"]
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"models_catalog: cannot read OAuth token: {exc}")
        return None


def _fetch_live() -> list[dict] | None:
    """Returns one entry per family (most recent created_at), or None on failure."""
    tok = _read_oauth_token()
    if not tok:
        return None
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/models?limit=1000",
        headers={"Authorization": f"Bearer {tok}", "anthropic-version": "2023-06-01"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
        logger.warning(f"models_catalog: /v1/models fetch failed: {exc}")
        return None

    by_family: dict[str, dict] = {}
    for m in data.get("data", []):
        fam = _family_of(m.get("id", ""))
        if not fam:
            continue
        created = m.get("created_at", "")
        ctx = m.get("max_input_tokens") or DEFAULT_CONTEXT_WINDOW
        cur = by_family.get(fam)
        if cur is None or created > cur["_created"]:
            by_family[fam] = {
                "family":  fam,
                "api_id":  m["id"],
                "display": m.get("display_name") or m["id"],
                "ctx":     ctx,
                "_created": created,
            }
    return [{k: v for k, v in e.items() if k != "_created"} for e in by_family.values()]


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def cli_model(alias: str | None) -> str | None:
    """Translate a bot-side alias into the model identifier the CLI expects."""
    if not alias:
        return alias
    if alias in _CLI_BY_ALIAS:
        return _CLI_BY_ALIAS[alias]
    # Legacy aliases stored in DB from previous bot versions ("opus-1m",
    # "opus-4-7", "sonnet-4-5", ...) → map to the current family head.
    fam = _family_of(alias)
    if fam and FAMILY_ALIAS[fam] in _CLI_BY_ALIAS:
        return _CLI_BY_ALIAS[FAMILY_ALIAS[fam]]
    return alias


def context_window(alias: str | None) -> int:
    """Effective input-token window for a bot alias."""
    if alias and alias in CONTEXT_WINDOWS:
        return CONTEXT_WINDOWS[alias]
    fam = _family_of(alias or "")
    if fam:
        return CONTEXT_WINDOWS.get(FAMILY_ALIAS[fam], DEFAULT_CONTEXT_WINDOW)
    return DEFAULT_CONTEXT_WINDOW


def refresh(force: bool = False) -> bool:
    """
    Try to refresh the catalog from /v1/models. Returns True if we ended up
    with a fresh (or still-valid cached) live catalog, False if we had to fall
    back to the snapshot. Safe to call repeatedly; the bot calls it once at
    startup.
    """
    cached = db.catalog_get("models")
    age = time.time() - (cached["updated_at"] if cached else 0)

    if cached and not force and age < CACHE_TTL_SECONDS:
        try:
            _rebuild_from_entries(json.loads(cached["value"]))
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"models_catalog: cached value unusable, refetching: {exc}")

    live = _fetch_live()
    if live:
        _rebuild_from_entries(live)
        try:
            db.catalog_set("models", json.dumps(live))
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"models_catalog: cache write failed: {exc}")
        return True

    if cached:
        try:
            _rebuild_from_entries(json.loads(cached["value"]))
            logger.info("models_catalog: live fetch failed, using stale cache")
            return True
        except Exception:  # noqa: BLE001
            pass

    _rebuild_from_entries(_FALLBACK)
    logger.warning("models_catalog: using hardcoded fallback (no live fetch, no cache)")
    return False


# Populate on import with whatever we already have on disk, so the first
# picker render works even before the bot's post_init triggers a refresh.
# We deliberately do NOT hit the network here to keep import fast and offline-safe.
try:
    _cached = db.catalog_get("models")
    if _cached:
        _rebuild_from_entries(json.loads(_cached["value"]))
    else:
        _rebuild_from_entries(_FALLBACK)
except Exception as exc:  # noqa: BLE001
    logger.warning(f"models_catalog: initial load failed, using fallback: {exc}")
    _rebuild_from_entries(_FALLBACK)
