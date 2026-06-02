"""
Claude Agent SDK wrapper — the equivalent of opencode-bot's OpenCode client + SSE.

run() drives a ClaudeSDKClient in streaming mode and yields *normalized* events
that the Telegram bot consumes to render live status and the final reply:

    {"type": "client",  "client": <ClaudeSDKClient>}   # first, so the bot can interrupt()
    {"type": "session", "session_id": str}
    {"type": "text",    "text": str}
    {"type": "thinking","text": str}
    {"type": "tool",    "name": str, "input": dict}
    {"type": "usage",   "input": int, "output": int}
    {"type": "result",  "text": str, "cost": float, "input": int, "output": int,
                        "session_id": str, "is_error": bool, "subtype": str}
    {"type": "error",   "message": str}

The `ask_user` MCP tool lets Claude ask the operator a question through Telegram;
it blocks on a bridge callback the bot installs via set_question_bridge().
"""

import logging

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    AssistantMessage,
    SystemMessage,
    ResultMessage,
    TextBlock,
    ThinkingBlock,
    ToolUseBlock,
    tool,
    create_sdk_mcp_server,
)

logger = logging.getLogger(__name__)

# Tools that edit files — used to surface "files edited" in the live status.
EDIT_TOOLS = {"Write", "Edit", "MultiEdit", "NotebookEdit"}

# Claude model aliases offered by /models
MODELS = ["opus", "sonnet", "haiku"]
DEFAULT_MODEL = "sonnet"


# --------------------------------------------------------------------------- #
# ask_user MCP tool (the "question" tool equivalent)
# --------------------------------------------------------------------------- #
_QUESTION_BRIDGE = None  # async (question: str, options: str) -> str


def set_question_bridge(fn) -> None:
    global _QUESTION_BRIDGE
    _QUESTION_BRIDGE = fn


@tool(
    "ask_user",
    "Ask the human operator a question and wait for their answer. Use this when "
    "you need a decision or clarification before continuing. 'options' is an "
    "optional comma-separated list of suggested answers shown as buttons.",
    {"question": str, "options": str},
)
async def _ask_user(args: dict) -> dict:
    question = args.get("question", "")
    options = args.get("options", "")
    if _QUESTION_BRIDGE is None:
        return {"content": [{"type": "text", "text": "(no hay interfaz para preguntar)"}]}
    try:
        answer = await _QUESTION_BRIDGE(question, options)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"ask_user bridge error: {exc}")
        answer = ""
    return {"content": [{"type": "text", "text": answer or "(el usuario no respondió)"}]}


def build_mcp_server():
    return create_sdk_mcp_server(name="bot", version="1.0.0", tools=[_ask_user])


# Appended to Claude Code's default system prompt so it actually *uses* the
# ask_user tool. Without this, Claude silently makes its own choices and the
# operator never sees a question on Telegram.
ASK_USER_GUIDANCE = (
    "Estás operando a través de un bot de Telegram con un único operador humano. "
    "Cuando necesites una decisión, una aclaración, o haya una ambigüedad real que "
    "cambie el resultado (qué archivo, qué enfoque, datos que faltan, una acción "
    "destructiva o irreversible), DEBES preguntar usando la herramienta "
    "`mcp__bot__ask_user` y esperar la respuesta, en lugar de suponer o continuar a "
    "ciegas. Pasa la pregunta en `question` y, si aplica, una lista de respuestas "
    "sugeridas separadas por comas en `options`. No abuses: para tareas claras y sin "
    "ambigüedad, trabaja directamente sin preguntar. Responde siempre en español."
)


# --------------------------------------------------------------------------- #
# Event normalization
# --------------------------------------------------------------------------- #
def _usage_tokens(usage) -> tuple[int, int]:
    """Extract (input, output) token counts from an SDK usage object/dict."""
    if not usage:
        return 0, 0
    if isinstance(usage, dict):
        i = usage.get("input_tokens", 0) or 0
        o = usage.get("output_tokens", 0) or 0
        i += (usage.get("cache_read_input_tokens", 0) or 0)
        i += (usage.get("cache_creation_input_tokens", 0) or 0)
        return i, o
    return (getattr(usage, "input_tokens", 0) or 0,
            getattr(usage, "output_tokens", 0) or 0)


def _normalize(msg):
    if isinstance(msg, SystemMessage):
        if msg.subtype == "init":
            sid = (msg.data or {}).get("session_id")
            if sid:
                yield {"type": "session", "session_id": sid}
        return

    if isinstance(msg, AssistantMessage):
        i, o = _usage_tokens(getattr(msg, "usage", None))
        if i or o:
            yield {"type": "usage", "input": i, "output": o}
        for block in (msg.content or []):
            if isinstance(block, TextBlock):
                if block.text:
                    yield {"type": "text", "text": block.text}
            elif isinstance(block, ThinkingBlock):
                if block.thinking:
                    yield {"type": "thinking", "text": block.thinking}
            elif isinstance(block, ToolUseBlock):
                yield {"type": "tool", "name": block.name, "input": block.input or {}}
        return

    if isinstance(msg, ResultMessage):
        i, o = _usage_tokens(getattr(msg, "usage", None))
        yield {
            "type": "result",
            "text": msg.result or "",
            "cost": getattr(msg, "total_cost_usd", 0.0) or 0.0,
            "input": i,
            "output": o,
            "session_id": msg.session_id,
            "is_error": bool(getattr(msg, "is_error", False)),
            "subtype": getattr(msg, "subtype", ""),
        }
        return
    # StreamEvent / RateLimitEvent / UserMessage → ignored for status purposes


# --------------------------------------------------------------------------- #
# Main entry point
# --------------------------------------------------------------------------- #
async def run(prompt: str, cwd: str, model: str | None, resume_session_id: str | None,
              permission_mode: str, can_use_tool, mcp_server):
    """Async generator of normalized events for one prompt."""
    kwargs = dict(
        cwd=cwd,
        permission_mode=permission_mode,
        setting_sources=["user", "project", "local"],
    )
    if model:
        kwargs["model"] = model
    if resume_session_id:
        kwargs["resume"] = resume_session_id
    if mcp_server is not None:
        kwargs["mcp_servers"] = {"bot": mcp_server}
        kwargs["allowed_tools"] = ["mcp__bot__ask_user"]
        # Instruct Claude to actually ask via the tool when it needs a decision,
        # otherwise the operator never gets a question on Telegram.
        kwargs["system_prompt"] = {
            "type": "preset",
            "preset": "claude_code",
            "append": ASK_USER_GUIDANCE,
        }
    # The interactive permission callback only matters when not bypassing.
    if can_use_tool is not None and permission_mode != "bypassPermissions":
        kwargs["can_use_tool"] = can_use_tool

    options = ClaudeAgentOptions(**kwargs)
    client = ClaudeSDKClient(options=options)
    await client.connect()
    try:
        yield {"type": "client", "client": client}
        await client.query(prompt)
        async for msg in client.receive_response():
            for ev in _normalize(msg):
                yield ev
    except Exception as exc:  # noqa: BLE001
        logger.error(f"claude run error: {exc}", exc_info=True)
        yield {"type": "error", "message": str(exc)}
    finally:
        try:
            await client.disconnect()
        except Exception:  # noqa: BLE001
            pass
