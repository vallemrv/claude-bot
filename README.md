# claude-bot 🤖

Control remoto de **Claude Code por Telegram** — clon de `opencode-bot` adaptado a
Claude. Habla con Claude desde el móvil, gestiona múltiples proyectos y sesiones, ve
el progreso en vivo y recibe el resultado al terminar.

Usa el **Claude Agent SDK** por debajo (mismo login de Claude Code, sin API key aparte):
streaming en vivo, sesiones reanudables, permisos inline, tool de preguntas, interrupción.

## Arquitectura

```
Tú (Telegram) ──prompt──▶ telegram_bot.py ──▶ claude_client (Agent SDK)
                              ▲                      │ streaming
                              └── status en vivo ◀───┘ (texto, tools, tokens, coste)
```

| Módulo | Función |
|---|---|
| `src/telegram_bot.py` | Bot: comandos, status en vivo, permisos, preguntas, send mode, uploads, audio |
| `src/claude_client.py` | Envoltura del Agent SDK: `run()` → eventos normalizados; tool MCP `ask_user` |
| `src/db.py` | SQLite: sesión activa + modelo por sesión (la conversación la guarda Claude en disco) |
| `src/md2tgv2.py` | Markdown → Telegram MarkdownV2 |
| `src/transcription.py` | Transcripción de voz (X.AI Grok STT) |

## Comandos

| Comando | Descripción |
|---|---|
| `/start` | Estado: sesión activa, proyecto, modelo, permisos |
| `/open` | Browser de carpetas → abrir proyecto / crear o elegir sesión |
| `/sessions` | Gestionar sesiones de un proyecto (activar / borrar) |
| `/projects` | Proyectos con sesiones |
| `/models` | Cambiar modelo (opus / sonnet / haiku) |
| `/permisos` | Modo de permisos (bypass / acceptEdits / default / plan) |
| `/send` · `/endsend` | Enviar a otro proyecto sin cambiar la sesión activa |
| `/close` | Borrar todas las sesiones de un proyecto |
| `/esc` | Cancelar la tarea en curso |
| `/restart` | Reiniciar el bot (si está como servicio systemd) |

**Conversaciones paralelas:** responde (reply) a un mensaje del bot para continuar esa
sesión concreta; varios proyectos pueden trabajar a la vez. Envía **audio** para dictar
y **archivos** para guardarlos en el proyecto activo.

## Puesta en marcha

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env   # rellena TELEGRAM_BOT_TOKEN y TELEGRAM_ADMIN_ID
./run.sh
```

Requisitos: `claude` CLI con sesión iniciada, Python 3.11+. Para audio: `XAI_API_KEY`.

## Dejarlo siempre activo

Ver `claude-bot.service.example` (servicio systemd de usuario).

## 🔒 Seguridad

Solo responde a `TELEGRAM_ADMIN_ID`. Con `PERMISSION_MODE=bypassPermissions` Claude
ejecuta todo sin preguntar — úsalo solo en tu máquina. Cambia a `default` con `/permisos`
para que te pida confirmación (botones) en cada acción.
