# Informe de mejoras — claude-bot

> Mando a distancia de **Claude Code** por Telegram (Claude Agent SDK + python-telegram-bot).
> Revisión de las 3 capas (`telegram_bot.py`, `claude_client.py`, `db.py`) + utilidades
> (`md2tgv2.py`, `transcription.py`), contrastada con el **Claude Agent SDK** real
> (`v0.2.x`, Python 3.14). Varios hallazgos verificados empíricamente contra el SDK.

---

## ✅ Estado: IMPLEMENTADO (todas las prioridades #1–#10)

Resumen de los cambios aplicados y verificados con batería de pruebas:

| Área | Qué se hizo | Verificado |
|------|-------------|------------|
| **KEYSTORE persistente** | Nueva tabla `keystore` en SQLite; se carga al arrancar. Los botones de antes de un reinicio vuelven a resolver. `_val` ahora devuelve un centinela `KEY_MISSING` (≠ `""`). | ✓ boot test |
| **Guardas anti-huérfano** | `cb_closedir/actsess/delsess/senddel/setmodel/sendmodel/sendsess` (+ navegación) abortan con "menú caducado" si el callback no resuelve, en vez de operar sobre `""`. | ✓ no hay borrado masivo ni clobber del puntero |
| **Timeout real** | `_run_task` envuelve el stream en `asyncio.wait_for(TASK_TIMEOUT)`; al vencer hace `interrupt()`, limpia estado y avisa. | ✓ cleanup test |
| **Feedback de error** | `_send_reply` distingue `is_error`/`subtype`/cancelación → cabecera `✅`/`❌`/`🛑` con motivo. Ya no se muestra un error como éxito. | ✓ header tests |
| **Envío resiliente** | `_safe_send` reintenta ante `RetryAfter`/`NetworkError` y cae a texto plano ante `BadRequest`; si todo falla, avisa "no se pudo enviar (ver logs)". | ✓ fallback tests |
| **Error handler global** | `app.add_error_handler(on_error)`: ninguna excepción queda muda; el admin recibe un resumen. | ✓ registrado |
| **Dispatch a prueba de fallos** | Si falla el `send_message` inicial, se libera el slot `STATUSES` (no más sesión "ocupada" eterna) y se avisa. | ✓ |
| **Cola tras cancelar** | El drenado de cola va con `asyncio.shield` → un `/esc` ya no pierde los prompts encolados. | ✓ cancel→drain test |
| **Recuperación tras reinicio** | Nueva tabla `inflight`: al arrancar se detectan tareas interrumpidas, se borra su status fantasma y se avisa al usuario. | ✓ db test |
| **Saneo / TTL / SQLite** | `handle_file` sanea nombre (`Path().name`) + anti-colisión; flujos `mkdir`/pregunta caducan a los 5 min; SQLite con WAL + `busy_timeout`. | ✓ db + logic tests |

El detalle técnico de cada problema original se conserva abajo como referencia.

---

## Resumen ejecutivo (diagnóstico original)

El código está bien estructurado y la mayoría de llamadas al SDK ya van con `try/except`.
Pero hay **fallos críticos confirmados** que pueden provocar **pérdida de datos irreversible**
o **bloqueo permanente de una sesión**, además de dos debilidades de fondo que el usuario
pidió revisar expresamente:

1. **Feedback de errores incompleto**: hay situaciones de error que se le presentan al
   usuario *como éxito* (✅), y otras que solo quedan en el log y nunca llegan a Telegram.
2. **Tareas de larga duración mal cubiertas**: `TASK_TIMEOUT` no se aplica nunca, y un
   reinicio del bot durante una tarea larga deja el progreso perdido y el mensaje de estado
   "congelado" sin avisar.

---

## 🔎 Cómo funciona Claude Code aquí (para entender los riesgos)

- El SDK lanza el **CLI local `claude`** como **subproceso** y se comunica por un protocolo de
  control sobre stdin/stdout. `cc.run()` consume `client.receive_response()`, que produce
  mensajes hasta un `ResultMessage`.
- **Clave**: el docstring del propio SDK dice que `receive_response()` *"continúa
  indefinidamente si no llega un `ResultMessage`"*. No hay timeout interno.
- Los **errores del CLI** (p.ej. salida con código ≠ 0) se propagan como **excepción** dentro
  de `receive_response()`; el wrapper los convierte en `{"type": "error", ...}`. ✓
- Un **resultado de error "normal"** (max_turns, error_during_execution) llega como
  `ResultMessage` con `is_error=True` y un `subtype`; **no** lanza excepción. El bot lo recibe
  como evento `result` y lo trata igual que un éxito (ver F1).
- `list_sessions` / `delete_session` leen/borran ficheros JSONL en `~/.claude/projects/...`.
  Verificado: `list_sessions(directory="")` **devuelve TODAS las sesiones de TODOS los
  proyectos** (no filtra); `delete_session("", ...)` lanza `ValueError` (capturado).
- `STATUSES`, `RUNNING`, `QUEUES`, `KEYSTORE`, `MSG2SESS`, `PENDING_*` son **estado en memoria**;
  se pierden por completo en cada reinicio del proceso.

---

## 🔴 CRÍTICO

### C1 — `cb_closedir` tras reinicio borra sesiones de TODOS los proyectos (pérdida de datos)
`telegram_bot.py:814-831`

`KEYSTORE` (int→path/sid) es **solo memoria** y muere en cada reinicio, pero los botones inline
de mensajes antiguos **siguen siendo pulsables**. `run_polling(drop_pending_updates=True)`
descarta updates *atrasados*, no invalida botones ya entregados: al pulsarlos tras reiniciar,
el entero embebido ya no está en `KEYSTORE` y `_val()` devuelve `""`.

Verificado contra el SDK:

```
sdk.list_sessions(directory="")  →  16 sesiones de proyectos DISTINTOS
```

Secuencia del desastre: botón viejo de `/close` → `cwd = _val(...) = ""` →
`_list_sessions(directory="")` devuelve **todas las sesiones del workspace** → el bucle
`:820-826` ejecuta `sdk.delete_session(s.session_id, ...)` sobre cada una (con UUIDs reales y
válidos de otros proyectos) → **borrado masivo irreversible**. Cada `delete` va en
`try/except` silencioso (`:825-826`), así que ni se percibe.

### C2 — `cb_actsess` tras reinicio destruye el puntero de sesión activa
`telegram_bot.py:677-692`

Mismo origen. `sid` y `cwd` quedan `""`, y `db.set_active("", "", model)` (`:685`)
**sobrescribe el puntero activo** con directorio/sesión vacíos (`set_active` no valida;
`directory TEXT NOT NULL` acepta `""`). El bot muestra "✅ Sesión activa" como si fuese bien,
pero el siguiente mensaje cae en "❌ No hay sesión activa" (`:1430`). Persistente en SQLite.

### C3 — `TASK_TIMEOUT` definido pero NUNCA aplicado → tareas colgadas para siempre
`telegram_bot.py:48` (definido), sin uso. **(Tareas de larga duración)**

`_run_task` itera `cc.run()` sin `asyncio.wait_for` ni timeout alguno. Como
`receive_response()` "continúa indefinidamente si no llega `ResultMessage`", si el CLI se
cuelga o el subproceso queda zombie, la sesión queda **eternamente en `STATUSES`**, el
heartbeat sigue latiendo, y **todos los prompts siguientes se encolan para siempre**. La única
salida es `/esc` manual. `TASK_TIMEOUT=1800` da una falsa sensación de protección que no existe.

---

## 🟠 FEEDBACK DE ERRORES — qué ve (y qué NO ve) el usuario

> Respuesta directa a *"¿tenemos buen feedback de qué pasa y qué ha pasado si hay errores?"*:
> **parcial**. Funciona para errores duros del SDK, pero falla en estos puntos:

### F1 — Un error de Claude se muestra como ÉXITO ✅  *(alto)*
`telegram_bot.py:451-454` y `_send_reply:324`

El evento `result` trae `is_error` y `subtype` (`claude_client.py:145-146`), pero `_run_task`
los **descarta**: solo guarda `final = ev`. Luego `_send_reply` construye el header
**siempre** empezando por `✅` (`:324`), sin mirar `is_error`. Consecuencia: un
`error_max_turns`, `error_during_execution` o un resultado interrumpido se le presenta al
usuario como una respuesta correcta. **No sabe que ha fallado.**
→ *Arreglo*: si `final.get("is_error")`, usar cabecera `❌`/`⚠️` y mostrar el `subtype`.

### F2 — Errores que solo van al log, nunca a Telegram  *(medio)*
- `_send_reply` `:357-358` y `:371-372`: si falla el envío del documento `respuesta.md` o de un
  chunk, se hace `logger.error(...)` y **nada más**. El usuario se queda **sin la respuesta y
  sin aviso** (el status ya se borró). Especialmente grave en respuestas largas (las más
  costosas de regenerar).
- `_list_sessions` `:189-190`: `logger.warning` y devuelve `[]`. Un fallo de FS aparece como
  "no hay sesiones", indistinguible de la realidad.
- `_delete_msg` `:201-202`, `cb_closedir` `:825-826`, `/btw` `:920-921`, `_abort` `:974-975`:
  `except: pass` totalmente mudos.

### F3 — Sin `add_error_handler` global → botón "cargando" eterno  *(alto)*
`telegram_bot.py:main()` (`:1516-1614`)

No hay manejador de errores de PTB. Cualquier excepción no capturada en un handler (p.ej. los
`IndexError`/`ValueError` de M1) **solo se loguea**; el usuario ve el spinner del botón sin
ningún mensaje. Un único `app.add_error_handler(...)` que avise al admin con un resumen del
fallo cubre toda esta clase de errores de golpe. **Es la mejora de feedback más rentable.**

### F4 — `_send_reply` no captura `RetryAfter`/`NetworkError` en el caso normal  *(alto)*
`telegram_bot.py:361-374`

El path de chunks captura `BadRequest` y reintenta en texto plano, pero **no** `RetryAfter`
(flood-control) ni `NetworkError`. Si Telegram limita justo al enviar la respuesta final, la
excepción sube al `finally` de `_run_task`; la respuesta se pierde **sin reintento y sin
aviso**.

### F5 — La cancelación (`/esc`) no se confirma en el mensaje final  *(medio)*
`telegram_bot.py:457-464`

Al cancelar, `_finish` borra el status y `_send_reply` manda… el `final` que hubiera (o
"_Listo._" vacío). El usuario ve un cierre ambiguo en vez de un claro "🛑 Cancelado por ti".
No queda registro de *qué* pasó.

### F6 — El log local es el único registro histórico  *(bajo)*
No hay traza persistente de "qué tareas se ejecutaron / fallaron". `bot.log` existe pero el
usuario, desde el móvil, no lo ve. Para "saber qué ha pasado" conviene, al menos, que **todos**
los errores relevantes terminen en un mensaje a Telegram (idealmente con el `skey`/proyecto).

---

## ⏱️ TAREAS DE LARGA DURACIÓN — análisis específico

> Respuesta directa a *"¿has tenido en cuenta que las tareas pueden durar mucho tiempo?"*:
> el diseño asíncrono lo soporta, pero faltan salvaguardas para los casos límite.

### L1 — (= C3) Sin timeout: una tarea atascada bloquea la sesión indefinidamente
Ya descrito arriba. Es el problema número uno de las tareas largas. Recomendado: envolver el
consumo del stream en `asyncio.wait_for(..., TASK_TIMEOUT)` y, al vencer, hacer `interrupt()` +
limpiar estado + avisar "⏱️ Tarea cancelada por exceder el tiempo límite".

### L2 — Reinicio durante una tarea larga = progreso perdido + status fantasma  *(alto)*
`STATUSES`/`RUNNING` son memoria; el subproceso `claude` es hijo del bot.

Si el bot reinicia mientras Claude trabaja (vía `/restart`, redeploy de systemd, o crash):
- El **subproceso `claude` muere** con el bot → **se pierde el trabajo en curso** (lo que no
  hubiera persistido Claude en su JSONL).
- El **mensaje de estado** "🔴 TRABAJANDO…" queda **congelado para siempre**: nadie lo borra
  ni lo actualiza, porque el `STATUSES` que lo referenciaba ya no existe. El usuario cree que
  sigue trabajando.
- Las **colas** (`QUEUES`) se evaporan sin ejecutarse.

→ *Mejora*: registrar en SQLite las tareas "en vuelo" y, en `post_init`, detectar las que
quedaron a medias para **avisar** ("⚠️ El bot se reinició mientras trabajaba en `proyecto`; la
tarea se interrumpió") y editar/borrar el status huérfano.

### L3 — Límite de edición de mensajes de Telegram (48 h)  *(bajo, pero real)*
El status se mantiene vía `edit_message_text`. Telegram **no permite editar mensajes de más de
48 h**. Una tarea (o sesión con status) que supere ese tiempo empezaría a fallar el `edit` con
`BadRequest` (hoy se traga como `pass`, `:275-276`), congelando el status. Improbable para una
sola tarea, pero posible con sesiones muy longevas.

### L4 — Heartbeat: el contador de tiempo es lo único que cambia  *(bajo)*
`_heartbeat` corre cada `STATUS_INTERVAL` (10 s) y refresca el status. En tareas largas con
poca actividad, el cuerpo del mensaje a menudo es idéntico salvo el cronómetro → Telegram
responde `message is not modified` (se ignora). Funciona, pero conviene asegurar que el
elapsed cambie siempre el texto para que el usuario vea que "sigue vivo". Hoy lo hace porque el
`⏱` se recalcula, así que está OK; solo vigilar si se reordena el render.

### L5 — `/esc` puede no drenar la cola tras cancelar  *(alto)*
`telegram_bot.py:457-464` + `377-387`

`_run_task` captura `CancelledError` y hace `raise`, pero el `finally` ejecuta
`await _finish(...)`, que contiene `await`s (`_send_reply`, `_drain_queue`). En una task
cancelada, el primer `await` del `finally` puede volver a propagar `CancelledError` y **abortar
`_drain_queue`**. Si había prompts encolados para esa sesión larga, **quedan sin ejecutar y sin
aviso**. → Blindar el drenado con `asyncio.shield` o sacarlo del path de cancelación.

---

## 🟠 ALTO (otros)

### A1 — `_dispatch`: si falla el `send_message` inicial, la sesión queda "ocupada" para siempre
`telegram_bot.py:479-504`

Se reserva el slot antes del await (`STATUSES[skey] = {"state": "reserving"}`, `:481`). Si el
`send_message` de `:491` lanza (`NetworkError`, `RetryAfter`, `BadRequest`), la función sale por
la excepción y **nunca limpia `STATUSES[skey]`** ni crea la task → esa sesión queda bloqueada
(todo se encola) sin tarea que la libere. Falta `try/except` que haga `STATUSES.pop(skey)` ante
fallo.

---

## 🟡 MEDIO

### M1 — `IndexError`/`ValueError` por `split(":")` con desempaquetado estricto
Sin guarda de número de campos (combinado con F3 → botón colgado mudo):
- `cb_ob` `:555` — `_, pk, pg = q.data.split(":")` (estricto a 3)
- `cb_perm_answer` `:1038`, `cb_q_answer` `:1067` — estricto a 3
- `cb_setmodel` `:653-655`, `cb_sendsess` `:1225-1227`, `cb_sendmodel` `:1254-1256` —
  `parts[2]` sin comprobar `len`

### M2 — `cb_setmodel` (rama `/models`) corrompe el modelo a `""` tras reinicio
`telegram_bot.py:649-667` (`:662`, `:664`)

Con `pk == -1` (sobrevive) pero `model = _val(...) = ""`, persiste modelo vacío en `active` y
`session_meta` mostrando "✅ Modelo `` aplicado". El `or DEFAULT_MODEL` lo rescata en runtime,
pero el estado guardado queda corrupto y `/models` muestra "Modelo actual: ``".

### M3 — Estados de flujo sin caducidad colisionan en `handle_text`
`telegram_bot.py:1377-1409`

`MKDIR_PENDING` y `ctx.bot_data["q_custom_qid"]` no caducan. Si el usuario abandona un flujo
(abrió "Nueva carpeta" y luego escribe un prompt normal), su mensaje se interpreta como nombre
de carpeta o respuesta a una pregunta ya muerta. Añadir TTL o botón de cancelar que limpie los
flags.

### M4 — Descarga de archivos: ruta no saneada / colisiones
`telegram_bot.py:1324-1332`

`save_path = Path(cwd) / file_name` con el `file_name` que envía Telegram. Un nombre tipo
`../x` o absoluto escaparía del proyecto; nombres repetidos se sobrescriben sin avisar. Sanear
con `Path(file_name).name` + detectar colisión.

---

## 🟢 BAJO / robustez

- **B1** — `main()` sin try/except global: un fallo al arrancar (token inválido, red en
  `set_my_commands`) tumba el proceso; systemd reinicia en bucle. Loguear + backoff.
- **B2** — `md2tgv2.convert`: cortar por longitud (`text[i:i+3800]`) puede partir entidades
  MarkdownV2 → `BadRequest`. Hay fallback a texto plano en `_send_reply`, pero **no** en `/btw`
  (`:927-934`) para chunks 2+.
- **B3** — `KEYSTORE` crece sin límite y `_key` hace búsqueda lineal O(n); en sesiones largas
  degrada y consume memoria. Usar índice inverso str→int y/o LRU.
- **B4** — SQLite: cada operación abre conexión sin `timeout`; con cola + callbacks
  concurrentes puede saltar `database is locked`. Añadir `timeout=` y `PRAGMA journal_mode=WAL`.
- **B5** — `_heartbeat`/`_update_status` solo capturan `BadRequest`/`RetryAfter`/`NetworkError`;
  un error inesperado rompería el heartbeat en silencio.

---

## ✅ Recomendaciones priorizadas

| # | Acción | Resuelve | Esfuerzo |
|---|--------|----------|----------|
| 1 | **Persistir `KEYSTORE`** (o validar `_val` y rechazar callbacks huérfanos: "menú caducado, reabre /open") | C1, C2, M2 | Medio |
| 2 | Guardas `if cwd=="" or sid=="": abortar` en `cb_closedir`/`cb_actsess`/`cb_delsess`/`cb_senddel`/`cb_setmodel` | C1, C2 (ya) | Bajo |
| 3 | `asyncio.wait_for(stream, TASK_TIMEOUT)` en `_run_task` + interrupt + aviso | C3 / L1 | Bajo |
| 4 | **Distinguir `is_error` en `_send_reply`** (cabecera ❌/⚠️ + `subtype`) | F1 | Bajo |
| 5 | Registrar `app.add_error_handler(...)` que avise al admin | F3, M1, B5 | Bajo |
| 6 | Capturar `RetryAfter`/`NetworkError` con reintento en `_send_reply` y avisar si se pierde la respuesta | F2, F4 | Bajo |
| 7 | Envolver `_dispatch` (tras reservar slot) en `try/except` → `STATUSES.pop` ante fallo | A1 | Bajo |
| 8 | Drenar cola fuera del path de cancelación (o `shield`); mensaje claro "🛑 Cancelado" | L5, F5 | Bajo |
| 9 | Detectar tareas huérfanas tras reinicio (SQLite + `post_init`): avisar y limpiar status fantasma | L2 | Medio |
| 10 | `Path(file_name).name` + anti-colisión; TTL en `MKDIR_PENDING`/`q_custom_qid`; SQLite WAL+timeout | M3, M4, B4 | Bajo |

### Orden sugerido de ataque
1. **Bloque #2–#8** (todo esfuerzo bajo): neutraliza los 3 críticos, arregla el feedback de
   errores (F1–F5) y cubre las tareas largas (timeout + cancelación). Máximo retorno.
2. **#1 y #9** (esfuerzo medio): persistencia de `KEYSTORE` y recuperación tras reinicio,
   que cierran los modos de fallo más peligrosos (silenciosos y destructivos) y mejoran el
   "saber qué ha pasado".
