# Plan de mejoras — claude-bot

Auditoría de redundancias, código huérfano, fallos de estado/mezcla de mensajes y
seguridad. Cada punto incluye ubicación y el arreglo concreto. Las correcciones se
ejecutan con subagentes (Sonnet) en fases **secuenciales** porque casi todas tocan
`src/telegram_bot.py` y en paralelo se pisarían.

---

## Fase 1 — Bugs reales (crítico)

### 1.1 `/rename` roto: `@admin_only` mal aplicado a `_apply_rename`
- **Dónde:** `src/telegram_bot.py:1270-1276` (definición), llamadas en `:1293` y `:2131`.
- **Problema:** `admin_only` envuelve la función en `wrapper(update, ctx)` (2 args),
  pero `_apply_rename` se llama con 5 args → `TypeError: wrapper() takes 2 positional
  arguments but 5 were given`. Verificado en runtime. Tanto `/rename <nombre>` como el
  flujo pendiente de rename fallan y caen en `on_error`.
- **Fix:** quitar el decorador `@admin_only` de `_apply_rename` (es un helper interno;
  sus llamadores — `cmd_rename` y `handle_text` — ya están protegidos).

### 1.2 Fuga ilimitada del keystore (estado huérfano + degradación O(n))
- **Dónde:** `_can_use_tool` (`:1672`) y `_question_bridge` (`:1734`) generan el `qid`
  con `_key(f"perm-…{time.time()}")` / `_key(f"q-…{time.time()}")`. `_key` (`:101`)
  **persiste en la tabla `keystore`** y hace **scan lineal** sobre todo `KEYSTORE`.
- **Problema:** cada permiso y cada pregunta `ask_user` escriben una fila única que
  **nunca se borra** → la tabla crece sin límite y `_key` se degrada. Esos qids son
  efímeros (se resuelven en memoria vía `PENDING_PERMS`/`PENDING_Q`), no deberían
  tocar el keystore persistente.
- **Fix:**
  1. Añadir un contador efímero en memoria para los qids de permiso/pregunta (p.ej.
     `_EPHEMERAL_QID = itertools.count()` o un simple int global), sin pasar por `_key`.
     Los handlers `cb_perm_answer`/`cb_q_answer`/`cb_q_custom` ya usan `int(qid_k)` como
     clave de los dicts `PENDING_*`, así que basta con que el qid sea un int único.
  2. Añadir un índice inverso `value -> k` (`KEYSTORE_REV: dict[str,int]`) para que
     `_key` sea O(1) en vez de escanear. Mantenerlo sincronizado al cargar el keystore
     en `main()` y en cada `_key` nuevo.

---

## Fase 2 — Redundancias, código muerto y seguridad de estado

### 2.1 Teclado de "picker de proyectos" duplicado (5 sitios)
- **Dónde:** `cmd_sessions` (`:1061`), `cmd_close` (`:1095`), `cmd_multisesion`
  (`:1808`), `cmd_send` (`:1833`), `cb_sendback` (`:1861`), `cb_sessback` (`:1879`).
- **Fix:** extraer un helper que construya la lista de botones de proyectos a partir de
  `_group_by_dir(...)`, parametrizando el patrón de `callback_data` y si marca el activo.
  Reemplazar las 5+ copias por llamadas al helper. Sin cambios de comportamiento.

### 2.2 Teclado de "picker de modelos" duplicado (4 sitios)
- **Dónde:** `cmd_models` (`:1146`), `cb_refreshmodels` (`:1176`), `_show_model_picker`
  (`:906`), `cb_sendnew` (`:1966`).
- **Fix:** helper `_model_buttons(cb_for_model, current=None)` que devuelva la lista de
  filas de botones; cada sitio aporta su builder de `callback_data`. Sin cambios de
  comportamiento.

### 2.3 Ramas `"sessions"`/`"activate"` casi idénticas en `_show_session_picker`
- **Dónde:** `:865-884`.
- **Fix:** fusionar ambas ramas; la única diferencia real es el sufijo `:s` en el
  `callback_data` de borrar (re-render en modo sessions). Unificar con un flag.

### 2.4 Validación de esfuerzo duplicada
- **Dónde:** `cb_seteffort` (`:1242`).
- **Fix:** eliminar el set `valid = {...}` hardcodeado; la comprobación por familia
  (`_EFFORT_LEVELS.get(fam, [])`) justo debajo ya lo cubre.

### 2.5 Asignación muerta
- **Dónde:** `:1218` — `levels = _effort_levels = _EFFORT_LEVELS.get(...)`.
- **Fix:** quitar `_effort_levels` (no se usa).

### 2.6 `KNOWN_SID` crece sin purgarse
- **Dónde:** se llena en `_run_task` (`:625`) pero nunca se limpia al borrar sesiones.
- **Fix:** al borrar una sesión (`cb_delsess`, `cb_senddel`, `cb_closedir`) hacer
  `KNOWN_SID.pop(sid, None)` para el sid borrado. Fuga menor, arreglo trivial.

### 2.7 Reply mal enrutado al expulsarse de `MSG2SESS` (mezcla de mensajes)
- **Dónde:** `handle_text` (`:2187-2194`) y `cb_abort` (`:1483`).
- **Problema:** `MSG2SESS` se limita a 200 entradas (`_track_msg`, `:342`). Si respondes
  a un mensaje viejo del bot cuyo mapeo ya se expulsó, `handle_text` cae **en silencio**
  a la sesión activa → tu mensaje va a otra sesión. En `cb_abort` el botón Cancelar de un
  status sin mapeo aborta la sesión activa, no la suya.
- **Fix:** en `handle_text`, cuando el mensaje es un *reply a un mensaje del bot* pero NO
  está en `MSG2SESS`, avisar explícitamente ("No sé a qué sesión pertenece ese mensaje
  —probablemente caducó—; reenvía o usa /sessions") en vez de asumir la activa. Mantener
  el fallback a activa solo cuando NO es un reply.

---

## Fase 3 — Seguridad: `/undo` borra untracked sin avisar

### 3.1 Confirmación antes de borrar archivos no rastreados
- **Dónde:** `gitops.restore` (`:214`, hace `git clean -fd`), `cmd_undo`
  (`telegram_bot.py:1518`).
- **Problema:** `git clean -fd` (sin `-x`, respeta `.gitignore`) borra los archivos/dirs
  untracked **no ignorados**. Para archivos creados por la tarea es correcto, pero borra
  también untracked nuevos que el usuario pudiera querer conservar (caso límite: un
  `node_modules` no ignorado se borra entero).
- **Fix:**
  1. En `gitops.py`, añadir `untracked_to_clean(directory) -> list[str]` que ejecute
     `git clean -nd` (dry-run; ya respeta `.gitignore`) y parsee las rutas ("Would
     remove X").
  2. En `cmd_undo`, antes de restaurar: si hay untracked que se borrarían, **no**
     restaurar directamente; mostrar la lista y un teclado inline
     "✅ Sí, deshacer (borra esos archivos) / ❌ Cancelar". Solo al confirmar se ejecuta
     el `restore` actual. Si no hay untracked, comportamiento igual que ahora.
  3. Añadir el handler de callback correspondiente y registrarlo en `main()`.

---

## Notas — investigado, sin cambio necesario

- **Etiqueta de proyecto en `_question_bridge`/`_ctx_line` bajo multisesión:** leen
  `CURRENT_CTX` (un `ContextVar` fijado en `_run_task`). **Investigado y descartado como
  riesgo:** cada prompt crea su propio `ClaudeSDKClient`, y `client.connect()` se ejecuta
  dentro de `_run_task` *después* de `CURRENT_CTX.set(...)`. Como las ContextVars se copian
  al crear tareas, las tareas internas del SDK para ese cliente capturan el contexto de esa
  sesión concreta → no hay mezcla entre sesiones concurrentes. El enrutado de la respuesta
  ya era seguro (cada `qid` tiene su Future). Peor caso posible: etiqueta vacía (benigno),
  nunca atribuida a otra sesión. No requiere cambio de código.

## Verificación
Tras cada fase: `.venv/bin/python -m py_compile src/*.py`. No hay test suite ni linter.
