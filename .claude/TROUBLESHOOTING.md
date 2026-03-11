# Troubleshooting — Known Pitfalls

## Polars

### Utf8View / LargeUtf8 casting errors
**Problem**: Newer Polars/fastexcel versions produce `Utf8View` columns. Casting
directly from `Utf8View` to numeric/date raises `"casting from Utf8View is not allowed"`.

**Fix**: Always normalize to `pl.String` first:
```python
df = _normalize_utf8_view(df)  # see parser.py or storage.py
```

### Column dtype before casting
**Problem**: Calling `.cast(pl.Float64)` on a column that's already numeric but
contains nulls or mixed types can crash.

**Fix**: Check dtype first, use `strict=False`:
```python
pl.col("amount").cast(pl.Float64, strict=False)
```

### Empty DataFrame operations
**Problem**: `.group_by().agg()` on an empty DataFrame can produce unexpected
schema or raise errors.

**Fix**: Check `df.height == 0` before aggregation and return early with
the correct schema.

## Database

### Async vs Sync engine confusion
**Problem**: Using `async_engine` for DDL (CREATE TABLE, ALTER TABLE) or bulk
COPY operations fails or hangs because asyncpg doesn't support these.

**Fix**: All DDL and bulk operations MUST use `sync_engine` (imported from
`database.py`). The `storage.py` service enforces this. App-level queries
use `async_engine` via `get_db()` dependency.

### Railway DATABASE_URL format
**Problem**: Railway provides `postgresql://...` but SQLAlchemy async needs
`postgresql+asyncpg://...`.

**Fix**: `config.py` has a `_normalise` validator that auto-converts. If adding
a new database connection, follow the same pattern.

### Migration conflicts
**Problem**: Two branches add migrations with the same sequence number.

**Fix**: Renumber your migration and update `down_revision` to chain correctly.
Check `alembic/versions/` for the latest number before creating a new migration.

## AI Agents

### Token budget exceeded
**Problem**: System prompt + context + tools exceeds model's context window,
causing truncated responses or errors.

**Fix**: `ai_context.build_agent_context()` targets <4000 tokens. If you're
adding content to system prompts or tool descriptions, measure impact.
Keep tool descriptions concise. Use `_MAX_LABELS_INLINE = 20` to limit
inline value labels.

### Schema agent timeout
**Problem**: `schema_agent.analyze_schema()` has a `_AGENT_TIMEOUT_SECONDS = 300`
hard limit. Complex multi-sheet uploads can timeout.

**Fix**: This is intentional — uploads shouldn't block forever. If the agent
times out, the dataset is still created but `ai_analyzed = False`. Users can
trigger re-analysis via the `/reanalyze` endpoint.

### Chat tool-use infinite loops
**Problem**: Claude keeps calling tools in a loop without giving a final response.

**Fix**: `_MAX_TOOL_ROUNDS = 5` in `chat.py` caps consecutive tool calls.
If this happens frequently, the tool descriptions may be ambiguous — Claude
doesn't know when to stop. Clarify tool descriptions.

### Negative value sign convention
**Problem**: In German accounting data, expenses are typically stored as negative
values. "Increase costs by 300K" means offset = -300000 (more negative).

**Fix**: The `create_scenario_rules` tool description explicitly documents this.
When modifying the tool or adding new financial logic, preserve sign awareness.

## Frontend

### The App.jsx monolith
**Problem**: At ~4300 lines, `App.jsx` is difficult to navigate.

**Approach**: When modifying, search for the specific component function name
or feature keyword. Each "view" is a self-contained function. Don't try to
read the entire file — focus on the relevant section.

### React Query cache invalidation
**Problem**: After a mutation (create/update/delete), the UI shows stale data.

**Fix**: Always invalidate related queries after mutations:
```jsx
queryClient.invalidateQueries({ queryKey: ["datasets", modelId] });
queryClient.invalidateQueries({ queryKey: ["baseline", modelId] });
```

### SSE streaming chat
**Problem**: `EventSource` only supports GET. Chat uses POST with a body.

**Fix**: The `streamChat()` function in `api.js` uses raw `fetch` +
`ReadableStream` + manual SSE parsing. Don't switch to `EventSource`.

### Number formatting locale
**Problem**: The app uses German number format (1.234,56) via `Intl.NumberFormat("de-DE")`.

**Fix**: Don't change the locale constants in `fmt()` and `fmtS()` without
understanding the user base. The `valColor()` function returns green/red/muted
based on positive/negative/zero values.

## Deployment

### Railway memory limits
**Problem**: Polars operations on large datasets can OOM on Railway's default plan.

**Fix**: `POLARS_MAX_THREADS=2` is set in `main.py` before any Polars import.
The health endpoint reports memory usage via `psutil`. Use lazy evaluation
(`.lazy()` → `.collect()`) to minimize peak memory.

### Static file serving
**Problem**: In production, the frontend is served from `backend/static/`.

**Fix**: Build frontend with `cd frontend && npm run build`, then copy
`frontend/dist/*` to `backend/static/`. The FastAPI app mounts `/` → `static/`
only if the directory exists.

### Route order in routes.py
**Problem**: FastAPI matches routes in registration order. `/datasets/{dataset_id}`
registered before `/datasets/baseline` will match "baseline" as a dataset_id.

**Fix**: Always register literal paths first. The file header documents this
requirement. When adding new endpoints, respect the ordering.
