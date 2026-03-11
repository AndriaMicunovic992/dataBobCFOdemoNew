# Coding Conventions

## Python (Backend)

### Naming
- **snake_case** for functions, variables, modules
- **PascalCase** for SQLAlchemy models and Pydantic schemas
- **UPPER_CASE** for module-level constants
- Private helpers prefixed with `_` (e.g., `_find_period_col`, `_build_mask`)

### Imports
- Group: stdlib, third-party, app-internal (separated by blank lines)
- Use `from __future__ import annotations` in service modules
- Models: `from app.models.metadata import Dataset, Scenario, ...`
- Database: `from app.database import get_db, sync_engine, async_engine`

### Database Patterns
- **Dependency injection**: Routes receive `db: AsyncSession = Depends(get_db)`
- **Async queries**: `await db.execute(select(Model).where(...))` → `.scalars().all()`
- **Eager loading**: Use `selectinload()` for relationships needed in the response
- **Sync engine only** for DDL, `COPY`, and bulk inserts (asyncpg doesn't support these)
- **UUIDs as strings**: All primary keys are `String` with `server_default=func.gen_random_uuid().cast(String)`

### Pydantic Schemas (`schemas/api.py`)
- Request models: `*Create`, `*Update` suffix
- Response models: `*Response` suffix
- Use `model_config = ConfigDict(from_attributes=True)` for ORM compatibility
- Optional fields use `X | None = None`

### Error Handling
- Raise `HTTPException(status_code=4xx, detail="...")` in route handlers
- Service functions raise plain exceptions; routes catch and convert to HTTP errors
- Log warnings with `logger.warning(...)` for non-fatal issues

### Polars
- Always use `pl.DataFrame` / `pl.LazyFrame` (never pandas)
- Cast columns with `strict=False` to avoid crashes on bad data
- Handle `Utf8View` → `pl.String` normalization before any type casts
- Use `.collect()` only at the end of a lazy chain
- Check column existence before accessing: `if col in df.columns`

## JavaScript (Frontend)

### Style
- **camelCase** for variables and functions
- Component functions are plain functions (not arrow), declared inline in `App.jsx`
- No TypeScript (plain JSX)
- No CSS files — all styles via `S` object (shared style blocks) and `C` object (color constants)

### State Management
- **React Query** (`useQuery`, `useMutation`, `useQueryClient`) for server state
- **useState** for local UI state (active tab, expanded sections, etc.)
- Query keys follow pattern: `["models"]`, `["datasets", modelId]`, `["baseline", modelId, ...]`
- Invalidate related queries after mutations: `queryClient.invalidateQueries({queryKey: [...]})`

### API Client (`api.js`)
- All functions use the `req()` helper which prepends `/api` base path
- POST/PUT/PATCH bodies wrapped with `json(body)` helper
- Streaming chat uses raw `fetch` + `ReadableStream` (not EventSource)
- Upload uses `FormData`, not JSON

### Number Formatting
- `fmt(n)` — full German locale format (de-DE, 2 decimal places)
- `fmtS(n)` — short format (1.2M, 300K, etc.)
- `valColor(v)` — green for positive, red for negative, muted for zero

### Component Patterns
- Each "view" is a function returning JSX, rendered conditionally by active tab
- Inline styles using spread: `style={{...S.card, marginTop: 10}}`
- `S.badge(color)` and `S.tag(color)` are factory functions returning style objects
- `S.btn("primary")` / `S.btn("ghost", true)` for button variants

## Alembic Migrations

- Naming: `NNNN_description.py` (zero-padded 4-digit sequence)
- Current range: `0000_initial.py` through `0010_model_settings.py`
- Always chain `revision` and `down_revision` correctly
- Test both `upgrade()` and `downgrade()` paths
- For new columns with NOT NULL: provide a `server_default` or make nullable first

## Git
- Commit messages: imperative mood, concise ("Add scenario projection endpoint")
- One logical change per commit
- Never commit `.env`, `uploads/`, or `__pycache__/`
