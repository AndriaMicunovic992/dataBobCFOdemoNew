# Testing & Validation Checklist

This project does not currently have an automated test suite. Use these manual
validation steps to verify changes before committing.

## Backend Validation

### Server Starts
```bash
cd backend && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```
- No import errors on startup
- Health check returns 200: `curl http://localhost:8000/api/health`
- OpenAPI docs load: `http://localhost:8000/docs`

### Migration Applies
```bash
cd backend && alembic upgrade head
```
- No errors during migration
- `alembic downgrade -1` + `alembic upgrade head` round-trips cleanly

### Endpoint Smoke Tests
After changes to routes, verify with curl or the Swagger UI:
- `GET /api/models` — returns list (possibly empty)
- `POST /api/models` with `{"name": "Test"}` — creates a model
- `POST /api/models/{id}/upload` with a file — upload + parse succeeds
- `GET /api/models/{id}/datasets` — returns parsed datasets
- `POST /api/models/{id}/datasets/baseline` — returns baseline data
- `POST /api/models/{id}/chat` — SSE stream starts (requires API key)

### Service-Level Checks

**Scenario engine** (`services/scenario.py`):
- Multiplier rules: value * factor applied to matching rows only
- Offset rules: total amount distributed across matching rows
- Period filtering: rules respect periodFrom/periodTo boundaries
- Empty filters: rule applies to all rows
- Empty DataFrame: no crash, returns empty result

**Parser** (`services/parser.py`):
- xlsx with multiple sheets → one dataset per sheet
- csv/tsv → single dataset
- German number format (1.234,56) correctly parsed as numeric
- Date columns detected and normalized
- Boolean columns (Yes/No, TRUE/FALSE, X) handled

**Storage** (`services/storage.py`):
- Dynamic table created with `ds_` prefix
- Column types match Polars inference
- Bulk load completes without data loss
- Table cleanup on dataset deletion

## Frontend Validation

### Build Succeeds
```bash
cd frontend && npm run build
```
- No compilation errors
- Output in `frontend/dist/`

### Dev Server
```bash
cd frontend && npm run dev
```
- Loads without console errors at `http://localhost:5173`
- API proxy to backend works (no CORS issues)

### UI Smoke Tests
- Model selector: can create, switch, rename, delete models
- File upload: drag or click, shows progress, datasets appear after parse
- Schema view: column roles display, can change roles via dropdown
- Baseline view: data table renders with merged dimensions
- Scenario creation: via chat or manual, rules display correctly
- Scenario chart: bars render with correct colors and values
- Chat: messages stream in, tool calls show execution status
- Knowledge panel: entries display, can add/edit/delete

### Cross-Cutting Concerns
- Number formatting uses German locale (1.234,56)
- Colors: green = positive, red = negative, muted = zero
- Responsive layout: sidebar + main content area
- Loading states: spinners or skeleton UI during fetches
- Error states: user-friendly error messages, not stack traces

## AI Agent Validation

### Schema Agent
- Upload a multi-sheet xlsx → all sheets get `ai_analyzed = True`
- Column roles are sensible (measures are numeric, keys are identifiers)
- `agent_context_notes` populated with summary, dimensions, hints
- Relationships auto-detected between fact and dimension tables

### Chat Agent
- Ask a data question → `query_data` tool called → answer with numbers
- Ask to create a scenario → `create_scenario_rules` called → rule created
- Ask "what categories exist?" → `list_dimension_values` called
- Multi-turn: follow-up questions maintain context
- Tool results are valid JSON, no hallucinated column names
- SSE events arrive in correct order: text_delta* → done

## Pre-Commit Checklist

Before committing any change:

- [ ] Backend server starts without import errors
- [ ] `alembic upgrade head` succeeds (if migration added)
- [ ] `npm run build` succeeds (if frontend changed)
- [ ] Changed endpoints return expected response shapes
- [ ] No hardcoded secrets or API keys in committed code
- [ ] No `print()` statements left in Python code (use `logger`)
- [ ] No `console.log()` left in JS code (unless intentional debugging)
