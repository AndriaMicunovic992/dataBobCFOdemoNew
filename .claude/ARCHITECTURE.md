# Architecture

## System Overview

DataBobIQ is a financial data analysis platform where users upload ERP exports,
an AI agent classifies the schema, and a chat agent helps create what-if scenarios.

```
┌─────────────────┐     ┌──────────────────────────────────┐
│  React Frontend  │────▶│  FastAPI Backend (:8000)          │
│  (Vite :5173)    │◀────│                                  │
└─────────────────┘     │  ┌─────────────┐ ┌────────────┐  │
                        │  │ Schema Agent │ │ Chat Agent │  │
                        │  │ (Haiku)      │ │ (Sonnet)   │  │
                        │  └──────┬───────┘ └─────┬──────┘  │
                        │         │               │          │
                        │  ┌──────▼───────────────▼──────┐  │
                        │  │       PostgreSQL             │  │
                        │  │  ┌──────────┐ ┌──────────┐  │  │
                        │  │  │ Metadata │ │ ds_* data │  │  │
                        │  │  │ tables   │ │ tables    │  │  │
                        │  │  └──────────┘ └──────────┘  │  │
                        │  └─────────────────────────────┘  │
                        └──────────────────────────────────┘
```

## Data Flow

### 1. Upload & Parse
```
File (xlsx/csv)
  → parser.py: read with Polars, infer types (numeric/text/date/boolean)
  → storage.py: CREATE TABLE ds_xxxx (sync engine), COPY bulk load
  → metadata.py: insert Dataset + DatasetColumn records
  → schema_agent.py: Claude classifies columns (key/measure/time/attribute/ignore)
  → calendar_svc.py: auto-link time columns to dim_calendar
```

### 2. Semantic Layer
```
Schema Agent output
  → agent_context_notes (JSON on Dataset): summary, dimensions, time range
  → DatasetColumn.ai_suggestion: per-column classification reasoning
  → SemanticColumn: human descriptions, synonyms per column
  → SemanticValueLabel: raw_value → display_label mappings (e.g. "400100" → "Personnel Costs")
  → KnowledgeEntry: business terms, metric definitions, relationships, notes
```

### 3. Baseline Computation
```
POST /models/{id}/datasets/baseline
  → Find fact table (has measure columns) + dimension tables
  → Join fact → dimensions via DatasetRelationship records
  → Return merged rows with all dimension columns attached
  → Respects base_config: { source: "actuals"|"scenario", base_year, ... }
```

### 4. Scenario Engine
```
Scenario.rules (JSON array)
  → scenario.apply_rules(df, rules, value_col)
  → For each rule:
      1. Build mask: filters (AND logic) + period range
      2. Apply: multiplier (value * factor) or offset (distribute amount)
      3. Offset distribution: "use_base" (proportional) or "equal" (flat split)
  → Returns DataFrame with original + adjusted values
```

### 5. Chat Agent (Tool-Use Loop)
```
POST /models/{id}/chat  (SSE stream)
  1. ai_context.build_agent_context() → XML context block (<4000 tokens)
  2. build_system_prompt() → system message with data context
  3. Claude receives: system + conversation history + tools
  4. Tool-use loop (max 5 rounds):
     - Claude calls tool → execute_tool() → return result → Claude continues
  5. SSE events: text_delta | tool_executing | tool_result | scenario_rule | done | error
```

Available chat tools:
- `query_data` — grouped aggregation with filters
- `create_scenario_rules` — build what-if rules (single or batch)
- `list_dimension_values` — unique column values lookup
- `list_knowledge` — search business knowledge base
- `list_scenarios` — list existing scenarios
- `copy_scenario` — duplicate a scenario

Data Understanding Agent tools (separate persona):
- `save_knowledge` — persist domain knowledge
- `query_data` — same as above
- `list_dimension_values` — same as above

## Database Schema

### Metadata Tables (Alembic-managed)
- `models` — top-level workspace container
- `datasets` — uploaded file metadata + AI analysis results
- `dataset_columns` — column definitions per dataset
- `dataset_relationships` — join relationships between datasets
- `scenarios` — saved what-if scenarios with rules (JSON)
- `semantic_columns` — column descriptions and synonyms
- `semantic_value_labels` — value → display label mappings
- `transformation_steps` — replayable data transformations
- `knowledge_entries` — AI/user-captured domain knowledge

### Dynamic Data Tables
- Named `ds_<random_hex>` (e.g., `ds_a1b2c3d4`)
- Created by `storage.py` using sync engine
- Schema matches parsed file columns (Text, Numeric(18,4), BigInteger, Date, Boolean)
- Special: `dim_calendar` — global calendar dimension (2020–2027)

## Frontend Architecture

Single-file monolith (`App.jsx`, ~4300 lines):

```
App.jsx
  ├── Theme constants (C, SC_COLORS, ROLE_COLORS)
  ├── Formatting utils (fmt, fmtS, valColor)
  ├── Style objects (S)
  ├── Component functions:
  │   ├── ModelSelector — workspace picker/creator
  │   ├── DatasetUpload — file upload + status
  │   ├── SchemaView — column roles, relationships, semantic layer
  │   ├── BaselineView — merged data table with drill-down
  │   ├── ScenarioEditor — rule creation/editing UI
  │   ├── ScenarioChart — Recharts bar chart comparison
  │   ├── ChatPanel — streaming chat interface
  │   ├── KnowledgePanel — business knowledge CRUD
  │   └── TransformationView — data transformation editor
  └── App() — main component with tab routing
```

State management:
- **Server state**: React Query (`useQuery`, `useMutation`)
- **UI state**: `useState` (active tab, selections, expanded sections)
- **No global store** (no Redux, Zustand, etc.)

## Deployment

### Railway (Production)
- `railway.toml` + `Dockerfile` for deployment
- `DATABASE_URL` auto-converted: `postgresql://` → `postgresql+asyncpg://`
- Frontend built with `vite build`, served as static files from `backend/static/`
- Memory constraints: `POLARS_MAX_THREADS=2` set in `main.py`

### Local Development
- PostgreSQL via `docker-compose.yml`
- Backend: `uvicorn --reload` on `:8000`
- Frontend: Vite dev server on `:5173` with proxy to backend
- Two terminals: `make dev` + `make frontend`
