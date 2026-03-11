# Playbooks — Step-by-Step Task Recipes

## 1. Add a New API Endpoint

1. **Define Pydantic schemas** in `backend/app/schemas/api.py`:
   - `FooCreate` (request body), `FooResponse` (response), `FooUpdate` (optional)
   - Add `model_config = ConfigDict(from_attributes=True)` to response models

2. **Add the route** in `backend/app/api/routes.py`:
   - Import new schemas at the top
   - Add `@router.get/post/put/delete(...)` with `response_model=`
   - Inject `db: AsyncSession = Depends(get_db)` for DB access
   - IMPORTANT: Place literal paths BEFORE parameterized paths to avoid conflicts

3. **Implement service logic** (if complex):
   - Create or extend a file in `backend/app/services/`
   - Keep route handler thin; delegate business logic to services

4. **Wire up the frontend**:
   - Add API function in `frontend/src/api.js` using `req()` helper
   - Add React Query hook or call from component in `App.jsx`

## 2. Add a New Database Model / Column

1. **Define the model** in `backend/app/models/metadata.py`:
   - Inherit from `Base`
   - Use `Mapped[type]` with `mapped_column(...)` for all fields
   - Add relationships with `back_populates=` on both sides

2. **Create an Alembic migration**:
   ```bash
   cd backend && alembic revision --autogenerate -m "NNNN_description"
   ```
   - Review the generated migration — autogenerate misses some changes
   - Verify `upgrade()` and `downgrade()` are correct

3. **Apply the migration**:
   ```bash
   cd backend && alembic upgrade head
   ```

4. **Add Pydantic schemas** if the model is exposed via API (see Playbook #1)

## 3. Add a New Chat Tool

The chat agent in `services/chat.py` uses Anthropic tool-use. To add a new tool:

1. **Define the tool schema** — add an entry to the `TOOLS` list:
   ```python
   {
       "name": "my_new_tool",
       "description": "What this tool does and when Claude should use it.",
       "input_schema": {
           "type": "object",
           "required": ["param1"],
           "properties": {
               "param1": {"type": "string", "description": "..."},
           },
       },
   }
   ```

2. **Implement the handler** — add a case in the `execute_tool()` function:
   ```python
   elif tool_name == "my_new_tool":
       result = await _handle_my_new_tool(tool_input, dataset_id, model_id, ...)
   ```

3. **Update the system prompt** — if the tool changes Claude's behavior, update
   `build_system_prompt()` to mention when/how to use the new tool.

4. **Mind the token budget** — tool definitions count against context. Keep
   descriptions concise. Total tools + system prompt should stay under ~4000 tokens.

## 4. Add a New Scenario Rule Type

Current rule types: `multiplier`, `offset`. To add a new type:

1. **Update the rule schema** in `services/chat.py` → `TOOLS` → `create_scenario_rules`:
   - Add the new type to the `type.enum` array
   - Add any new properties to the rule item schema

2. **Implement in the scenario engine** (`services/scenario.py`):
   - Update `apply_rules()` to handle the new type
   - Follow the existing pattern: build a mask → compute adjusted values → apply

3. **Update the frontend** in `App.jsx`:
   - Update the scenario rule display/editor to show the new rule type
   - Update `computeScenario` call if response shape changes

4. **Update Pydantic schemas** in `schemas/api.py` if rule validation changes

## 5. Add a New Frontend View / Tab

1. **Create the component function** in `App.jsx`:
   ```jsx
   function MyNewView({ modelId, ... }) {
     const { data } = useQuery({ queryKey: ["myData", modelId], queryFn: ... });
     return <div style={S.card}>...</div>;
   }
   ```

2. **Add to tab navigation** — find the tab bar section and add your tab:
   - Add to the tab constants/array
   - Add conditional rendering in the main layout

3. **Wire up data** — use React Query for server state:
   ```jsx
   const { data, isLoading } = useQuery({
     queryKey: ["myData", modelId],
     queryFn: () => myApiFn(modelId),
     enabled: !!modelId,
   });
   ```

4. **Use existing style patterns** — `S.card`, `S.cardT`, `S.th`, `S.td`, `S.btn()`, `S.badge()`

## 6. Modify AI Agent Behavior

### Schema Agent (one-shot classification)
- File: `services/schema_agent.py`
- Edit `_SYSTEM_PROMPT` to change classification rules
- Edit response parsing in `analyze_schema()` to handle new output fields
- Model: Currently uses a fast/cheap model (Haiku-class)

### Chat Agent (multi-turn tool-use)
- File: `services/chat.py`
- `build_system_prompt()` — constructs the system message with data context
- `TOOLS` list — defines available tool schemas
- `stream_chat()` — the main SSE generator (handles tool-use loop)
- `_MAX_TOOL_ROUNDS = 5` — max consecutive tool calls per turn
- Model: `_CHAT_MODEL = "claude-sonnet-4-6"`

### AI Context Builder
- File: `services/ai_context.py`
- `build_agent_context()` — generates XML context from semantic layer
- Called from `routes.py` before passing to `stream_chat()`
- Token target: <4000 tokens total

## 7. Upload + Parse Flow

When a file is uploaded:

1. `routes.py` receives multipart upload → saves to `UPLOAD_DIR`
2. `parser.parse_file()` reads xlsx/csv with Polars, infers types
3. `storage.create_table_and_load()` creates a `ds_*` PostgreSQL table (sync engine)
4. Dataset + DatasetColumn records are inserted
5. `schema_agent.analyze_schema()` runs in background → classifies columns, detects relationships
6. `calendar_svc.auto_link_calendar()` links time columns to the calendar dimension
7. Results are returned as `DatasetResponse` list

## 8. Add a Knowledge Entry Type

1. Update the `entry_type` comment/enum in `models/metadata.py` → `KnowledgeEntry`
2. Add the type to `DATA_UNDERSTANDING_TOOLS` in `services/chat.py` → `save_knowledge` tool
3. Add to `list_knowledge` tool's `entry_type.enum` if filterable
4. Update `ai_context.py` if the new type should appear in AI context blocks
5. Update frontend rendering in `App.jsx` (Knowledge panel section)
