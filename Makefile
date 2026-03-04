# DataBobIQ — developer convenience targets
#
# Quick start:
#   make install   ← one-time setup
#   make dev       ← postgres + backend in one terminal
#   make frontend  ← frontend dev server in a second terminal

.PHONY: help install dev frontend migrate reset-db logs clean stop

# ── Default ──────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  dataBobIQ — development commands"
	@echo ""
	@echo "  make install    Install backend (pip) + frontend (npm) dependencies"
	@echo "  make dev        Start PostgreSQL + run migrations + start backend"
	@echo "  make frontend   Start Vite dev server  (http://localhost:5173)"
	@echo "  make migrate    Apply pending Alembic migrations"
	@echo "  make reset-db   Drop all migrations and rerun from scratch"
	@echo "  make logs       Tail Docker service logs"
	@echo "  make stop       Stop all Docker services"
	@echo "  make clean      Stop Docker + clear uploaded files"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	@echo "→ Installing backend dependencies…"
	cd backend && pip install -e .
	@echo "→ Installing frontend dependencies…"
	cd frontend && npm install
	@echo "→ Creating .env if it doesn't exist…"
	@test -f backend/.env || cp backend/.env.example backend/.env 2>/dev/null || \
	  (echo "  (no .env.example found — copy .env.example manually)" && true)
	@echo "✓ Done.  Edit backend/.env to set ANTHROPIC_API_KEY_CHAT / AGENT."

# ── Services ──────────────────────────────────────────────────────────────────

dev:
	@echo "→ Starting PostgreSQL via docker-compose…"
	docker-compose up -d postgres
	@echo "→ Waiting for PostgreSQL to be ready…"
	@until docker-compose exec -T postgres pg_isready -U databobiq -d databobiq \
	    >/dev/null 2>&1; do printf '.'; sleep 1; done; echo " ready."
	@echo "→ Running Alembic migrations…"
	cd backend && alembic upgrade head
	@echo "→ Starting backend on http://localhost:8000 (Ctrl-C to stop)"
	cd backend && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

frontend:
	@echo "→ Starting Vite dev server on http://localhost:5173 (Ctrl-C to stop)"
	cd frontend && npm run dev

# ── Database ──────────────────────────────────────────────────────────────────

migrate:
	@echo "→ Applying Alembic migrations…"
	cd backend && alembic upgrade head
	@echo "✓ Migrations applied."

reset-db:
	@echo "⚠  This will DROP all tables and rerun all migrations."
	@printf "   Press Enter to continue, Ctrl-C to abort: " && read _
	@echo "→ Reverting all migrations…"
	cd backend && alembic downgrade base
	@echo "→ Reapplying all migrations…"
	cd backend && alembic upgrade head
	@echo "✓ Database reset complete."

# ── Utilities ─────────────────────────────────────────────────────────────────

logs:
	docker-compose logs -f

stop:
	docker-compose down

clean: stop
	@echo "→ Clearing uploaded files…"
	find backend/uploads -type f ! -name '.gitkeep' -delete
	@echo "✓ Clean."
