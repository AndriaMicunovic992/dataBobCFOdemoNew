# DataBobIQ

A financial data analysis platform powered by AI.

## Project Structure

```
databobiq/
├── backend/           # Python FastAPI
├── frontend/          # React Vite (coming soon)
├── docker-compose.yml # PostgreSQL for local dev
└── README.md
```

## Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Node.js 18+ (for frontend, coming soon)

## Quick Start

### 1. Start the database

```bash
docker-compose up -d
```

### 2. Set up the backend

```bash
cd backend
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY

pip install -e .
# or: pip install uv && uv pip install -e .

alembic upgrade head
uvicorn app.main:app --reload
```

The API will be available at http://localhost:8000.

## API

- Health check: `GET /api/health`
- API docs: `GET /docs`

## Environment Variables

See `backend/.env.example` for all required environment variables.
