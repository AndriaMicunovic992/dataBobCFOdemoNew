# ── Stage 1: build React frontend ────────────────────────────────
FROM node:18-slim AS frontend
WORKDIR /frontend
COPY frontend/package*.json .
RUN npm ci
COPY frontend/ .
RUN npm run build

# ── Stage 2: Python backend ───────────────────────────────────────
FROM python:3.11-slim
WORKDIR /app
COPY --from=frontend /frontend/dist static/
COPY backend/pyproject.toml .
RUN pip install . --no-cache-dir
COPY backend/app/ app/
COPY backend/alembic/ alembic/
COPY backend/alembic.ini .
RUN mkdir -p uploads
EXPOSE 8000
CMD alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
