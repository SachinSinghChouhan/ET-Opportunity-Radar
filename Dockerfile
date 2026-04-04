# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS base

# System deps needed by scipy/numpy build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make libffi-dev libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps (full requirements for Docker/Railway)
COPY requirements-full.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ── Stage 2: App ─────────────────────────────────────────────────────────────
COPY . .

# Create data dir for SQLite
RUN mkdir -p data app/static/briefings

# Expose port (Railway injects $PORT)
EXPOSE 8000

# Entrypoint
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
