# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS base

# System deps needed by Playwright + pydub + scipy
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make libffi-dev libssl-dev \
    ffmpeg \
    # Playwright chromium deps
    libnss3 libxss1 libasound2 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdbus-1-3 libdrm2 libgbm1 libgtk-3-0 \
    libnspr4 libx11-xcb1 libxcb-dri3-0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libxrender1 libxtst6 \
    ca-certificates fonts-liberation wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps (full requirements for Docker/Railway)
COPY requirements-full.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright chromium browser
RUN playwright install chromium

# ── Stage 2: App ─────────────────────────────────────────────────────────────
COPY . .

# Create data dir for SQLite
RUN mkdir -p data app/static/briefings

# Expose port (Railway injects $PORT)
EXPOSE 8000

# Entrypoint
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
