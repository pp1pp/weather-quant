# Stage 1: Build frontend
FROM node:20-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Python runtime
FROM python:3.11-slim
WORKDIR /app

# Install system deps for scipy
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY main.py main_shanghai.py ./
COPY src/ src/
COPY config/settings.yaml config/cities.yaml config/markets*.yaml config/
COPY scripts/ scripts/
COPY tools/ tools/
COPY tests/ tests/

# Copy built frontend from stage 1
COPY --from=frontend-build /app/frontend/dist frontend/dist

# Create data directory for SQLite
RUN mkdir -p data/logs data/reviews

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/api/health || exit 1

CMD ["python3", "main.py", "--web"]
