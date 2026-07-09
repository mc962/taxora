# ======================================
# Stage 1: Builder
# ======================================
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy only dependency metadata first (layer caching)
COPY pyproject.toml uv.lock* README.md ./
RUN uv sync --no-dev --no-install-project

# Copy full project and install the project package itself
COPY . .
RUN uv sync --no-dev

# ======================================
# Stage 2: Runtime
# ======================================
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN groupadd --system taxora && useradd --system --gid taxora --no-create-home taxora

COPY --from=builder --chown=taxora:taxora /app /app

USER taxora

EXPOSE 8000

# Overridden by docker-compose's `command:` for the worker service
CMD ["uvicorn", "taxora.main:app", "--host", "0.0.0.0", "--port", "8000"]
