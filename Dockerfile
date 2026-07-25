# WRC pipeline image — hosts Dagit (web UI + daemon) with the scraper and
# transform code baked in. `docker compose up` boots Mongo + MinIO alongside;
# the reviewer materializes partitions from Dagit at http://localhost:3000.
#
# Multi-stage: builder resolves + freezes the environment via uv, runtime
# installs only the compiled venv. Keeps the final image small and lets the
# builder cache survive source-only edits.

FROM python:3.11-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1

# uv is a single static binary — copy it from the official image.
COPY --from=ghcr.io/astral-sh/uv:0.9.21 /uv /uvx /usr/local/bin/

WORKDIR /app

# Install dependencies first (cached layer) — lockfile only, no source yet.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-editable

# Now install the project itself.
COPY src ./src
COPY scrapy.cfg ./
RUN uv sync --frozen --no-editable


FROM python:3.11-slim AS runtime

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app

EXPOSE 3000

# Dagit listens on all interfaces so docker-compose port forwarding works.
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000", "-m", "wrc_pipeline.orchestration"]
