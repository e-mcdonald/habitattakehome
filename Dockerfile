# Multi-stage build: keep runtime slim, avoid shipping build tools.

# ---- builder ---------------------------------------------------------------
FROM python:3.11-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

# Install into an isolated venv so we can copy it wholesale to runtime.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy dependency manifest first for better layer caching.
COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip \
 && pip install .

# ---- test -------------------------------------------------------------------
# Extends builder (already has pyproject.toml, src, and installed deps) with
# dev extras and the full tests/ dir, which runtime intentionally omits.
FROM builder AS test

RUN pip install ".[dev]"
COPY sql ./sql
COPY sources ./sources
COPY tests ./tests

ENTRYPOINT ["pytest"]
CMD ["-m", "not db"]

# ---- runtime ---------------------------------------------------------------
FROM python:3.11-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIPELINE_ENV=prod

# Non-root user for defense in depth.
RUN useradd --create-home --shell /bin/bash pipeline
WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --chown=pipeline:pipeline sources ./sources
COPY --chown=pipeline:pipeline sql ./sql
COPY --chown=pipeline:pipeline tests/fixtures ./tests/fixtures

# Pre-create so the pipeline_data volume mount inherits pipeline:pipeline
# ownership instead of being created root-owned on first mount.
RUN mkdir -p /app/data && chown pipeline:pipeline /app/data

USER pipeline

ENTRYPOINT ["habitat-pipeline"]
CMD ["--help"]
