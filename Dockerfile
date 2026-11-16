# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.13.8 AS uv
FROM python:3.12.16-slim-bookworm AS build
WORKDIR /app
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-editable
# The locked greenlet wheel bundles its own tests. Keep those out of runtime too.
RUN /app/.venv/bin/python -c "import pathlib,shutil,greenlet; shutil.rmtree(pathlib.Path(greenlet.__file__).parent / 'tests')"

FROM python:3.12.16-slim-bookworm AS runtime
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN groupadd --gid 10001 payments && useradd --uid 10001 --gid 10001 --no-create-home payments
COPY --from=build /app/.venv /app/.venv
COPY migrations ./migrations
COPY alembic.ini ./
COPY examples/webhook_receiver.py ./examples/webhook_receiver.py
USER 10001:10001
EXPOSE 8000
CMD ["python", "-m", "payments.api"]
