# Airflow + the SECOP Ledger pipeline in its OWN virtual environment.
#
# Why two environments: Airflow and dbt both pin many libraries; installing them together breaks one
# of them sooner or later. Airflow runs from the image's Python; the pipeline runs from
# /opt/secop-venv, built from uv.lock (the same versions as on the laptop and in CI).
# The DAG only calls /opt/secop-venv/bin/python and /opt/secop-venv/bin/dbt.
FROM apache/airflow:3.4.0-python3.12

COPY --from=ghcr.io/astral-sh/uv:0.11.15 /uv /usr/local/bin/uv

USER root
RUN mkdir -p /opt/secop /opt/secop-venv && chown airflow:0 /opt/secop /opt/secop-venv
USER airflow

ENV UV_PROJECT_ENVIRONMENT=/opt/secop-venv \
    UV_PYTHON=/usr/local/bin/python \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /opt/secop
# Dependencies first (cached layer), then the project itself.
COPY --chown=airflow:0 pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY --chown=airflow:0 src ./src
# Editable install: it points to /opt/secop/src, where docker-compose mounts the repo,
# so a code change needs no rebuild (a dependency change does).
RUN uv sync --frozen --no-dev
