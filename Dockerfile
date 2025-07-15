FROM python:3.13.3-alpine

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV PYTHONPATH=/app

COPY pyproject.toml uv.lock ./
RUN uv sync
COPY . .