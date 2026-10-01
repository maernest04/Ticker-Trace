FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml ./
COPY services/engine/src ./services/engine/src
RUN pip install --no-cache-dir .

COPY alembic.ini ./
COPY migrations ./migrations

CMD ["market-execution-api"]
