# --- Builder Stage ---
FROM python:3.11-slim AS builder

WORKDIR /build

# 1. Install Poetry
RUN pip install --no-cache-dir poetry

# 2. Add the export plugin (Fixes your exact error)
RUN poetry self add poetry-plugin-export

COPY pyproject.toml poetry.lock ./

# Export poetry dependencies directly to a classic requirements text
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

# --- Production Stage ---
FROM python:3.11-slim AS runner

WORKDIR /workspace

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=builder /build/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy source application
COPY ./app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]