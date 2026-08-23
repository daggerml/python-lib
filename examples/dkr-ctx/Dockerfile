FROM python:3.13-slim AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential cmake git \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN python -m venv /opt/venv \
  && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
  && /opt/venv/bin/pip install --no-cache-dir "/app" pandas scikit-learn pyarrow s3fs

FROM python:3.13-slim AS runtime

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

WORKDIR /app
