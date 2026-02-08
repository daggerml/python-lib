FROM python:3.11-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential cmake git \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir "/app[s3]" pandas scikit-learn pyarrow s3fs

ENV PYTHONUNBUFFERED=1
