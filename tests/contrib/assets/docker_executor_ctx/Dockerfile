FROM python:3.14-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SETUPTOOLS_SCM_PRETEND_VERSION=0.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/daggerml

COPY . /opt/daggerml

RUN pip install --no-cache-dir boto3 pandas pyarrow /opt/daggerml
