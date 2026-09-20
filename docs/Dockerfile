FROM python:3.13-alpine AS builder

RUN apk add --no-cache build-base cmake

WORKDIR /app
COPY pyproject.toml README.md CMakeLists.txt ./
COPY c ./c
COPY src ./src

# The tutorial funk needs DaggerML's runtime, not the dashboard application or
# its generated web assets.
RUN rm -rf src/daggerml/dashboard

ARG SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DAGGERML=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DAGGERML=${SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DAGGERML}

RUN python -m pip wheel \
      --no-cache-dir \
      --no-deps \
      --wheel-dir /tmp/wheels \
      /app \
  && python -m pip install \
      --no-cache-dir \
      --no-compile \
      --prefix /install \
      /tmp/wheels/daggerml-*.whl \
      boto3

FROM python:3.13-alpine AS runtime

COPY --from=builder /install /usr/local

ENV PYTHONUNBUFFERED=1

WORKDIR /app
