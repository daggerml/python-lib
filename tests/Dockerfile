FROM python:3.13.0a6-bullseye

RUN --mount=type=bind,target=/tmp/mount,source=. \
    cp -r /tmp/mount /tmp/build \
    && ls -Alh /tmp/build \
    && pip install /tmp/build[all] \
    # && pip install /tmp/build/submodules/daggerml_cli \
    && rm -r /tmp/build \
    && echo done
