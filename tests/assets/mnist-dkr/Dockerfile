FROM python:3.12.3-bullseye

RUN --mount=type=bind,target=/tmp/mount,source=. \
    cp -r /tmp/mount /tmp/build \
    && ls -Alh /tmp/build \
    && python3 -m pip install /tmp/build[all] \
    && python3 -m pip install Cython \
    && python3 -m pip install pandas==2.0.1 scikit-learn==1.4 lightgbm==4.3 polars==0.20 \
    # && pip install /tmp/build/submodules/daggerml_cli \
    && rm -r /tmp/build \
    && echo done

RUN python3 -c 'import pandas as pd; print(pd.__file__)'
