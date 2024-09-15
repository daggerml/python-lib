FROM python:3.12
# .3-bullseye

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update -y \
    && apt-get install -y unzip curl \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws/ /var/cache/apt \
    && apt-get clean

RUN --mount=type=bind,target=/tmp/mount,source=. \
    cp -r /tmp/mount /tmp/build \
    && ls -Alh /tmp/build \
    && python3 -m pip install --no-cache-dir /tmp/build/submodules/daggerml_cli/ \
    && python3 -m pip install --no-cache-dir /tmp/build \
    && rm -rf /tmp/build \
    && echo done

COPY <<EOT /bin/dml-runner.sh
#!/bin/bash
set -ex
set -o pipefail

script_uri=\$DML_SCRIPT_URI
dump_uri=\$DML_DUMP_URI
out_uri=\$DML_RESULT_URI

aws s3 mv \$dump_uri /tmp/dml-input.dump
aws s3 cp \$script_uri /tmp/dml_script.py
cat /tmp/dml-input.dump | python /tmp/dml_script.py > /tmp/dml-output.dump
aws s3 cp /tmp/dml-output.dump \$out_uri
EOT

RUN chmod +x /bin/dml-runner.sh
CMD ["dml-runner.sh"]
