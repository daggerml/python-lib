FROM ubuntu:20.04

RUN apt-get update && apt-get -y install curl python3.8 build-essential libssl-dev libffi-dev python3-dev python3-pip

# Python won’t try to write .pyc or .pyo files on the import of source modules
# Force stdin, stdout and stderr to be totally unbuffered. Good for logging
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONIOENCODING=UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN python3 -m pip install pyarrow pandas s3fs boto3 fsspec

ADD main.py /code/main.py
WORKDIR /code
ENV PATH=/code:${PATH}
RUN mkdir /data

ENTRYPOINT ["main.py"]
