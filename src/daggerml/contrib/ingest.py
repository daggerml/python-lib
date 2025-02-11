#!/usr/bin/env python3
import hashlib
from tempfile import NamedTemporaryFile

import boto3

from daggerml import Resource


def compute_file_hash(path, chunk_size=8192, hash_algorithm="sha256"):
    hash_fn = hashlib.new(hash_algorithm)
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            hash_fn.update(chunk)
    return hash_fn.hexdigest()


def tar(dml, path, bucket, prefix, excludes=(), boto_session=None):
    exclude_flags = [["--exclude", x] for x in excludes]
    exclude_flags = [y for x in exclude_flags for y in x]
    with NamedTemporaryFile(suffix=".tar") as tmpf:
        dml(
            "util",
            "tar",
            *exclude_flags,
            str(path),
            tmpf.name,
        )
        hash_ = compute_file_hash(tmpf.name)
        key = f"{prefix}/{hash_}.tar"
        client = (boto_session or boto3).client("s3")
        client.upload_file(tmpf.name, bucket, key)
    return Resource(f"s3://{bucket}/{key}")
