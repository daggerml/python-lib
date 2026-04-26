from __future__ import annotations

import fnmatch
import hashlib
import io
import json
import os
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal, cast
from urllib.parse import urlparse

from daggerml import Node, Uri
from daggerml._config import DmlConfig
from daggerml._internal.types import DmlRepoError


def is_s3_uri(value: str) -> bool:
    p = urlparse(value)
    return p.scheme == "s3" and bool(p.netloc) and bool(p.path and p.path != "/")


def _boto3_client(service: str):
    try:
        import boto3
    except Exception as e:
        raise DmlRepoError(f"S3Store requires boto3: {e}") from e
    return boto3.client(service)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _flatten_names(*name_or_uris):
    if len(name_or_uris) == 1 and isinstance(name_or_uris[0], (list, tuple)):
        return list(name_or_uris[0])
    return list(name_or_uris)


def _validate_safe_extract_path(*, dest_path: Path, member_name: str) -> None:
    member_path = Path(member_name)
    if member_path.is_absolute():
        raise DmlRepoError(f"Refusing to extract absolute tar path: {member_name}")
    target_path = (dest_path / member_path).resolve()
    if os.path.commonpath([str(dest_path), str(target_path)]) != str(dest_path):
        raise DmlRepoError(f"Refusing to extract path outside destination: {member_name}")


@dataclass(frozen=True)
class S3Store:
    bucket: str | None = None
    prefix: str | None = None
    client: Any = None

    def __post_init__(self):
        bucket = self.bucket
        prefix = self.prefix
        if bucket is None and prefix is None:
            cfg = DmlConfig.resolve()
            remote_root = cfg.remote.root
            if not remote_root:
                raise DmlRepoError(
                    "S3Store requires configured remote.root (set DML_REMOTE_ROOT or pass bucket/prefix)"
                )
            p = urlparse(remote_root)
            if p.scheme != "s3" or not p.netloc:
                raise DmlRepoError("remote.root must be an s3:// URI")
            bucket = p.netloc
            base = p.path.lstrip("/").rstrip("/")
            prefix = f"{base}/data" if base else "data"
        if bucket is None:
            raise DmlRepoError("S3Store bucket not configured")
        if prefix is None:
            prefix = ""
        object.__setattr__(self, "bucket", bucket)
        object.__setattr__(self, "prefix", prefix.strip("/"))
        object.__setattr__(self, "client", self.client or _boto3_client("s3"))

    @classmethod
    def from_remote_root(cls, remote_root: str) -> "S3Store":
        p = urlparse(remote_root)
        if p.scheme != "s3" or not p.netloc:
            raise DmlRepoError("remote root must be an s3:// URI")
        base = p.path.lstrip("/").rstrip("/")
        prefix = f"{base}/data" if base else "data"
        return cls(bucket=p.netloc, prefix=prefix)

    def parse_uri(self, name_or_uri) -> tuple[str, str]:
        if isinstance(name_or_uri, Node):
            name_or_uri = name_or_uri.value()
        if isinstance(name_or_uri, Uri):
            name_or_uri = name_or_uri.uri
        if not isinstance(name_or_uri, str):
            raise DmlRepoError("S3Store name_or_uri must be a string or uri-bearing object")
        p = urlparse(name_or_uri)
        if p.scheme == "s3":
            return p.netloc, p.path[1:]
        if self.bucket is None:
            raise DmlRepoError("S3Store bucket not configured")
        key = f"{self.prefix}/{name_or_uri}" if self.prefix else name_or_uri
        return cast(str, self.bucket), key

    def _name2uri(self, name) -> Uri:
        bucket, key = self.parse_uri(name)
        return Uri(f"s3://{bucket}/{key}")

    def put(self, data: bytes | None = None, filepath: str | None = None, *, suffix: str = "") -> Uri:
        if (data is None) == (filepath is None):
            raise DmlRepoError("S3Store.put requires exactly one of data or filepath")
        if data is None:
            # filepath is not None from previous check
            assert filepath is not None
            data = Path(filepath).read_bytes()
        name = _sha256_bytes(data) + suffix
        bucket, key = self.parse_uri(name)
        self.client.put_object(Bucket=bucket, Key=key, Body=data)
        return Uri(f"s3://{bucket}/{key}")

    def get(self, name_or_uri) -> bytes:
        bucket, key = self.parse_uri(name_or_uri)
        obj = self.client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()

    def exists(self, name_or_uri) -> bool:
        bucket, key = self.parse_uri(name_or_uri)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def ls(self, s3_root=None, *, recursive: bool = False, lazy: bool = False):
        bucket, prefix = self.parse_uri(s3_root or self._name2uri(""))
        if prefix:
            prefix = prefix.rstrip("/") + "/"
        kw: dict[str, Any] = {}
        if not recursive:
            kw["Delimiter"] = "/"
        paginator = self.client.get_paginator("list_objects_v2")

        def _iter():
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, **kw):
                for obj in page.get("Contents", []):
                    yield Uri(f"s3://{bucket}/{obj['Key']}")

        out = _iter()
        if lazy:
            return out
        return list(out)

    def rm(self, *name_or_uris):
        values = _flatten_names(*name_or_uris)
        if not values:
            return
        grouped: dict[str, list[str]] = {}
        for item in values:
            bucket, key = self.parse_uri(item)
            grouped.setdefault(bucket, []).append(key)
        for bucket, keys in grouped.items():
            for i in range(0, len(keys), 1000):
                batch = keys[i : i + 1000]
                self.client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})

    def put_js(self, data: Any) -> Uri:
        encoded = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return self.put(data=encoded, suffix=".json")

    def get_js(self, name_or_uri):
        return json.loads(self.get(name_or_uri).decode("utf-8"))

    def tar(
        self,
        path: str | os.PathLike[str],
        excludes: Iterable[str] = (),
        *,
        symlinks: Literal["ignore", "raise"] = "raise",
    ) -> Uri:
        root = Path(path).resolve()
        if not root.exists() or not root.is_dir():
            raise DmlRepoError("S3Store.tar path must be an existing directory")
        if symlinks not in {"ignore", "raise"}:
            raise DmlRepoError("S3Store.tar symlinks must be 'ignore' or 'raise'")
        patterns = list(excludes)
        buf = io.BytesIO()

        def excluded(rel: str) -> bool:
            return any(fnmatch.fnmatch(rel, pat) for pat in patterns)

        def normalize(info: tarfile.TarInfo) -> tarfile.TarInfo:
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            return info

        with tarfile.open(fileobj=buf, mode="w") as tf:
            for dirpath, dirnames, filenames in os.walk(root):
                dirpath = Path(dirpath)
                rel_dir = dirpath.relative_to(root).as_posix()

                kept_dirnames = []
                for dirname in sorted(dirnames):
                    child = dirpath / dirname
                    rel = child.relative_to(root).as_posix()
                    if excluded(rel):
                        continue
                    if child.is_symlink():
                        if symlinks == "raise":
                            raise DmlRepoError(f"S3Store.tar encountered symlink with symlinks='raise': {rel}")
                        continue
                    kept_dirnames.append(dirname)
                dirnames[:] = kept_dirnames

                if rel_dir != ".":
                    tf.addfile(normalize(tf.gettarinfo(str(dirpath), arcname=rel_dir)))

                for filename in sorted(filenames):
                    p = dirpath / filename
                    rel = p.relative_to(root).as_posix()
                    if excluded(rel):
                        continue
                    if p.is_symlink():
                        if symlinks == "raise":
                            raise DmlRepoError(f"S3Store.tar encountered symlink with symlinks='raise': {rel}")
                        continue
                    with p.open("rb") as f:
                        tf.addfile(normalize(tf.gettarinfo(str(p), arcname=rel)), fileobj=f)
        return self.put(data=buf.getvalue(), suffix=".tar")

    def untar(self, tar_uri, dest: str | os.PathLike[str], *, unsafe: bool = False) -> None:
        payload = self.get(tar_uri)
        dest_path = Path(dest)
        dest_path.mkdir(parents=True, exist_ok=True)
        resolved_dest = dest_path.resolve()
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r") as tf:
            members = tf.getmembers()
            if not unsafe:
                for member in members:
                    _validate_safe_extract_path(dest_path=resolved_dest, member_name=member.name)
                tf.extractall(dest_path, members=members)
                return
            try:
                tf.extractall(dest_path, members=members, filter="fully_trusted")
            except TypeError:
                tf.extractall(dest_path, members=members)

    def cd(self, new_prefix: str) -> "S3Store":
        current = Path("/" + self.prefix) if self.prefix else Path("/")
        next_prefix = (current / new_prefix).resolve().as_posix().lstrip("/")
        if next_prefix == ".":
            next_prefix = ""
        return S3Store(bucket=self.bucket, prefix=next_prefix, client=self.client)
