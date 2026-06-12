"""S3-backed CAS"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Literal, overload
from urllib.parse import urlparse

if TYPE_CHECKING:
    import boto3


@dataclass
class CasItem:
    """S3-backed content-addressable storage item for function execution."""

    key: str
    data: str
    etag: str

    @property
    def json(self) -> dict | list | str | int | float | bool | None:
        """Get the body as JSON."""
        return json.loads(self.data)


class CasItemConflict(Exception):
    """Raised when a CAS item is updated by another process between read and write."""


@dataclass
class S3Remote:
    """S3 location for CAS items."""

    s3_uri: str
    client: "boto3.client"

    @property
    def bucket(self) -> str:
        uri = urlparse(self.s3_uri)
        return uri.netloc

    @property
    def prefix(self) -> str:
        uri = urlparse(self.s3_uri)
        return uri.path.lstrip("/")

    def _key_for(self, relative_key: str) -> str:
        """Join configured prefix and relative key without leading slash when prefix is empty."""
        return f"{self.prefix}/{relative_key}" if self.prefix else relative_key

    @staticmethod
    def _is_missing_error(exc: Exception) -> bool:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        return code in {"404", "NoSuchKey", "NotFound"}

    def _put(self, key: str | CasItem, value, *, overwrite: bool = True, **kwargs) -> bool:
        """Overwrite the body with new data and update the ETag."""
        kw = {}
        if not overwrite:
            kw["IfNoneMatch"] = "*"
        if isinstance(key, CasItem):
            kw["IfMatch"] = key.etag
            key = key.key
        body = value if isinstance(value, bytes) else value.encode()
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body, **kw, **kwargs)
            self.client.get_waiter("object_exists").wait(Bucket=self.bucket, Key=key)
            return True
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code in ("PreconditionFailed", "412"):
                raise CasItemConflict(f"CAS item {key} was updated by another process") from e
            raise

    def _put_js(self, key, value, *, overwrite: bool = True, **kwargs):
        """Overwrite the body with new data and update the ETag."""
        kwargs["ContentType"] = "application/json"
        value = json.dumps(value, separators=(",", ":"), sort_keys=True)
        return self._put(key, value, overwrite=overwrite, **kwargs)

    @overload
    def _get(self, key: str, *, cas: Literal[False] = False) -> str: ...
    @overload
    def _get(self, key: str, *, cas: Literal[True] = True) -> CasItem: ...
    def _get(self, key: str, *, cas: bool = False) -> str | CasItem:
        resp = self.client.get_object(Bucket=self.bucket, Key=key)
        data = resp["Body"].read().decode().strip()
        if cas:
            return CasItem(key, data, resp["ETag"].strip('"'))
        return data

    def _delete(self, key: str | CasItem, **kw) -> bool:
        """Delete an object; no-op if absent or CAS preconditions fail."""
        if isinstance(key, CasItem):
            kw["IfMatch"] = key.etag
        key = key.key if isinstance(key, CasItem) else key
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key, **kw)
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            if self._is_missing_error(exc):
                return False
            if code in ("PreconditionFailed", "412"):
                return False
            raise
        return True

    def _exists(self, key: str) -> bool:
        """Check if the object exists."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception as exc:
            if self._is_missing_error(exc):
                return False
            raise

    def _iter(self, prefix: str, keys: bool = True) -> Iterable[str]:
        """Iterate over keys with the given prefix."""
        paginator = self.client.get_paginator("list_objects_v2")
        kw = {"Bucket": self.bucket, "Prefix": prefix}
        if not keys:
            kw["Delimiter"] = "/"
        page_iterator = paginator.paginate(**kw)
        for page in page_iterator:
            if keys:
                yield from (obj["Key"] for obj in page.get("Contents", []))
            else:  # yield just the prefixes (subdirectories)
                yield from (pref["Prefix"] for pref in page.get("CommonPrefixes", []))
