"""Bounded local cache for validated custom-dashboard results."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

DEFAULT_MAX_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_ENTRY_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_AGE_SECONDS = 30 * 24 * 60 * 60
CACHE_SCHEMA = 1
RESULT_SCHEMA = 1


@dataclass(frozen=True)
class DashboardCacheIdentity:
    """Inputs whose changes invalidate one rendered dashboard result."""

    dashboard: str
    dag_ref: str
    distribution: str
    distribution_version: str
    cache_version: str
    result_schema: int = RESULT_SCHEMA

    @property
    def key(self) -> str:
        encoded = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        return hashlib.sha256(encoded).hexdigest()


def _strict_loads(raw: bytes) -> Any:
    return json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


class DashboardResultCache:
    """Atomic JSON-file cache with age and least-recently-used bounds."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_entry_bytes: int = DEFAULT_MAX_ENTRY_BYTES,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    ):
        self.root = Path(root)
        self.max_bytes = max_bytes
        self.max_entry_bytes = max_entry_bytes
        self.max_age_seconds = max_age_seconds
        self._lock = threading.RLock()
        self.cleanup()

    def path(self, identity: DashboardCacheIdentity) -> Path:
        return self.root / f"{identity.key}.json"

    def get(self, identity: DashboardCacheIdentity) -> dict[str, Any] | None:
        with self._lock:
            path = self.path(identity)
            try:
                stat = path.stat()
                if time.time() - stat.st_mtime > self.max_age_seconds or stat.st_size > self.max_entry_bytes:
                    path.unlink(missing_ok=True)
                    return None
                envelope = _strict_loads(path.read_bytes())
                if not isinstance(envelope, Mapping):
                    raise ValueError("cache envelope must be an object")
                if envelope.get("schema") != CACHE_SCHEMA or envelope.get("identity") != asdict(identity):
                    raise ValueError("cache identity is invalid")
                result = envelope.get("result")
                if not isinstance(result, Mapping):
                    raise ValueError("cache result must be an object")
                os.utime(path, None)
                return dict(result)
            except FileNotFoundError:
                return None
            except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                path.unlink(missing_ok=True)
                return None

    def put(self, identity: DashboardCacheIdentity, result: Mapping[str, Any]) -> dict[str, Any]:
        envelope = {"schema": CACHE_SCHEMA, "identity": asdict(identity), "result": dict(result)}
        encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        if len(encoded) > self.max_entry_bytes:
            raise ValueError(f"Dashboard result exceeds the {self.max_entry_bytes}-byte cache entry limit")
        with self._lock:
            self.root.mkdir(parents=True, exist_ok=True)
            temporary: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(dir=self.root, prefix=".tmp-", delete=False) as stream:
                    temporary = Path(stream.name)
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.path(identity))
                temporary = None
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            self.cleanup()
        return dict(result)

    def cleanup(self) -> None:
        with self._lock:
            if not self.root.exists():
                return
            now = time.time()
            entries: list[tuple[float, int, Path]] = []
            for path in self.root.iterdir():
                if not path.is_file():
                    continue
                if path.name.startswith(".tmp-"):
                    path.unlink(missing_ok=True)
                    continue
                if path.suffix != ".json":
                    continue
                try:
                    stat = path.stat()
                    envelope = _strict_loads(path.read_bytes())
                    if not isinstance(envelope, Mapping) or envelope.get("schema") != CACHE_SCHEMA:
                        raise ValueError("invalid cache envelope")
                except FileNotFoundError:
                    continue
                except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                    path.unlink(missing_ok=True)
                    continue
                if now - stat.st_mtime > self.max_age_seconds or stat.st_size > self.max_entry_bytes:
                    path.unlink(missing_ok=True)
                    continue
                entries.append((stat.st_mtime, stat.st_size, path))
            total = sum(size for _modified, size, _path in entries)
            for _modified, size, path in sorted(entries):
                if total <= self.max_bytes:
                    break
                path.unlink(missing_ok=True)
                total -= size
