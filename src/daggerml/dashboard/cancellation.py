"""Confirmation and background driving for dashboard cancellation."""

from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class _Nonce:
    execution_id: str
    expires_at: float


class CancellationCoordinator:
    """Issue one-use nonces and safely re-drive cancellation to completion."""

    def __init__(self, model: Any, *, nonce_ttl: float = 60.0, drive_timeout: float = 60.0, interval: float = 0.5):
        self.model = model
        self.nonce_ttl = nonce_ttl
        self.drive_timeout = drive_timeout
        self.interval = interval
        self._nonces: dict[str, _Nonce] = {}
        self._lock = threading.Lock()
        self.audit: list[dict[str, Any]] = []

    def issue_nonce(self, execution_id: str) -> dict[str, Any]:
        # Validate the target before showing a destructive-action confirmation.
        self.model.execution(execution_id)
        token = secrets.token_urlsafe(32)
        expires_at = time.time() + self.nonce_ttl
        with self._lock:
            self._nonces[token] = _Nonce(execution_id, expires_at)
            self._prune_locked()
        return {"nonce": token, "execution_id": execution_id, "expires_at": int(expires_at)}

    def consume(self, execution_id: str, nonce: str) -> None:
        now = time.time()
        with self._lock:
            entry = self._nonces.pop(nonce, None)
            self._prune_locked(now)
        if entry is None or entry.execution_id != execution_id or entry.expires_at < now:
            raise PermissionError("Cancellation confirmation is invalid or expired")

    def start(self, execution_id: str, nonce: str) -> dict[str, Any]:
        self.consume(execution_id, nonce)
        summary = self.model.dml.runtime.cancel(execution_id, mode="full")
        self.audit.append({"execution_id": execution_id, "event": "planned", "timestamp": int(time.time())})
        return summary

    def drive(self, execution_id: str) -> None:
        deadline = time.monotonic() + self.drive_timeout
        while time.monotonic() < deadline:
            try:
                record = self.model.dml.runtime.read_execution_record(execution_id)
                if record["lifecycle"] in {"canceled", "succeeded", "failed"}:
                    self.audit.append(
                        {
                            "execution_id": execution_id,
                            "event": "terminal",
                            "lifecycle": record["lifecycle"],
                            "timestamp": int(time.time()),
                        }
                    )
                    return
                self.model.dml.runtime.cancel(execution_id, mode="drive")
            except Exception as exc:
                self.audit.append(
                    {
                        "execution_id": execution_id,
                        "event": "error",
                        "error": str(exc),
                        "timestamp": int(time.time()),
                    }
                )
                return
            time.sleep(self.interval)
        self.audit.append({"execution_id": execution_id, "event": "timeout", "timestamp": int(time.time())})

    def _prune_locked(self, now: float | None = None) -> None:
        now = time.time() if now is None else now
        for token, entry in list(self._nonces.items()):
            if entry.expires_at < now:
                self._nonces.pop(token, None)
