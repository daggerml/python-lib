from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

_CURRENT_EXECUTION_ID: ContextVar[str | None] = ContextVar("daggerml_current_execution_id", default=None)
_CURRENT_CACHE_KEY: ContextVar[str | None] = ContextVar("daggerml_current_cache_key", default=None)


def get_current_execution_context() -> tuple[str | None, str | None]:
    return _CURRENT_EXECUTION_ID.get(), _CURRENT_CACHE_KEY.get()


@contextmanager
def execution_context(execution_id: str | None, cache_key: str | None) -> Iterator[None]:
    execution_token = _CURRENT_EXECUTION_ID.set(execution_id)
    cache_token = _CURRENT_CACHE_KEY.set(cache_key)
    try:
        yield
    finally:
        _CURRENT_EXECUTION_ID.reset(execution_token)
        _CURRENT_CACHE_KEY.reset(cache_token)
