from __future__ import annotations

from daggerml.contrib.executor_state import StateBase


class ExecutorBase:
    state_class = StateBase

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        raise NotImplementedError("Executor resolve_runnable method is not implemented")

    @classmethod
    def start(cls, *, runnable, argv_ptr, cache_key, remote, state=None):
        raise NotImplementedError("Executor start method is not implemented")

    @classmethod
    def poll(cls, *, state=None):
        raise NotImplementedError("Executor poll method is not implemented")

    @classmethod
    def gc(cls, *, state=None):
        raise NotImplementedError("Executor gc method is not implemented")
