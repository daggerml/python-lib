from __future__ import annotations

import inspect
from collections.abc import Callable
from contextlib import chdir
from dataclasses import dataclass
from functools import wraps
from tempfile import TemporaryDirectory
from typing import Any, Generic, TypeVar

from daggerml import Node
from daggerml._internal import DmlRepoError
from daggerml.contrib.api import DelayedRunnable

T = TypeVar("T")


@dataclass(frozen=True)
class MockNode(Generic[T]):
    _value: T

    def value(self) -> T:
        return self._value

    @classmethod
    def from_value(cls, value: T) -> MockNode | Node:
        if isinstance(value, (Node, MockNode)):
            return value
        return cls(value)


def wrap_node(arg: Any) -> Any:
    if isinstance(arg, (Node, MockNode)):
        return arg
    return MockNode(arg)


def defunkify(value: DelayedRunnable) -> Callable[..., Any]:
    current = value
    while isinstance(current.sub, DelayedRunnable):
        current = current.sub
    if current.uri != "script":
        raise DmlRepoError("defunkify requires innermost script delayed runnable")
    fn = current.kwargs.get("fn")
    if not callable(fn):
        raise DmlRepoError("defunkify requires callable fn in innermost script kwargs")

    sig = inspect.signature(fn)
    param_names = tuple(sig.parameters)

    @wraps(fn)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        for name, param in sig.parameters.items():
            if name not in bound.arguments or name == param_names[0]:
                continue
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                bound.arguments[name] = tuple(wrap_node(arg) for arg in bound.arguments[name])
            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                bound.arguments[name] = {key: wrap_node(arg) for key, arg in bound.arguments[name].items()}
            else:
                bound.arguments[name] = wrap_node(bound.arguments[name])
        with TemporaryDirectory(prefix="dml-defunkify-") as tmpd:
            with chdir(tmpd):
                return fn(*bound.args, **bound.kwargs)

    return wrapped


__all__ = ["MockNode", "defunkify"]
