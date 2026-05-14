"""Codec registry, built-in codecs, and DAG-owned staging helpers."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata
from threading import RLock
from typing import TYPE_CHECKING, Any, Iterator, Protocol

from daggerml._internal import Error, Runnable
from daggerml._internal._db import Ref

if TYPE_CHECKING:
    from daggerml.api import Dag, Node


LITERAL_CODEC_ENTRYPOINT_GROUP = "daggerml.codecs"


class CodecError(Error):
    def __init__(self, message: str):
        super().__init__(message, origin="dml-codec", type="codec-error")


@dataclass(frozen=True)
class DelayedRef:
    name: str


@dataclass(frozen=True)
class DelayedLoad:
    dagname: str
    nodename: str | None = None


@dataclass(frozen=True)
class DelayedRunnable:
    uri: str
    adapter: str
    sub: Any | DelayedRunnable | None
    kwargs: dict[str, Any]


class LiteralCodec(Protocol):
    def can_encode(self, value: Any) -> bool: ...

    def encode(self, value: Any, dag: "Dag") -> Any: ...


_literal_codecs: list[tuple[int, int, LiteralCodec]] = []
_literal_codec_seq = 0
_plugins_loaded = False
_literal_codec_max_reencodes = 64
_lock = RLock()


def _is_codec(value: Any) -> bool:
    return callable(getattr(value, "can_encode", None)) and callable(getattr(value, "encode", None))


def _register_unlocked(codec: LiteralCodec, *, priority: int) -> None:
    global _literal_codec_seq
    _literal_codec_seq += 1
    _literal_codecs.append((priority, _literal_codec_seq, codec))
    _literal_codecs.sort(key=lambda item: (-item[0], item[1]))


def register_codec(codec: LiteralCodec, *, priority: int = 0) -> None:
    with _lock:
        _register_unlocked(codec, priority=priority)


def _entry_points() -> list[metadata.EntryPoint]:
    points = metadata.entry_points()
    result = list(points.select(group=LITERAL_CODEC_ENTRYPOINT_GROUP))
    result.sort(key=lambda ep: (ep.name, ep.value))
    return result


def _register_plugin_value(value: Any, *, source: str) -> None:
    if _is_codec(value):
        _register_unlocked(value, priority=0)
        return
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[1], int) and _is_codec(value[0]):
        _register_unlocked(value[0], priority=value[1])
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _register_plugin_value(item, source=source)
        return
    raise CodecError(f"Literal codec plugin '{source}' returned invalid codec registration")


def ensure_literal_codec_plugins_loaded() -> None:
    global _plugins_loaded
    with _lock:
        if _plugins_loaded:
            return
        for entry_point in _entry_points():
            source = f"{entry_point.name} ({entry_point.value})"
            try:
                loaded = entry_point.load()
                if _is_codec(loaded):
                    _register_unlocked(loaded, priority=0)
                    continue
                value = loaded() if callable(loaded) else loaded
                _register_plugin_value(value, source=source)
            except CodecError:
                raise
            except Exception as e:
                raise CodecError(f"Literal codec plugin '{source}' failed: {e}") from e
        _plugins_loaded = True


def iter_literal_codecs() -> Iterator[LiteralCodec]:
    ensure_literal_codec_plugins_loaded()
    with _lock:
        codecs = [codec for _priority, _seq, codec in _literal_codecs]
    yield from codecs


def _values_equal(a: Any, b: Any) -> bool:
    if a is b:
        return True
    try:
        return bool(a == b)
    except Exception:
        return False


def _is_repo_error(value: Exception) -> bool:
    return type(value).__name__ == "DmlRepoError"


def apply_codec(value: Any, *, dag: "Dag") -> Any:
    current = value
    reencode_count = 0

    while True:
        matched = False
        for codec in iter_literal_codecs():
            try:
                if not codec.can_encode(current):
                    continue
                matched = True
                encoded = codec.encode(current, dag)
            except CodecError:
                raise
            except Exception as e:
                if _is_repo_error(e):
                    raise
                raise CodecError(f"Literal codec {codec.__class__.__name__} failed: {e}") from e

            if _values_equal(encoded, current):
                return encoded

            current = encoded
            reencode_count += 1
            if reencode_count > _literal_codec_max_reencodes:
                raise CodecError("Literal codec recursion failed to converge")
            break
        if not matched:
            return current


def normalize_codec_value(value: Any, *, dag: "Dag") -> Any:
    value = apply_codec(value, dag=dag)
    if isinstance(value, (list, tuple)):
        return [normalize_codec_value(v, dag=dag) for v in value]
    if isinstance(value, dict):
        return {k: normalize_codec_value(v, dag=dag) for k, v in value.items()}
    if isinstance(value, Runnable):
        target = normalize_codec_value(value.target, dag=dag)
        sub = normalize_codec_value(value.sub, dag=dag) if value.sub is not None else None
        kwargs = {k: normalize_codec_value(v, dag=dag) for k, v in value.kwargs.items()}
        return Runnable(target=target, adapter=value.adapter, kwargs=kwargs, sub=sub)
    return value


def stage_value(dag: "Dag", value: Any, *, name: str | None = None) -> Ref:
    normalized = normalize_codec_value(value, dag=dag)
    return dag._put_literal(normalized, name=name)


class NodeCodec:
    def can_encode(self, value: Any) -> bool:
        from daggerml import api as core_api

        return isinstance(value, core_api.Node)

    def encode(self, value: "Node", dag: "Dag") -> Ref:
        assert dag.token is not None, "DAG must have a token to encode nodes"
        if value.dag.token is not None and value.dag.token == dag.token:
            return value.ref
        if value.dag.ref is None:
            raise CodecError("Cannot encode node from uncommitted DAG in a different index")
        try:
            return dag.dml.runtime.put_import(dag._require_index_ref(), value.dag.ref, node=value.ref, name=None)
        except Exception as e:
            raise CodecError(f"Failed to encode cross-dag node import: {e}") from e


class DelayedActionCodec:
    def can_encode(self, value: Any) -> bool:
        return isinstance(value, (DelayedRef, DelayedLoad, DelayedRunnable))

    def encode(self, value: DelayedRef | DelayedLoad | DelayedRunnable, dag: "Dag"):
        if isinstance(value, DelayedRef):
            return dag[value.name].ref
        if isinstance(value, DelayedRunnable):
            from daggerml.contrib.adapter_registry import get_adapter

            adapter_spec = get_adapter(value.adapter)
            sub = value.sub
            if isinstance(sub, DelayedRunnable):
                sub = normalize_codec_value(sub, dag=dag)
            resolved = adapter_spec.resolve_runnable(value.uri, value.kwargs, sub)
            if not isinstance(resolved, Runnable):
                raise CodecError("Adapter resolve_runnable must return Runnable")
            return resolved
        dag_ref, node_ref = dag.dml._index_ops().resolve_dag_node(
            dag._require_index_ref(), value.dagname, value.nodename
        )
        return dag.dml.runtime.put_import(dag._require_index_ref(), dag_ref, node=node_ref, name=None)


def codecs() -> list[Any]:
    return [NodeCodec(), DelayedActionCodec()]
