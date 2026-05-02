"""Literal codec registry and plugin loading."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata
from threading import RLock
from typing import TYPE_CHECKING, Any, Iterator, Protocol

from daggerml._internal.types import DmlRepoError

if TYPE_CHECKING:
    from daggerml._internal.ops.index import IndexOps

LITERAL_CODEC_ENTRYPOINT_GROUP = "daggerml.codecs"


class LiteralCodec(Protocol):
    def can_encode(self, value: Any) -> bool: ...

    def encode(self, value: Any, ctx: "CodecContext") -> Any: ...


@dataclass(frozen=True)
class CodecContext:
    index_id: str
    index_ops: "IndexOps"


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
    raise DmlRepoError(f"Literal codec plugin '{source}' returned invalid codec registration")


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
            except Exception as e:
                raise DmlRepoError(f"Literal codec plugin '{source}' failed: {e}") from e
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


def apply_codec(value: Any, *, ctx: CodecContext) -> Any:
    current = value
    reencode_count = 0

    while True:
        matched = False
        for codec in iter_literal_codecs():
            try:
                if not codec.can_encode(current):
                    continue
                matched = True
                encoded = codec.encode(current, ctx)
            except Exception as e:
                raise DmlRepoError(f"Literal codec {codec.__class__.__name__} failed: {e}") from e

            if _values_equal(encoded, current):
                return encoded

            current = encoded
            reencode_count += 1
            if reencode_count > _literal_codec_max_reencodes:
                raise DmlRepoError("Literal codec recursion failed to converge")
            break
        if not matched:
            return current
