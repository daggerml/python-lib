"""Utility functions for the DML repository system.

Public API:
    unnest - Flatten a list of lists
    some - Return first truthy value or default
    assert_exactly_one - Assert exactly one non-None value
    makedirs - Create directories with secure permissions
    readfile - Read file contents
    writefile - Write file contents
    fullname - Get full qualified name of object
    now - Get current UTC time as ISO string
    as_list - Ensure value is a list
    merge_counters - Merge counter dictionaries
    tree_map - Apply function to tree structure
"""

from __future__ import annotations

import os
import secrets
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable


def unnest(nested: Iterable[Iterable[Any]]) -> list:
    return [x for xs in nested for x in xs]


def some(xs, default=None):
    return next((x for x in xs if x), default)


def assert_exactly_one(*objs, message=None):
    """
    Asserts that exactly one of the provided objects is not None.
    """
    count = sum(1 for v in objs if v is not None)
    if count != 1:
        raise ValueError(
            message or f"Exactly one of the provided values must be non-None, but found {count} non-None values: {objs}"
        )


def makedirs(path):
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


def readfile(path, *paths):
    if path is not None:
        p = os.path.join(path, *paths)
        if os.path.exists(p):
            with open(p) as f:
                result = f.read().strip()
                return result or None


def writefile(contents, path, *paths):
    if path is not None:
        p = os.path.join(path, *paths)
        if contents is None:
            if os.path.exists(p):
                os.remove(p)
        else:
            os.makedirs(os.path.dirname(p), mode=0o700, exist_ok=True)
            with open(p, "w") as f:
                f.write(contents)


def fullname(obj):
    if not isinstance(obj, type):
        return fullname(type(obj))
    return f"{obj.__module__}.{obj.__qualname__}"


def now():
    return datetime.now(timezone.utc).isoformat()


def as_list(x) -> list:
    return list(x) if isinstance(x, (list, tuple)) else [x]


def merge_counters(x, *xs):
    if not len(xs):
        return x
    y, rest = xs[0], xs[1:]
    result = {}
    for k in set(x.keys()).union(set(y.keys())):
        result[k] = unnest([as_list(x.get(k, 0)), as_list(y.get(k, 0))])
    return merge_counters(result, *rest) if len(rest) else result


def tree_map(predicate, fn, item):
    if predicate(item):
        item = fn(item)
    if isinstance(item, list):
        return [tree_map(predicate, fn, x) for x in item]
    if isinstance(item, dict):
        return {k: tree_map(predicate, fn, v) for k, v in item.items()}
    return item


def uuid7() -> uuid.UUID:
    """Temporally orderable UUID (up to the millisecond)"""
    # Unix timestamp in milliseconds (48 bits)
    ts_ms = int(time.time_ns() // 1_000_000) & ((1 << 48) - 1)
    # 80 random bits
    rand = secrets.randbits(80)
    # Layout:
    #
    # 48b timestamp
    # 4b version (0111)
    # 12b rand_a
    # 2b variant (10)
    # 62b rand_b
    value = 0
    # timestamp
    value |= ts_ms << 80
    # version
    value |= 0x7 << 76
    # rand_a (12 bits)
    value |= ((rand >> 68) & 0xFFF) << 64
    # variant (RFC 4122 / RFC 9562)
    value |= 0b10 << 62
    # rand_b (62 bits)
    value |= rand & ((1 << 62) - 1)
    return uuid.UUID(int=value)
