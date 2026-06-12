"""Shared utility functions for the DML repository system."""

from __future__ import annotations

import secrets
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable


def unnest(nested: Iterable[Iterable[Any]]) -> list:
    return [x for xs in nested for x in xs]


def now():
    return datetime.now(timezone.utc).isoformat()


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
