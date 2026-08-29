"""Bounded CloudWatch log readers for persisted execution resources."""

from typing import Any


def read_cloudwatch_log(
    client: Any,
    cache_key: str,
    stream: str,
    *,
    cursor: str | None = None,
    limit: int = 1_000,
) -> dict:
    """Read canonical DaggerML CloudWatch events without exposing AWS metadata."""
    if stream not in {"stdout", "stderr"}:
        raise ValueError("stream must be stdout or stderr")
    kwargs: dict[str, Any] = {
        "logGroupName": "dml",
        "logStreamName": f"/run/{cache_key}/{stream}",
        "startFromHead": True,
        "limit": max(1, min(int(limit), 10_000)),
    }
    if cursor:
        kwargs["nextToken"] = cursor
    response = client.get_log_events(**kwargs)
    events = [
        {"timestamp": event.get("timestamp"), "message": str(event.get("message", ""))}
        for event in response.get("events", [])
    ]
    return {
        "source": "cloudwatch",
        "stream": stream,
        "events": events,
        "cursor": cursor,
        "next_cursor": response.get("nextForwardToken"),
        "has_more": bool(events),
    }
