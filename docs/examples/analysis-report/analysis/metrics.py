"""Small analysis helpers used by the multi-file documentation example."""


def summarize(values: list[int]) -> dict[str, int]:
    """Return the count, total, and largest observation."""
    return {"count": len(values), "total": sum(values), "largest": max(values)}
