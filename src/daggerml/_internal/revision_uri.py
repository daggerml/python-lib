from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlsplit

_SEGMENT_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


def validate_segment(label: str, value: str) -> str:
    if not isinstance(value, str) or not _SEGMENT_RE.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def validate_ref_name(label: str, value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Invalid {label}: must be a non-empty string")
    if value in {".", ".."} or "\\" in value:
        raise ValueError(f"Invalid {label}: {value!r}")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Invalid {label}: {value!r}")
    for part in parts:
        validate_segment(f"{label} segment", part)
    return value


@dataclass(frozen=True)
class RevisionUri:
    owner: str
    project: str
    branch: str | None = None
    tag: str | None = None

    def __post_init__(self) -> None:
        validate_segment("project owner", self.owner)
        validate_segment("project name", self.project)
        if (self.branch is None) == (self.tag is None):
            raise ValueError("Revision URI must include exactly one selector (branch xor tag)")
        if self.branch is not None:
            validate_ref_name("branch", self.branch)
        if self.tag is not None:
            validate_ref_name("tag", self.tag)


def stringify_revision_uri(uri: RevisionUri) -> str:
    base = f"dml://{uri.owner}/{uri.project}"
    if uri.branch is not None:
        return f"{base}#{uri.branch}"
    return f"{base}@{uri.tag}"


def parse_revision_uri(
    uri: str,
    *,
    default_branch: str | None = None,
    require_identifier: bool = False,
) -> RevisionUri:
    if not isinstance(uri, str) or not uri.startswith("dml://"):
        raise ValueError(f"Invalid DML URI: {uri!r}")
    if "#" in uri and "@" in uri:
        raise ValueError(f"Invalid DML URI: cannot include both branch and tag: {uri!r}")

    base = uri
    branch: str | None = None
    tag: str | None = None
    if "#" in uri:
        base, branch = uri.split("#", 1)
    elif "@" in uri:
        base, tag = uri.split("@", 1)

    parsed = urlsplit(base)
    if parsed.scheme != "dml" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError(f"Invalid DML URI: {uri!r}")
    project = parsed.path.strip("/")
    if "/" in project or not project:
        raise ValueError(f"Invalid DML URI project path: {uri!r}")

    if branch is None and tag is None:
        if default_branch is not None:
            branch = validate_ref_name("branch", default_branch)
        elif require_identifier:
            raise ValueError(f"DML URI must include a branch or tag: {uri!r}")
        else:
            raise ValueError("Revision URI parser requires default_branch when selector is omitted")

    return RevisionUri(
        owner=validate_segment("project owner", parsed.netloc),
        project=validate_segment("project name", project),
        branch=validate_ref_name("branch", branch) if branch is not None else None,
        tag=validate_ref_name("tag", tag) if tag is not None else None,
    )


def canonicalize_revision_uri(
    uri: str,
    *,
    default_branch: str | None = None,
    require_identifier: bool = False,
) -> str:
    canonical = stringify_revision_uri(
        parse_revision_uri(uri, default_branch=default_branch, require_identifier=require_identifier)
    )
    if len(canonical.encode("utf-8")) > 64:
        raise ValueError("Canonical DML URI exceeds 64-byte ref limit")
    return canonical
