from __future__ import annotations

from dataclasses import dataclass

from daggerml._core.db import Ref
from daggerml._core.head import _validate_ref_name


@dataclass(frozen=True)
class Revision:
    head: int | None = None
    name: str | None = None
    commit: Ref | None = None

    def __post_init__(self) -> None:
        kinds = [self.head is not None, self.name is not None, self.commit is not None]
        if sum(kinds) != 1:
            raise ValueError("Revision must specify exactly one selector")
        if self.head is not None:
            if self.head < 0:
                raise ValueError(f"Invalid HEAD revision: {self.head}")
        if self.name is not None:
            _validate_ref_name("revision name", self.name)
        if self.commit is not None and self.commit.ns() != "commit":
            raise ValueError(f"Expected commit ref, got: {self.commit}")

    @classmethod
    def from_str(cls, value: str) -> "Revision":
        if not isinstance(value, str) or not value:
            raise ValueError("Revision is required")
        if value.startswith("origin/"):
            raise ValueError(f"Unsupported named-remote selector: {value}")
        if value.startswith("dml://"):
            raise ValueError(f"Unsupported revision: {value}")
        if value == "HEAD":
            return cls(head=0)
        if value.startswith("HEAD~"):
            try:
                steps = int(value[5:], 10)
            except ValueError as exc:
                raise ValueError(f"Unsupported revision: {value}") from exc
            return cls(head=steps)
        if value.startswith("commit:"):
            return cls(commit=Ref(value))
        if len(value) == 64 and all(ch in "0123456789abcdef" for ch in value):
            return cls(commit=Ref(f"commit:{value}"))
        return cls(name=value)

    @property
    def kind(self) -> str:
        if self.head is not None:
            return "head"
        if self.name is not None:
            return "name"
        if self.commit is not None:
            return "commit"
        raise AssertionError("Revision selector is missing")

    def __str__(self) -> str:
        if self.head is not None:
            if self.head == 0:
                return "HEAD"
            return f"HEAD~{self.head}"
        if self.name is not None:
            return self.name
        if self.commit is not None:
            return self.commit.to
        raise AssertionError("Revision selector is missing")
