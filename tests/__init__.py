import os
from uuid import uuid4

from daggerml import temporary as _temporary


def temporary_dml(*, repo: str | None = None, remote_root: str | None = None, **kw):
    return _temporary(
        name=repo or f"repo-{uuid4().hex}",
        remote_uri=remote_root or os.environ["DML_REMOTE_URI"],
        **kw,
    )
