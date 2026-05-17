import os

from daggerml import temporary as _temporary


def temporary_dml(*, repo: str | None = None, remote_root: str | None = None, **kw):
    if repo is not None and "remote_project" not in kw:
        kw["remote_project"] = f"dml://test/{repo}"
    return _temporary(
        remote_uri=remote_root or os.environ["DML_REMOTE_ROOT"],
        **kw,
    )
