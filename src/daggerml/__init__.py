"""DaggerML public package exports."""

from daggerml._core import (
    BadExecutionStatusError,
    CanceledExecutionError,
    CancellationError,
    Dml,
    Error,
    Ref,
    Runnable,
    Uri,
)
from daggerml.api import (
    Dag,
    Node,
    clear_default_dml,
    get_default_dml,
    load,
    new,
    set_default_dml,
    status,
    temporary,
    use_default_dml,
)

try:
    from daggerml.__about__ import __version__
except ImportError:
    __version__ = "local"

__all__ = (
    "BadExecutionStatusError",
    "CanceledExecutionError",
    "CancellationError",
    "Dag",
    "Dml",
    "Error",
    "Node",
    "Ref",
    "Uri",
    "Runnable",
    "get_default_dml",
    "set_default_dml",
    "use_default_dml",
    "clear_default_dml",
    "new",
    "load",
    "status",
    "temporary",
)
