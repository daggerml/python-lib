"""
DaggerML - A Python library for building and managing directed acyclic graphs.

This library provides tools for creating, manipulating, and executing DAGs
with strong typing support and a context-manager based interface.
"""

from daggerml.api import (
    Dag,
    Dml,
    Error,
    Node,
    Ref,
    Runnable,
    Uri,
    clear_default_dml,
    get_default_dml,
    load,
    new,
    set_default_dml,
    status,
    use_default_dml,
)

try:
    from daggerml.__about__ import __version__
except ImportError:
    __version__ = "local"

temporary = Dml.temporary

__all__ = (
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
)
