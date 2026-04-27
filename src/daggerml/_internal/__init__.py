"""DML Repository Native Implementation.

This package provides the native implementation of the DML repository system.
The public API is deliberately minimal to provide a clean, stable interface
while keeping internal implementation details private.

Public API:
    DmlOps - Main repository operations facade
    Ref - Reference to objects stored in repository
    Uri - External URI datum
    Runnable - Executable datum with defaults/adapter
    Error - Computation error representation
    DmlRepoError - Base exception for repository operations
    DEFAULT_HEAD - Default branch reference
    DEFAULT_USER - Default user identifier

All other functionality is accessed through the Repo class methods.
Internal modules (repo_core, vcs, gc, dag_runtime, etc.) are implementation
details and should not be used directly.
"""

# Public exports only.
from daggerml._internal._db import Ref
from daggerml._internal.codec import CodecContext, apply_codec, register_codec
from daggerml._internal.ops import DmlOps
from daggerml._internal.types import (
    DEFAULT_HEAD,
    DEFAULT_USER,
    DmlRepoError,
    Error,
    Runnable,
    Uri,
)

# Make the main classes available at package level
__all__ = [
    "DmlOps",
    "Ref",
    "apply_codec",
    "register_codec",
    "CodecContext",
    "Uri",
    "Runnable",
    "Error",
    "DmlRepoError",
    "DEFAULT_HEAD",
    "DEFAULT_USER",
]
