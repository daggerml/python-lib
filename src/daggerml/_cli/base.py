"""Base CLI functionality and common utilities."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from typing import Any

from daggerml._config import DmlConfig
from daggerml._internal import DmlOps
from daggerml._internal._db import DmlDbInvalidPathError, DmlDbInvalidRefError, Ref
from daggerml._internal.types import DmlRepoError, Runnable, Uri


class DmlJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder for Daggerml objects."""

    def default(self, obj):
        if isinstance(obj, Ref):
            return str(obj)
        if isinstance(obj, Uri):
            return {
                "__type__": "uri",
                "uri": obj.uri,
            }
        if isinstance(obj, Runnable):
            return {
                "__type__": "runnable",
                "target": obj.target,
                "sub": obj.sub,
                "kwargs": obj.kwargs,
                "adapter": obj.adapter,
            }
        return super().default(obj)


def get_repo_path(repo_arg: str | None) -> str:
    """Resolve repository path from args and environment."""
    cfg = DmlConfig.resolve(
        explicit={"repo": repo_arg},
        defaults={"repo": os.getcwd()},
    )
    return str(cfg.repo)


def get_ops_object(ops: DmlOps, op_name: str) -> Any:
    """Get ops object by operation name.

    For subsystem constructors (commit, head, index, dag, node, cache, gc),
    calls the method and returns the ops instance. For non-callable
    attributes, returns the attribute unchanged.

    Special case: remote returns the DmlOps instance so the command handler can
    provide runtime remote context/client and then call `ops.remote(...)`.
    """
    if op_name == "remote":
        return ops
    subsystem = getattr(ops, op_name)
    if callable(subsystem):
        return subsystem()
    return subsystem


def parse_ref(ref_string: str) -> Ref:
    """Parse string into Ref object."""
    return Ref(ref_string)


def setup_logging(verbose_level: int) -> None:
    """Configure logging based on verbosity level."""
    level = logging.WARNING  # silent by default
    if verbose_level >= 2:
        level = logging.DEBUG
    elif verbose_level == 1:
        level = logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr, format="%(levelname)s: %(message)s")


def output_json(data: Any) -> None:
    """Output compact JSON to stdout."""
    json.dump(data, sys.stdout, separators=(",", ":"), cls=DmlJsonEncoder)
    sys.stdout.write("\n")


def build_help_epilog(examples: Sequence[str]) -> str:
    """Format a consistent argparse epilog from example commands."""
    cleaned = [ex.strip() for ex in examples if ex and ex.strip()]
    if not cleaned:
        return ""
    lines = ["Examples:"]
    lines.extend(f"  {ex}" for ex in cleaned)
    return "\n".join(lines)


def apply_help_config(
    parser: argparse.ArgumentParser,
    *,
    description: str,
    examples: Sequence[str] | None = None,
) -> None:
    """Apply consistent help configuration to an argparse parser."""
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.description = description
    if examples:
        parser.epilog = build_help_epilog(examples)


def normalize_error_message(error: Exception, *, command: str | None) -> str:
    """Convert an exception into a consistent, actionable error message."""

    def _with_hint(message: str, hint: str) -> str:
        if not message:
            message = hint
        if hint and hint not in message:
            if message.endswith((".", "!", "?")):
                return f"{message} Hint: {hint}"
            return f"{message}. Hint: {hint}"
        return message

    message = str(error).strip() or type(error).__name__

    # Invalid ref formats should consistently mention `namespace:id`.
    if (
        isinstance(error, DmlDbInvalidRefError)
        or "invalid ref format" in message.lower()
        or "invalid ref" in message.lower()
    ):
        offending = None
        lowered = message.lower()
        if lowered.startswith("invalid ref format") and ":" in message:
            # e.g. "invalid ref format: <context>" (db layer)
            offending = message.split(":", 1)[1].strip() or None
        if message == "Invalid Ref format":
            offending = None
        base = "Invalid ref format (expected namespace:id)"
        message = f"{base}: {offending}" if offending else base

    if "Head reference must start with 'head:'" in message:
        message = "Invalid head ref (expected head:<name>)"

    # JSON parsing errors: surface the underlying decoder context if available.
    if isinstance(error, ValueError) and isinstance(getattr(error, "__cause__", None), json.JSONDecodeError):
        cause: json.JSONDecodeError = error.__cause__  # type: ignore[assignment]
        message = f"{message} ({cause.msg} at line {cause.lineno} column {cause.colno})"

    # Repository path errors should include recovery hints.
    if isinstance(error, (DmlDbInvalidPathError, FileNotFoundError, NotADirectoryError, PermissionError)):
        message = _with_hint(message, "pass --repo PATH or set DML_REPO")

    # Config errors are generally surfaced via DmlRepoError.
    if isinstance(error, DmlRepoError):
        lowered = message.lower()
        if "remote root" in lowered or "dml_remote_root" in lowered or "boto3" in lowered:
            message = _with_hint(message, "check required flags/env vars")

    if command:
        message = f"{command}: {message}"
    return message


def build_error_payload(error: Exception, *, command: str | None) -> dict[str, str]:
    """Build the structured JSON error payload with normalized message."""
    payload: dict[str, str] = {
        "error": normalize_error_message(error, command=command),
        "type": type(error).__name__,
    }
    if command:
        payload["command"] = command
    return payload


def output_error(error: Exception, command: str | None = None) -> None:
    """Output structured error JSON."""
    json.dump(build_error_payload(error, command=command), sys.stderr, separators=(",", ":"), cls=DmlJsonEncoder)
    sys.stderr.write("\n")


def _command_context_from_args(args: Any) -> str | None:
    parts: list[str] = []
    op = getattr(args, "op", None)
    if op:
        parts.append(str(op))
    for attr in ("subcommand", "method"):
        value = getattr(args, attr, None)
        if value:
            parts.append(str(value))
    return " ".join(parts) if parts else None


def execute_command(args) -> None:
    """Execute CLI command with repository context and error handling."""
    try:
        if not getattr(args, "func", None):
            raise ValueError("Missing command method; run with --help for available methods")
        if args.op in {"init", "contrib"}:
            result = args.func(args)
            if isinstance(result, str):
                sys.stdout.write(result)
                if not result.endswith("\n"):
                    sys.stdout.write("\n")
            else:
                output_json(result)
            return
        repo_path = get_repo_path(args.repo)
        cfg = DmlConfig.resolve(
            explicit={
                "repo": repo_path,
                "remote.root": getattr(args, "remote_root", None),
                "remote.cache": getattr(args, "remote_cache", None),
            }
        ).with_repo_defaults()
        open_kwargs: dict[str, Any] = {}
        if cfg.remote.root is not None:
            open_kwargs["remote_root"] = cfg.remote.root
        if cfg.remote.cache is not None:
            open_kwargs["remote_cache"] = cfg.remote.cache
        with DmlOps.open(repo_path, **open_kwargs) as ops:
            ops_obj = get_ops_object(ops, args.op)
            result = args.func(ops_obj, args)
            output_json(result)
    except Exception as e:
        output_error(e, _command_context_from_args(args))
