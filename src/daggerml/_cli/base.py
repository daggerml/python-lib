"""Base CLI functionality and common utilities."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from typing import Any

from daggerml._internal import (
    Dml,
    DmlDbInvalidPathError,
    DmlDbInvalidRefError,
    DmlRepoError,
    Ref,
    Runnable,
    Uri,
)


class DmlJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder for Daggerml objects."""

    def default(self, obj):
        if isinstance(obj, Ref):
            return str(obj.to)
        if isinstance(obj, Uri):
            return obj.uri
        if isinstance(obj, Runnable):
            raise NotImplementedError("Runnable objects cannot be serialized to JSON directly")
        return super().default(obj)


def get_repo_path(project_home_arg: str | None) -> str:
    """Resolve project home path from args and environment."""
    project_home = Dml(project_home=project_home_arg).config.show()["project"]["home"]
    if not project_home:
        raise DmlRepoError("Local config requires project.home (--project-home or DML_PROJECT_HOME)")
    return project_home


def get_ops_object(dml: Dml, op_name: str) -> Any:
    """Return the shared Dml boundary for a CLI command."""
    return dml


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

    # JSON parsing errors: surface the underlying decoder context if available.
    if isinstance(error, ValueError) and isinstance(getattr(error, "__cause__", None), json.JSONDecodeError):
        cause: json.JSONDecodeError = error.__cause__  # type: ignore[assignment]
        message = f"{message} ({cause.msg} at line {cause.lineno} column {cause.colno})"

    # Repository path errors should include recovery hints.
    if isinstance(error, (DmlDbInvalidPathError, FileNotFoundError, NotADirectoryError, PermissionError)):
        message = _with_hint(message, "pass --project-home PATH or set DML_PROJECT_HOME")

    # Config errors are generally surfaced via DmlRepoError.
    if isinstance(error, DmlRepoError):
        lowered = message.lower()
        if "remote root" in lowered or "remote uri" in lowered or "dml_remote_uri" in lowered or "boto3" in lowered:
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
        if args.op == "init":
            result = args.func(args)
            if isinstance(result, str):
                sys.stdout.write(result)
                if not result.endswith("\n"):
                    sys.stdout.write("\n")
            else:
                output_json(result)
            return
        repo_path = get_repo_path(getattr(args, "project_home", None))
        resolved_remote_uri = Dml(
            project_home=repo_path,
            remote_uri=getattr(args, "runtime_remote_uri", None),
        ).config.show()["remote"]["uri"]
        dml = Dml(project_home=repo_path, remote_uri=resolved_remote_uri)
        ops_obj = get_ops_object(dml, args.op)
        result = args.func(ops_obj, args)
        if isinstance(result, str) and getattr(args, "raw_output", False):
            sys.stdout.write(result)
            if not result.endswith("\n"):
                sys.stdout.write("\n")
            return
        output_json(result)
    except Exception as e:
        output_error(e, _command_context_from_args(args))
