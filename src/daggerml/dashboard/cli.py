"""Command-line launcher for the optional local DaggerML dashboard."""

from __future__ import annotations

import argparse
import ipaddress
import secrets
import sys
import threading
import webbrowser
from pathlib import Path
from urllib.parse import quote


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dml-dashboard",
        description="Launch the local DaggerML research dashboard.",
    )
    parser.add_argument(
        "--config-home",
        "--config-dir",
        dest="config_home",
        default=None,
        help="DaggerML configuration directory (default: DML_CONFIG_HOME or the platform default).",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Address to bind (default: 127.0.0.1).")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind (default: 8765).")
    parser.add_argument("--no-open", action="store_true", help="Do not open the dashboard in a browser.")
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow a non-loopback bind. Requires the ephemeral bearer token printed at startup.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    loopback = _is_loopback(args.host)
    if not loopback and not args.allow_remote:
        print(
            "error: refusing a non-loopback bind without --allow-remote; "
            "the dashboard can inspect and cancel live executions",
            file=sys.stderr,
        )
        return 2
    if not 1 <= args.port <= 65535:
        print("error: --port must be between 1 and 65535", file=sys.stderr)
        return 2
    try:
        import uvicorn

        from daggerml.dashboard.server import create_app
    except ImportError as exc:
        print(
            'error: dashboard dependencies are not installed; install with pip install "daggerml[dashboard]"',
            file=sys.stderr,
        )
        if exc.name not in {"fastapi", "starlette", "uvicorn"}:
            print(f"error: {exc}", file=sys.stderr)
        return 1
    config_home = str(Path(args.config_home).expanduser().resolve()) if args.config_home else None
    auth_token = secrets.token_urlsafe(32) if not loopback else None
    app = create_app(config_home=config_home, auth_token=auth_token)
    display_host = "127.0.0.1" if args.host in {"0.0.0.0", "::"} else args.host
    base_url = f"http://{display_host}:{args.port}"
    browser_url = base_url if auth_token is None else f"{base_url}/#token={quote(auth_token, safe='')}"
    print(f"DaggerML dashboard: {base_url}")
    print(f"Configuration: {app.state.projects.config_home}")
    if app.state.projects.default_project is not None:
        print(f"Project: {app.state.projects.default_project}")
    if auth_token is not None:
        print("WARNING: remote binding enabled; anyone with this ephemeral token can inspect and cancel executions.")
        print(f"Bearer token: {auth_token}")
    if not args.no_open:
        threading.Timer(0.6, webbrowser.open, args=(browser_url,)).start()
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0
