"""Conservative Docker garbage collection and optional user-systemd scheduling."""

import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from daggerml.api import DmlRepoError
from daggerml.contrib.executors.docker import DockerExecutor, _image_lock

_OWNED_REPOSITORIES = {"daggerml-archive", "daggerml-execution"}
logger = logging.getLogger(__name__)


def _owned_tag(tag: str) -> bool:
    tag = tag.removeprefix("docker.io/library/").removeprefix("index.docker.io/library/")
    return tag.partition(":")[0] in _OWNED_REPOSITORIES


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _run_if_present(*args: str) -> str | None:
    """Ignore resources removed by Docker or other clients since enumeration."""
    try:
        return DockerExecutor._run_docker(*args)
    except DmlRepoError as exc:
        if any(marker in str(exc).lower() for marker in (f"no such {args[0]}:", "no such object:")):
            return None
        raise


def collect(*, all_resources: bool = False, older_than_hours: float = 24, dry_run: bool = False) -> dict[str, Any]:
    """Collect old stopped containers and unreferenced image tags.

    Parameters
    ----------
    all_resources : bool
        Include foreign resources, rather than only DML labels and tag prefixes.
    older_than_hours : float
        Minimum time since container exit/creation or image creation.
    dry_run : bool
        Report candidates without deleting them.

    Returns
    -------
    dict
        Container IDs and image references selected for removal.
    """
    if not 0 < older_than_hours < float("inf"):
        raise ValueError("older-than-hours must be a positive finite number")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
    run = DockerExecutor._run_docker
    removed: dict[str, Any] = {"containers": [], "images": [], "dry_run": dry_run}
    with _image_lock():
        container_ids = run("container", "ls", "-aq", "--no-trunc").split()
        containers = []
        for ident in container_ids:
            output = _run_if_present("container", "inspect", ident)
            if output is not None:
                containers.append(json.loads(output)[0])
        retained = []
        for container in containers:
            state = container["State"]
            owned = (container.get("Config", {}).get("Labels") or {}).get("daggerml.managed") == "true"
            finished = state.get("FinishedAt") or container["Created"]
            if finished.startswith("0001-"):
                finished = container["Created"]
            if (state.get("Status") in {"exited", "dead", "created"}
                    and (all_resources or owned)
                    and max(_timestamp(finished), _timestamp(container["Created"])) < cutoff):
                if not dry_run:
                    if _run_if_present("container", "rm", container["Id"]) is None:
                        continue
                removed["containers"].append(container["Id"])
            else:
                retained.append(container)
        referenced = {container["Image"] for container in retained}
        image_ids = set(run("image", "ls", "-aq", "--no-trunc").split())
        pending = []
        for ident in sorted(image_ids):
            output = _run_if_present("image", "inspect", ident)
            if output is None:
                continue
            image = json.loads(output)[0]
            if image["Id"] in referenced or _timestamp(image["Created"]) >= cutoff:
                continue
            tags = image.get("RepoTags") or []
            refs = tags if all_resources else [tag for tag in tags if _owned_tag(tag)]
            if all_resources and not refs:
                refs = [image["Id"]]
            pending.extend(refs)
        while pending:
            deferred = []
            for ref in pending:
                try:
                    # Never force deletion. Docker remains the final reference check.
                    if not dry_run:
                        if _run_if_present("image", "rm", "--no-prune", ref) is None:
                            continue
                except DmlRepoError as exc:
                    if "image has dependent child images" not in str(exc).lower():
                        raise
                    deferred.append(ref)
                    continue
                removed["images"].append(ref)
            if len(deferred) == len(pending):
                logger.warning("Retaining images with dependent children: %s", ", ".join(deferred))
                break
            pending = deferred
    return removed


def _systemd_quote(value: str, *, exec_argument: bool = True) -> str:
    if "\n" in value or "\r" in value or "\x00" in value:
        raise ValueError("systemd values must be single-line strings")
    value = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    if exec_argument:
        value = value.replace("$", "$$")
    return '"' + value + '"'


def install(*, all_resources: bool = False, older_than_hours: float = 24) -> Path:
    """Install and enable an hourly systemd user timer for Docker GC.

    Parameters
    ----------
    all_resources : bool
        Persist host-wide collection in the installed service.
    older_than_hours : float
        Minimum resource age passed to each collection.

    Returns
    -------
    Path
        Directory containing the installed service and timer.
    """
    if not sys.platform.startswith("linux") or not shutil.which("systemctl"):
        raise DmlRepoError(
            "Automatic installation requires Linux and systemd; schedule 'dml contrib docker-gc' manually"
        )
    if not 0 < older_than_hours < float("inf"):
        raise ValueError("older-than-hours must be a positive finite number")
    command = [sys.executable, "-m", "daggerml.contrib.docker_gc", "--older-than-hours", str(older_than_hours)]
    if all_resources:
        command.append("--all")
    # User services need the same Docker executable, cache lock, and daemon
    # selection as the interactive installer. No shell interpolation is used.
    environment = {"PATH": os.environ.get("PATH", os.defpath)}
    unset_environment = []
    for name in ("XDG_CACHE_HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_TLS_VERIFY",
                 "DOCKER_CERT_PATH"):
        if name in os.environ:
            environment[name] = os.environ[name]
        else:
            # User services inherit the manager's environment, which can contain
            # stale Docker or cache settings absent from this shell.
            unset_environment.append(name)
    service = ("[Unit]\nDescription=DaggerML Docker garbage collection\n\n[Service]\nType=oneshot\n"
               + "".join(f"Environment={_systemd_quote(f'{key}={value}', exec_argument=False)}\n"
                          for key, value in environment.items())
               + ("UnsetEnvironment=" + " ".join(unset_environment) + "\n" if unset_environment else "")
               + "ExecStart=" + " ".join(_systemd_quote(arg) for arg in command) + "\n")
    timer = ("[Unit]\nDescription=Hourly DaggerML Docker garbage collection\n\n[Timer]\n"
             "OnCalendar=hourly\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n")
    directory = Path(os.getenv("XDG_CONFIG_HOME") or Path.home() / ".config") / "systemd" / "user"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "daggerml-docker-gc.service").write_text(service, encoding="utf-8")
    (directory / "daggerml-docker-gc.timer").write_text(timer, encoding="utf-8")
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "--user", "enable", "--now", "daggerml-docker-gc.timer"], check=True)
    return directory


def main() -> None:
    """Run the standalone collector used by the installed timer."""
    from daggerml.contrib.status import cli

    raise SystemExit(cli(["docker-gc", *sys.argv[1:]]))


if __name__ == "__main__":
    main()
