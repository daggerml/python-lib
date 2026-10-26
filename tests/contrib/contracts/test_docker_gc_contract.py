"""GC stays opt-in, scoped, age-gated, and coordinated with launches."""

import fcntl
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from daggerml.api import DmlRepoError
from daggerml.contrib import docker_gc
from daggerml.contrib.executors import docker as docker_module
from daggerml.contrib.status import cli


@pytest.fixture
def backend(monkeypatch, tmp_path):
    old = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    new = datetime.now(timezone.utc).isoformat()

    def container(ident, image, status="exited", owned=True, finished=old):
        return {"Id": ident, "Image": image, "Created": old,
                "Config": {"Labels": {"daggerml.managed": "true"} if owned else {}},
                "State": {"Status": status, "FinishedAt": finished}}

    def image(ident, tags, created=old):
        return {"Id": ident, "RepoTags": tags, "Created": created}

    state = SimpleNamespace(calls=[], containers={
        "stopped": container("stopped", "shared"),
        "running": container("running", "busy", "running"),
        "foreign": container("foreign", "foreign-image", owned=False),
        "recent": container("recent", "recent-image", finished=new),
        "created": container("created", "created-image", "created", finished="0001-01-01T00:00:00Z"),
        "paused": container("paused", "paused-image", "paused"),
    }, images={
        "shared": image("shared", ["daggerml-archive:a", "daggerml-archive:b", "user:keep"]),
        "busy": image("busy", ["daggerml-archive:busy"]),
        "foreign-image": image("foreign-image", ["user:foreign"]),
        "recent-image": image("recent-image", ["daggerml-archive:recent"]),
        "created-image": image("created-image", ["docker.io/library/daggerml-archive:created"]),
        "paused-image": image("paused-image", ["daggerml-archive:paused"]),
        "new-image": image("new-image", ["daggerml-archive:new"], new),
        "dangling": image("dangling", None),
        "unowned": image("unowned", ["user:unused"]),
    })

    def run(*args, **kwargs):
        state.calls.append(args)
        kind, operation = args[:2]
        items = state.containers if kind == "container" else state.images
        if operation == "ls":
            return "\n".join(items)
        if operation == "inspect":
            return json.dumps([items[args[-1]]])
        if operation == "rm":
            assert "-f" not in args and "--force" not in args
            if kind == "container":
                assert items[args[-1]]["State"]["Status"] not in {"running", "paused"}
            return ""
        pytest.fail(str(args))

    monkeypatch.setattr(docker_module, "_IMAGE_LOCK_PATH", tmp_path / "images.lock")
    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(run))
    return state


def test_default_gc_removes_only_old_owned_unreferenced_resources(backend):
    result = docker_gc.collect()
    assert result["containers"] == ["stopped", "created"]
    assert set(result["images"]) == {"daggerml-archive:a", "daggerml-archive:b",
                                     "docker.io/library/daggerml-archive:created"}
    assert ("image", "rm", "--no-prune", "user:keep") not in backend.calls


def test_all_is_explicit_opt_in_and_still_protects_active_and_recent_resources(backend):
    result = docker_gc.collect(all_resources=True)
    assert set(result["containers"]) == {"stopped", "created", "foreign"}
    assert set(result["images"]) == {"daggerml-archive:a", "daggerml-archive:b", "user:keep",
                                     "docker.io/library/daggerml-archive:created", "user:foreign",
                                     "dangling", "user:unused"}


def test_dry_run_reports_candidates_without_mutation(backend):
    result = docker_gc.collect(dry_run=True)
    assert result["containers"] and result["images"]
    assert not any(call[1] == "rm" for call in backend.calls)


@pytest.mark.parametrize("all_resources", [False, True])
@pytest.mark.parametrize("kind,operation,target", [
    ("container", "inspect", "foreign"),
    ("container", "rm", "stopped"),
    ("image", "inspect", "shared"),
    ("image", "rm", "daggerml-archive:a"),
])
@pytest.mark.parametrize("missing_object", [False, True])
def test_disappearing_resources_do_not_abort_collection(
    backend, monkeypatch, all_resources, kind, operation, target, missing_object,
):
    original = docker_module.DockerExecutor._run_docker
    failures = []

    def run(*args, **kwargs):
        if args[:2] == (kind, operation) and args[-1] == target:
            failures.append(args)
            resource = "object" if missing_object else kind
            raise DmlRepoError(f"Error response from daemon: No such {resource}: {target}")
        return original(*args, **kwargs)

    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(run))
    result = docker_gc.collect(all_resources=all_resources)
    assert len(failures) == 1
    assert target not in result["containers" if kind == "container" else "images"]
    assert "created" in result["containers"]
    assert "docker.io/library/daggerml-archive:created" in result["images"]
    if operation == "inspect":
        assert not any(call[:2] == (kind, "rm") and call[-1] == target for call in backend.calls)


@pytest.mark.parametrize("kind,operation", [
    ("container", "inspect"), ("container", "rm"), ("image", "inspect"), ("image", "rm"),
])
@pytest.mark.parametrize("error", ["Cannot connect to the Docker daemon", "permission denied"])
def test_resource_race_handling_preserves_real_errors(backend, monkeypatch, kind, operation, error):
    original = docker_module.DockerExecutor._run_docker

    def run(*args, **kwargs):
        if args[:2] == (kind, operation):
            raise DmlRepoError(error)
        return original(*args, **kwargs)

    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(run))
    with pytest.raises(DmlRepoError, match=error):
        docker_gc.collect()


@pytest.mark.parametrize("kind,target", [("container", "foreign"), ("image", "shared")])
def test_dry_run_skips_resources_missing_during_inspection(backend, monkeypatch, kind, target):
    original = docker_module.DockerExecutor._run_docker

    def run(*args, **kwargs):
        if args[:2] == (kind, "inspect") and args[-1] == target:
            raise DmlRepoError(f"No such {kind}: {target}")
        return original(*args, **kwargs)

    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(run))
    result = docker_gc.collect(dry_run=True)
    assert "created" in result["containers"]
    assert "docker.io/library/daggerml-archive:created" in result["images"]
    assert not any(call[1] == "rm" for call in backend.calls)


@pytest.mark.parametrize("dry_run", [False, True])
def test_child_dependency_conflicts_do_not_block_progress(backend, monkeypatch, dry_run):
    old = backend.images["dangling"]["Created"]
    backend.containers = {}
    backend.images = {
        "a-parent": {"Id": "a-parent", "RepoTags": [], "Created": old},
        "z-child": {"Id": "z-child", "RepoTags": ["child:test"], "Created": old},
    }
    original = docker_module.DockerExecutor._run_docker
    deleted = []

    def run(*args, **kwargs):
        if args[:2] == ("image", "rm"):
            if args[-1] == "a-parent" and "child:test" not in deleted:
                backend.calls.append(args)
                raise DmlRepoError("conflict: image has dependent child images")
            deleted.append(args[-1])
        return original(*args, **kwargs)

    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(run))
    result = docker_gc.collect(all_resources=True, dry_run=dry_run)
    assert set(result["images"]) == {"a-parent", "child:test"}
    if dry_run:
        assert deleted == []
    else:
        assert deleted == ["child:test", "a-parent"]
        assert backend.calls.count(("image", "rm", "--no-prune", "a-parent")) == 2


def test_dependency_conflicts_without_progress_are_retained(backend, monkeypatch, caplog):
    original = docker_module.DockerExecutor._run_docker
    attempts = []

    def run(*args, **kwargs):
        if args[:2] == ("image", "rm"):
            attempts.append(args[-1])
            raise DmlRepoError("conflict: image has dependent child images")
        return original(*args, **kwargs)

    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(run))
    assert docker_gc.collect()["images"] == []
    assert len(attempts) == len(set(attempts))
    assert "Retaining images with dependent children" in caplog.text


def test_dependency_deferral_does_not_hide_daemon_failures(backend, monkeypatch):
    original = docker_module.DockerExecutor._run_docker

    def run(*args, **kwargs):
        if args[:2] == ("image", "rm"):
            raise DmlRepoError("Cannot connect to the Docker daemon")
        return original(*args, **kwargs)

    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(run))
    with pytest.raises(DmlRepoError, match="Cannot connect"):
        docker_gc.collect()


def test_collection_holds_the_launch_lock(backend, monkeypatch):
    original = docker_module.DockerExecutor._run_docker

    def run(*args, **kwargs):
        with docker_module._IMAGE_LOCK_PATH.open("a+") as lock_file:
            with pytest.raises(BlockingIOError):
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return original(*args, **kwargs)

    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(run))
    docker_gc.collect()


@pytest.mark.parametrize("age", [0, -1, float("inf"), float("nan")])
def test_invalid_age_is_rejected_before_docker_access(backend, age):
    with pytest.raises(ValueError, match="positive finite"):
        docker_gc.collect(older_than_hours=age)
    assert backend.calls == []


def test_daemon_failure_is_not_reported_as_success(monkeypatch):
    def fail(*args, **kwargs):
        raise DmlRepoError("daemon unavailable")

    monkeypatch.setattr(docker_module.DockerExecutor, "_run_docker", staticmethod(fail))
    with pytest.raises(SystemExit) as error:
        cli(["docker-gc"])
    assert error.value.code == 1


@pytest.mark.parametrize("all_resources", [False, True])
def test_cli_collection_scope(monkeypatch, capsys, all_resources):
    calls = []
    monkeypatch.setattr(docker_gc, "collect", lambda **kwargs: calls.append(kwargs) or {})
    assert cli(["docker-gc", "--dry-run", *(["--all"] if all_resources else [])]) == 0
    assert calls == [{"all_resources": all_resources, "older_than_hours": 24, "dry_run": True}]
    assert json.loads(capsys.readouterr().out) == {}


@pytest.mark.parametrize("all_resources", [False, True])
def test_install_persists_scope_and_enables_hourly_user_timer(monkeypatch, tmp_path, all_resources):
    calls = []
    monkeypatch.setattr(docker_gc.sys, "platform", "linux")
    monkeypatch.setattr(docker_gc.sys, "executable", "/path with spaces/python")
    monkeypatch.setattr(docker_gc.shutil, "which", lambda _: "/bin/systemctl")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("DOCKER_CONTEXT", "local-test")
    monkeypatch.setattr(docker_gc.subprocess, "run", lambda command, **kwargs: calls.append((command, kwargs)))
    directory = docker_gc.install(all_resources=all_resources, older_than_hours=48)
    service = (directory / "daggerml-docker-gc.service").read_text()
    timer = (directory / "daggerml-docker-gc.timer").read_text()
    assert 'ExecStart="/path with spaces/python" "-m" "daggerml.contrib.docker_gc"' in service
    assert ('"--all"' in service) is all_resources
    assert '"--older-than-hours" "48"' in service
    assert 'Environment="DOCKER_CONTEXT=local-test"' in service
    assert f'Environment="XDG_CACHE_HOME={tmp_path / "cache"}"' in service
    assert "OnCalendar=hourly" in timer and "Persistent=true" in timer
    assert calls == [(["systemctl", "--user", "daemon-reload"], {"check": True}),
                     (["systemctl", "--user", "enable", "--now", "daggerml-docker-gc.timer"], {"check": True})]


@pytest.mark.parametrize("settings", [
    {},
    {"DOCKER_HOST": "unix:///run/user/1000/docker.sock"},
    {"DOCKER_CONTEXT": "", "XDG_CACHE_HOME": ""},
    {name: "configured" for name in (
        "XDG_CACHE_HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH")},
])
def test_installed_service_preserves_present_and_absent_connection_settings(monkeypatch, tmp_path, settings):
    tracked = {"XDG_CACHE_HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG",
               "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"}
    for name in tracked:
        monkeypatch.delenv(name, raising=False)
    for name, value in settings.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(docker_gc.sys, "platform", "linux")
    monkeypatch.setattr(docker_gc.shutil, "which", lambda _: "/bin/systemctl")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setattr(docker_gc.subprocess, "run", lambda *args, **kwargs: None)
    directory = docker_gc.install(all_resources=True)
    service = (directory / "daggerml-docker-gc.service").read_text()
    unset = {name for line in service.splitlines() if line.startswith("UnsetEnvironment=")
             for name in line.removeprefix("UnsetEnvironment=").split()}
    assert unset == tracked - settings.keys()
    assert "PATH" not in unset
    for name in tracked:
        if name in settings:
            assert f'Environment="{name}={settings[name]}"' in service
        else:
            assert f'Environment="{name}=' not in service
    if not unset:
        assert "UnsetEnvironment=" not in service


def test_unsupported_scheduler_leaves_files_untouched(monkeypatch, tmp_path):
    monkeypatch.setattr(docker_gc.sys, "platform", "darwin")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    with pytest.raises(DmlRepoError, match="schedule .* manually"):
        docker_gc.install()
    assert list(tmp_path.iterdir()) == []


def test_systemd_arguments_cannot_expand_or_inject_directives():
    assert docker_gc._systemd_quote('a%h$HOME"\\b') == '"a%%h$$HOME\\"\\\\b"'
    with pytest.raises(ValueError, match="single-line"):
        docker_gc._systemd_quote("a\nExecStart=bad")
