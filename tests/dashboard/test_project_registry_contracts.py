from pathlib import Path

from daggerml.dashboard.config import DashboardProjects


def test_dash_project_001__registration_is_versioned_global_dashboard_config(tmp_path, monkeypatch):
    other = tmp_path / "other"
    other.mkdir()
    config_home = tmp_path / "config"
    registry = DashboardProjects(config_home)
    monkeypatch.setattr(
        DashboardProjects,
        "directory",
        property(lambda _self: tmp_path / "config" / "version" / "dashboard"),
    )

    registered = registry.register(other, name="Other project")
    projects = registry.list()

    assert registry.path == tmp_path / "config" / "version" / "dashboard" / "projects.json"
    assert projects["default_project_id"] == registered["id"]
    assert registry.get(registered["id"]) == Path(other).resolve()
    assert registry.unregister(registered["id"]) is True
    assert registry.unregister(registered["id"]) is False


def test_dash_project_002__default_project_is_inferred_from_config_directory(tmp_path):
    config_home = tmp_path / "config"
    project = tmp_path / "project"
    config_home.mkdir()
    project.mkdir()
    (config_home / "config.json").write_text(f'{{"project_home": "{project}"}}', encoding="utf-8")

    registry = DashboardProjects(config_home)

    assert registry.default_project == project.resolve()
    assert registry.list()["items"][0]["path"] == str(project.resolve())


def test_dash_project_003__empty_config_does_not_treat_working_or_config_directory_as_a_project(tmp_path):
    config_home = tmp_path / "config"

    registry = DashboardProjects(config_home)

    assert registry.default_project is None
    assert registry.list() == {"items": [], "default_project_id": None}
