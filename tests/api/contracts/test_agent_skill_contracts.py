from __future__ import annotations

from importlib import resources

import pytest

from daggerml._cli import MethodCLI
from daggerml._core import Dml

SKILLS = ("querying", "authoring", "repository", "extensions")


def _skill(name: str) -> str:
    return resources.files("daggerml._core").joinpath("skills", name, "SKILL.md").read_text(encoding="utf-8")


def _dagclass_example() -> bytes:
    return resources.files("daggerml._core").joinpath("skills", "authoring", "examples", "dagclass.py").read_bytes()


@pytest.mark.parametrize("name", SKILLS)
def test_custom_name_and_explicit_overwrite(monkeypatch, tmp_path, name) -> None:
    monkeypatch.chdir(tmp_path)
    cli = MethodCLI(Dml, prog="dml")
    parent = tmp_path / "skills"
    destination = parent / f"custom-{name}" / "SKILL.md"
    assert cli.run(["skills", name, str(parent), "--name", f"custom-{name}"]) == 0
    assert destination.read_text(encoding="utf-8") == _skill(name).replace(
        f"name: daggerml-{name}\n", f"name: custom-{name}\n", 1
    )
    destination.write_text("user changes", encoding="utf-8")
    if name == "authoring":
        (destination.parent / "examples" / "dagclass.py").write_text("user example", encoding="utf-8")
    (destination.parent / "notes.txt").write_text("preserve", encoding="utf-8")
    with pytest.raises(FileExistsError):
        cli.run(["skills", name, str(parent), "--name", f"custom-{name}"])
    assert destination.read_text(encoding="utf-8") == "user changes"
    if name == "authoring":
        assert (destination.parent / "examples" / "dagclass.py").read_text(encoding="utf-8") == "user example"
    assert cli.run(["skills", name, str(parent), "--name", f"custom-{name}", "--overwrite"]) == 0
    assert destination.read_text(encoding="utf-8").startswith(f"---\nname: custom-{name}\n")
    if name == "authoring":
        assert (destination.parent / "examples" / "dagclass.py").read_bytes() == _dagclass_example()
    assert (destination.parent / "notes.txt").read_text(encoding="utf-8") == "preserve"


@pytest.mark.parametrize("bad_name", ["../escape", "two/parts", "", ".", "UPPER", "a b"])
def test_invalid_skill_name_does_not_write(monkeypatch, tmp_path, bad_name) -> None:
    monkeypatch.chdir(tmp_path)
    cli = MethodCLI(Dml, prog="dml")
    parent = tmp_path / "skills"
    with pytest.raises(ValueError, match="skill name"):
        cli.run(["skills", "authoring", str(parent), "--name", bad_name])
    assert not parent.exists()


def test_symlink_skill_target_is_rejected(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    cli = MethodCLI(Dml, prog="dml")
    parent = tmp_path / "skills"
    parent.mkdir()
    target = tmp_path / "elsewhere"
    target.mkdir()
    (parent / "daggerml-authoring").symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        cli.run(["skills", "authoring", str(parent), "--overwrite"])
    assert not (target / "SKILL.md").exists()


def test_python_skill_method_returns_write_diagnostic(tmp_path) -> None:
    directory = tmp_path / "skills" / "daggerml-querying"
    result = Dml().skills.querying(str(tmp_path / "skills"))
    assert result == f"Installed {directory} ({len(_skill('querying').encode('utf-8'))} bytes written)"
    assert (directory / "SKILL.md").read_text(encoding="utf-8") == _skill("querying")


def test_overwrite_rejects_symlinked_skill_file(tmp_path) -> None:
    parent = tmp_path / "skills"
    directory = parent / "daggerml-querying"
    directory.mkdir(parents=True)
    external = tmp_path / "external.md"
    external.write_text("original", encoding="utf-8")
    (directory / "SKILL.md").symlink_to(external)
    with pytest.raises(ValueError, match="regular file"):
        Dml().skills.querying(str(parent), overwrite=True)
    assert external.read_text(encoding="utf-8") == "original"


def test_authoring_overwrite_rejects_symlinked_example(tmp_path) -> None:
    parent = tmp_path / "skills"
    directory = parent / "daggerml-authoring"
    Dml().skills.authoring(str(parent))
    external = tmp_path / "elsewhere.py"
    external.write_text("unchanged", encoding="utf-8")
    (directory / "examples" / "dagclass.py").unlink()
    (directory / "examples" / "dagclass.py").symlink_to(external)
    (directory / "SKILL.md").write_text("user edits", encoding="utf-8")
    with pytest.raises(ValueError, match="regular file"):
        Dml().skills.authoring(str(parent), overwrite=True)
    assert (directory / "SKILL.md").read_text(encoding="utf-8") == "user edits"
    assert external.read_text(encoding="utf-8") == "unchanged"
