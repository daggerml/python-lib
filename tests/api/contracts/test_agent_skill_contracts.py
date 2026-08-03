from __future__ import annotations

import argparse
from importlib import resources

from daggerml._cli import MethodCLI
from daggerml._core import Dml


def _agent_skill() -> str:
    return resources.files("daggerml").joinpath("SKILL.md").read_text(encoding="utf-8")


def test_agent_skill_001__admin_help_and_command_export_the_bundled_skill(capsys, monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    cli = MethodCLI(Dml, prog="dml")
    subparsers = next(action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction))

    admin_help = subparsers.choices["admin"].format_help()
    assert "agent-skill" in admin_help
    assert "Print the bundled coding-agent skill as portable" in admin_help
    assert "Markdown." in admin_help
    assert cli.run(["admin", "agent-skill"]) == 0
    assert capsys.readouterr().out == _agent_skill()


def test_agent_skill_002__has_portable_metadata_and_required_guidance() -> None:
    skill = _agent_skill()

    assert skill.startswith(
        "---\nname: daggerml\ndescription: Concise guidance for coding agents working with DaggerML projects.\n---\n"
    )
    assert len(skill.split()) <= 400
    for text in (
        "dml --help",
        "dml init",
        "dml status",
        "dag.put()",
        "dag.call()",
        "dag.commit()",
        "dml.load()",
        "@api.funkify",
        ".value()",
        "not module globals",
        "module-level import is unavailable in the script worker",
        "the funk source imports NumPy in its worker",
        "    import numpy as np",
        "extra_objs",
        "extra_lines",
        "remote.root",
        "@api.dagclass",
        "Dagclasses compose",
        "## Sharp Bits",
        "remote.root` is required",
        "extra_objs=(normalize,)",
        "Do not run these concurrently.",
        "managed objects or refs",
    ):
        assert text in skill
