from __future__ import annotations

import argparse
from importlib import resources

import pytest

from daggerml._cli import MethodCLI
from daggerml._core import Dml

SKILLS = {
    "authoring": {
        "description": "Build reproducible DaggerML DAGs and script-backed funks.",
        "guidance": ("`dag.call()`", "extra_objs", "post_lines", "normalized DaggerML input identity"),
        "max_examples": 2,
    },
    "repository": {
        "description": "Manage DaggerML history, references, remotes, dependencies, and garbage collection.",
        "guidance": ("managed `.dml/`", "--unshallow", "concurrently with fetch, pull, or push"),
        "max_examples": 1,
    },
    "inspection": {
        "description": "Inspect committed graphs, open runtimes, executions, errors, provenance, and cache state.",
        "guidance": ("frozen runtime", "describe_graph", "cache.invalidate"),
        "max_examples": 1,
    },
}


def _skill(name: str) -> str:
    return resources.files("daggerml._core").joinpath("skills", f"{name}.md").read_text(encoding="utf-8")


@pytest.mark.parametrize("name", SKILLS)
def test_agent_skill_001__skills_help_and_commands_export_bundled_resources(
    capsys, monkeypatch, tmp_path, name
) -> None:
    monkeypatch.chdir(tmp_path)
    cli = MethodCLI(Dml, prog="dml")
    subparsers = next(action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction))

    skills_help = subparsers.choices["skills"].format_help()
    assert name in skills_help
    assert "Print the bundled" in skills_help
    assert cli.run(["skills", name]) == 0
    assert capsys.readouterr().out == _skill(name)


@pytest.mark.parametrize(("name", "contract"), SKILLS.items())
def test_agent_skill_002__resources_are_portable_compact_and_topic_specific(name, contract) -> None:
    skill = _skill(name)

    assert skill.startswith(f"---\nname: daggerml-{name}\ndescription: {contract['description']}\n---\n")
    assert len(skill.split()) <= 250
    assert skill.count("```") <= contract["max_examples"] * 2
    assert "](" not in skill
    for text in contract["guidance"]:
        assert text in skill
