from __future__ import annotations

import argparse
from importlib import resources

import pytest

from daggerml._cli import MethodCLI
from daggerml._core import Dml

SKILLS = {
    "querying": {
        "description": "Extract data, traverse DAGs and provenance, and capture persisted errors.",
        "guidance": ("`dag.result`", "`Projection`", "root=False", "NodeError"),
        "max_examples": 2,
    },
    "authoring": {
        "description": "Build reproducible DaggerML DAGs and script-backed funks.",
        "guidance": ("`dag.require", "`.value()`", "extra_objs", "normalized DaggerML input identity"),
        "max_examples": 2,
    },
    "repository": {
        "description": (
            "Set up and manage DaggerML projects, history, remotes, dependencies, cache, and garbage collection."
        ),
        "guidance": ("managed `.dml/`", "--unshallow", "cache describe", "exact `execution` ref"),
        "max_examples": 2,
    },
    "extensions": {
        "description": "Build and test DaggerML adapters, executors, codecs, and integration plugins.",
        "guidance": ("transport boundary", "`poll`", "validate_adapter_response", "daggerml.codecs"),
        "max_examples": 2,
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

    skills_parser = subparsers.choices["skills"]
    skill_subparsers = next(
        action for action in skills_parser._actions if isinstance(action, argparse._SubParsersAction)
    )
    assert set(skill_subparsers.choices) == set(SKILLS)
    skills_help = subparsers.choices["skills"].format_help()
    assert name in skills_help
    assert "Print the bundled" in skills_help
    assert cli.run(["skills", name]) == 0
    assert capsys.readouterr().out == _skill(name)


@pytest.mark.parametrize(("name", "contract"), SKILLS.items())
def test_agent_skill_002__resources_are_portable_compact_and_topic_specific(name, contract) -> None:
    skill = _skill(name)

    assert skill.startswith(f"---\nname: daggerml-{name}\ndescription: {contract['description']}\n---\n")
    assert len(skill.split()) <= 1000
    assert skill.count("```") <= contract["max_examples"] * 2
    assert "](" not in skill
    for text in contract["guidance"]:
        assert text in skill
