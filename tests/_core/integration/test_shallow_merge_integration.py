"""Public shallow snapshots with a published two-parent history."""

import pytest

import daggerml.api as api
from daggerml import Dml
from daggerml._core import DmlRepoError


def test_merge_tip_materializes_both_parents_and_deepens(shallow_merge_world):
    world = shallow_merge_world()
    tip = world.publisher.status()["commit"]
    source = world.publisher.log()["commits"]
    clone = world.clone("branch", depth=1)
    assert clone.show()["diff"] is None
    with pytest.raises(DmlRepoError, match="shallow"):
        clone.diff()
    assert clone.status()["commit"] == tip
    assert api.load("main-input", dml=clone).result.value() == "main"
    assert api.load("side-input", dml=clone).result.value() == "side"
    assert len(clone.log()["commits"]) == 1
    clone.fetch(depth=2)
    assert len(clone.log()["commits"]) >= 3
    clone.fetch(unshallow=True)
    assert len(clone.log()["commits"]) == len(source)

    exact = Dml.clone(tip, project_home=str(world.home / "exact"), remote_root=world.root, depth=1)
    assert exact.status()["mode"] == "detached"
    assert api.load("side-input", dml=exact).result.value() == "side"


def test_merge_tip_deepens_both_parent_histories(shallow_merge_world):
    world = shallow_merge_world()
    clone = world.clone("deepening", depth=1)
    clone.fetch(depth=2)
    clone.fetch(unshallow=True)
    assert len(clone.log()["commits"]) == len(world.publisher.log()["commits"])
    assert api.load("main-input", dml=clone).result.value() == "main"
    assert api.load("side-input", dml=clone).result.value() == "side"


def test_shallow_branch_can_advance_observed_tip_but_not_create_new_branch(shallow_merge_world):
    world = shallow_merge_world()
    clone = world.clone("writer", depth=1)
    with api.new("new-work", dml=clone) as dag:
        dag.commit(dag.put(42))
    clone.push()
    assert world.clone("reader").status()["commit"] == clone.status()["commit"]
    clone.branch.set_upstream("new-branch")
    with pytest.raises(DmlRepoError, match="shallow history"):
        clone.push()


def test_tagged_merge_tip_clones_by_public_tag(shallow_merge_world):
    world = shallow_merge_world()
    world.publisher.tag.create("merged")
    world.publisher.push(revision="@merged")
    with pytest.raises(DmlRepoError, match="already exists"):
        world.publisher.push(revision="@merged")
    tagged = world.clone("tagged", revision="@merged", depth=1)
    assert tagged.status()["mode"] == "detached"
    assert api.load("side-input", dml=tagged).result.value() == "side"
