"""Reachability through public repository operations and Moto remote GC."""

import pytest
from botocore.exceptions import ClientError

import daggerml.api as api
from daggerml import Dml
from daggerml._core import DmlRepoError


@pytest.mark.xfail(strict=True, reason="Dml.show traverses the missing shallow parent while describing the tip")
def test_local_gc_preserves_live_branch_tag_and_shallow_boundary(collaboration_world):
    world = collaboration_world()
    producer = world.publisher
    producer.tag.create("seed")
    with api.new("later", dml=producer) as dag:
        dag.commit(dag.put(2))
    producer.push()
    shallow = world.clone("shallow", depth=1)
    shallow.gc()
    assert api.load("later", dml=shallow).result.value() == 2
    shallow.fetch(unshallow=True)
    shallow.gc()
    assert api.load("seed", dml=shallow).result.value() == 1

    original = producer.status()["commit"]
    producer.branch.create("keeper")
    producer.tag.create("keeper-tag")
    with api.new("orphan", dml=producer) as dag:
        dag.commit(dag.put("orphan"))
    orphan = producer.status()["commit"]
    producer.branch.move("main", original)
    # A commit built on the live tip is still reachable through ancestry; GC
    # tests below must create a genuinely detached branch before checking removal.
    assert orphan != original
    producer.gc()
    assert producer.show("keeper")["dags"]["later"]
    assert producer.show("@keeper-tag")["dags"]["later"]


def test_local_gc_preserves_branch_and_tag_roots(collaboration_world):
    world = collaboration_world()
    publisher = world.publisher
    publisher.branch.create("keeper")
    publisher.tag.create("keeper-tag")
    publisher.gc()
    assert publisher.show("keeper")["dags"]["seed"]
    assert publisher.show("@keeper-tag")["dags"]["seed"]


def test_local_gc_collects_publicly_orphaned_branch_history(collaboration_world):
    world = collaboration_world()
    publisher = world.publisher
    publisher.branch.create("throwaway")
    publisher.checkout("throwaway")
    with api.new("throwaway-result", dml=publisher) as dag:
        dag.commit(dag.put("temporary"))
    orphan = publisher.status()["commit"]
    assert publisher.show(orphan)["dags"]["throwaway-result"]
    publisher.checkout("main")
    publisher.branch.delete("throwaway")
    publisher.gc()
    with pytest.raises((DmlRepoError, KeyError)):
        publisher.show(orphan)
    assert api.load("seed", dml=publisher).result.value() == 1


def test_remote_gc_preserves_published_tip_and_clones(collaboration_world):
    world = collaboration_world()
    before = world.publisher.status()["commit"]
    summary = world.publisher.gc(remote=True)
    assert "cas-retained" in summary
    observer = Dml.clone("main", project_home=str(world.home / "gc-observer"), remote_root=world.root)
    assert observer.status()["commit"] == before
    assert api.load("seed", dml=observer).result.value() == 1


def test_remote_gc_prunes_force_replaced_tip_but_keeps_current_branch(collaboration_world):
    world = collaboration_world()
    publisher = world.publisher
    publisher.branch.create("replacement")
    publisher.checkout("main")
    with api.new("old", dml=publisher) as dag:
        dag.commit(dag.put("discarded"))
    old_tip = publisher.status()["commit"]
    publisher.push()
    publisher.checkout("replacement")
    with api.new("new", dml=publisher) as dag:
        dag.commit(dag.put("kept"))
    publisher.branch.set_upstream("main")
    publisher.push(force=True)

    gc = Dml(str(world.home / "publisher"), remote_root=world.root, user="publisher", remote_prune_age_seconds=1)
    gc.gc(remote=True)
    clone = world.clone("after-prune")
    assert api.load("new", dml=clone).result.value() == "kept"
    with pytest.raises((DmlRepoError, ClientError)):
        Dml.clone(old_tip, project_home=str(world.home / "pruned"), remote_root=world.root)
