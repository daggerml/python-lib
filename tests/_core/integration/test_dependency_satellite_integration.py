"""Import-only dependency reads against an independent Moto endpoint."""

import daggerml.api as api


def test_dependency_fetch_inspect_and_checkout_does_not_change_primary_remote(satellite_world):
    primary, satellite = satellite_world()
    consumer = primary.publisher
    original_root = consumer.config.get("remote.root")
    initial = consumer.status()["commit"]

    assert consumer.dep.add("research", satellite.root) == "research"
    assert consumer.dep.list() == {"research": satellite.root}
    consumer.fetch("main", dep="research")
    ref = satellite.publisher.status()["commit"]
    assert consumer.rev_parse("main", dep="research")["commit"] == ref
    assert consumer.show("main", dep="research")["dags"]["satellite-input"]
    assert consumer.log("main", dep="research")["commits"]
    assert consumer.diff("main", dep="research")
    consumer.dag.checkout("main", "satellite-input", dep="research", name="imported")

    assert api.load("imported", dml=consumer).result.value() == {"answer": 42}
    assert consumer.status()["commit"] != initial
    assert consumer.config.get("remote.root") == original_root
    assert consumer.branch.list(remote=True) == []
    assert consumer.cache.describe("unrelated") is None
    consumer.dep.delete("research")
    assert consumer.dep.list() == {}
