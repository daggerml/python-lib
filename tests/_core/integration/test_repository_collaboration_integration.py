"""Public collaboration across independently initialized Moto-backed projects."""

import pytest

import daggerml.api as api
from daggerml import Dml
from daggerml._core import DmlRepoError


def _commit(dml, name, value):
    with api.new(name, dml=dml) as dag:
        dag.commit(dag.put(value, name="value"))
    return dml.status()["commit"]


def test_clone_diverge_merge_and_publish_to_observer(collaboration_world):
    world = collaboration_world()
    producer = world.publisher
    consumer = world.clone("consumer")
    observer = world.clone("observer")

    assert api.load("seed", dml=consumer).result.value() == 1
    assert consumer.status()["upstream"] == "main"
    assert consumer.status()["ahead"] == consumer.status()["behind"] == 0

    consumer_tip = _commit(consumer, "consumer-work", "reviewed")
    assert consumer.status()["ahead"] == 1
    producer_tip = _commit(producer, "producer-work", "published")
    producer.push()
    consumer.fetch()
    assert consumer.status()["ahead"] == consumer.status()["behind"] == 1
    with pytest.raises(DmlRepoError):
        consumer.push()

    merged = consumer.merge("main", remote=True, ff_only=False)
    assert merged["commit"] not in (consumer_tip, producer_tip)
    assert merged["behind"] == 0
    assert api.load("producer-work", dml=consumer).result.value() == "published"
    consumer.push()
    observer.pull()
    assert observer.status()["commit"] == merged["commit"]
    assert api.load("consumer-work", dml=observer).result.value() == "reviewed"
    assert api.load("producer-work", dml=observer).result.value() == "published"

    reopened = Dml(str(world.home / "observer"), remote_root=world.root, user="observer")
    assert reopened.status()["commit"] == merged["commit"]
    assert len(reopened.log()["commits"]) >= 4


def test_divergent_rebase_then_revert_is_visible_to_observer(collaboration_world):
    world = collaboration_world("rebase-world")
    producer, consumer = world.publisher, world.clone("rebasing")
    observer = world.clone("watcher")
    _commit(consumer, "consumer-work", "draft")
    _commit(producer, "producer-work", "approved")
    producer.push()

    consumer.fetch()
    rebased = consumer.rebase("main", remote=True)
    assert rebased["behind"] == 0
    assert api.load("consumer-work", dml=consumer).result.value() == "draft"
    assert api.load("producer-work", dml=consumer).result.value() == "approved"
    consumer.push()
    observer.pull()
    assert api.load("consumer-work", dml=observer).result.value() == "draft"

    reverted = consumer.revert("HEAD")
    assert "consumer-work" not in consumer.show()["dags"]
    assert "producer-work" in consumer.show()["dags"]
    consumer.push()
    observer.pull()
    assert observer.status()["commit"] == reverted["commit"]
    assert "consumer-work" not in observer.show()["dags"]
