"""Bounded S3 reads must not move database work onto worker threads."""

from collections import Counter
from contextlib import contextmanager
from threading import Barrier, Lock, get_ident
from types import SimpleNamespace

import pytest

from daggerml._core.remote import Remote
from daggerml._core.types import Commit, Dag, DmlDB, ListDatum, LiteralNode, ScalarDatum, Tree


class CallerOnlyDB:
    def __init__(self, db):
        self.db = db
        self.caller = get_ident()
        self.writes = 0

    @contextmanager
    def tx(self, **kwargs):
        assert get_ident() == self.caller
        with self.db.tx(**kwargs) as txn:
            yield self._guard(txn)

    def _guard(self, txn):
        def caller_only(method):
            def call(*args, **kwargs):
                assert get_ident() == self.caller
                return method(*args, **kwargs)
            return call

        return SimpleNamespace(exists=caller_only(txn.exists), get=caller_only(txn.get), put=caller_only(txn.put))

    def write_with_growth(self, callback):
        assert get_ident() == self.caller
        self.writes += 1
        return self.db.write_with_growth(lambda txn: callback(self._guard(txn)))


@pytest.fixture
def fetch_graph(tmp_path):
    def make_db(name):
        home = tmp_path / name
        home.mkdir()
        db = DmlDB(str(home), 1024 * 1024, 64 * 1024 * 1024)
        with db.tx(create_if_missing=True):
            pass
        return db

    source, target = make_db("source"), make_db("target")
    remote = object.__new__(Remote)
    remote._store = SimpleNamespace(_key_for=lambda key: key)
    with source.tx() as txn:
        leaves = [txn.put(ScalarDatum(value)) for value in range(4)]
        value = txn.put(ListDatum(leaves))
        node = txn.put(LiteralNode(value=value))
        dag = txn.put(Dag(nodes=[node], names={"result": node}, tags=[], result=node))
        tree = txn.put(Tree(dags={"main": dag}))
        first = txn.put(Commit(parents=[], tree=tree, author="test", message="first"))
        tip = txn.put(Commit(parents=[first], tree=tree, author="test", message="second"))
    payloads, _ = remote._collect_local_objects(tip, source)
    return SimpleNamespace(remote=remote, target=CallerOnlyDB(target), payloads=payloads,
                           leaves=leaves, dag=dag, tip=tip, commits={first, tip})


@pytest.mark.parametrize("kind", ["manifest", "project"])
@pytest.mark.parametrize("workers", [1, 2, 4])
def test_fetch_bounds_parallel_reads_and_keeps_database_access_on_caller(fetch_graph, kind, workers):
    """Concurrent fetch preserves the complete graph and skips shared/local objects."""
    graph = fetch_graph
    remote, target = graph.remote, graph.target
    remote.n_workers = workers
    leaf_keys = {remote._cas_key(ref.id()) for ref in graph.leaves}
    barrier, lock, calls = Barrier(workers), Lock(), Counter()
    active = peak = 0

    def get(key):
        nonlocal active, peak
        with lock:
            calls[key] += 1
            active += 1
            peak = max(peak, active)
        try:
            if key in leaf_keys:
                assert get_ident() != target.caller
                barrier.wait(timeout=5)
            return graph.payloads[key]
        finally:
            with lock:
                active -= 1

    remote._store._get = get

    def fetch():
        if kind == "manifest":
            return remote.materialize_ref(graph.dag, target)
        ref, available, omitted = remote.materialize_project_commit(remote._build_ref_payload(graph.tip), target)
        assert available == graph.commits
        assert not omitted
        return ref

    expected = graph.dag if kind == "manifest" else graph.tip
    assert fetch() == expected
    assert peak == workers
    assert all(count == 1 for count in calls.values())
    with target.tx(readonly=True) as txn:
        assert txn.exists(expected)
        assert all(txn.exists(ref) for ref in graph.leaves)
    before = dict(calls)
    # Existing roots skip the remote, as before; available-commit reporting is empty.
    if kind == "manifest":
        assert remote.materialize_ref(graph.dag, target) == graph.dag
    else:
        result = remote.materialize_project_commit(remote._build_ref_payload(graph.tip), target)
        assert result == (graph.tip, set(), set())
    assert dict(calls) == before


@pytest.mark.parametrize("kind", ["manifest", "project"])
def test_failed_worker_read_does_not_begin_materialization(fetch_graph, kind):
    graph = fetch_graph
    remote, target = graph.remote, graph.target
    remote.n_workers = 2
    failing_key = remote._cas_key(graph.leaves[0].id())

    def get(key):
        if key == failing_key:
            raise OSError("S3 read failed")
        return graph.payloads[key]

    remote._store._get = get
    with pytest.raises(OSError, match="S3 read failed"):
        if kind == "manifest":
            remote.materialize_ref(graph.dag, target)
        else:
            remote.materialize_project_commit(remote._build_ref_payload(graph.tip), target)
    assert target.writes == 0
    with target.tx(readonly=True) as txn:
        assert not txn.exists(graph.tip)
        assert not txn.exists(graph.dag)
        assert all(not txn.exists(ref) for ref in graph.leaves)
