"""Bounded S3 reads must not move database work onto worker threads."""

import json
from collections import Counter
from contextlib import contextmanager
from threading import Barrier, Event, Lock, get_ident
from types import SimpleNamespace

import pytest

from daggerml._core.db import Ref
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
        dags = [txn.put(Dag(nodes=[node], names={"result": node}, tags=[str(i)], result=node)) for i in range(4)]
        dag = dags[0]
        tree = txn.put(Tree(dags={f"dag-{i}": ref for i, ref in enumerate(dags)}))
        first = txn.put(Commit(parents=[], tree=tree, author="test", message="first"))
        tip = txn.put(Commit(parents=[first], tree=tree, author="test", message="second"))
    payloads = {}
    with source.tx(readonly=True) as txn:
        for ref in dags:
            payloads[remote._pack_key(ref)] = remote._encode_pack(ref, txn)
            payloads[remote._manifest_key(ref)] = json.dumps(dict(
                schema=1, root=ref.to, subdags=[], imports=[])).encode()
        for ref in (first, tip):
            commit, tree_obj = txn.get(ref), txn.get(tree)
            payloads[remote._commit_key(ref)] = json.dumps({
                "schema": 1, "commit": [ref.to, remote._dump_cas_object(commit)],
                "tree": [tree.to, remote._dump_cas_object(tree_obj)],
                "dags": {name: dag.to for name, dag in tree_obj.dags.items()},
            })
    return SimpleNamespace(remote=remote, target=CallerOnlyDB(target), payloads=payloads,
                           leaves=leaves, dag=dag, tip=tip, commits={first, tip})


@pytest.mark.parametrize("kind", ["manifest", "project"])
@pytest.mark.parametrize("workers", [1, 2, 4])
def test_fetch_bounds_parallel_reads_and_keeps_database_access_on_caller(fetch_graph, kind, workers):
    """Concurrent fetch preserves the complete graph and skips shared/local objects."""
    graph = fetch_graph
    remote, target = graph.remote, graph.target
    remote.n_workers = workers
    pack_keys = {key for key in graph.payloads if key.startswith("packs/")}
    expected_workers = workers if kind == "project" else 1
    barrier, lock, calls = Barrier(expected_workers), Lock(), Counter()
    active = peak = 0

    def get(key):
        nonlocal active, peak
        with lock:
            calls[key] += 1
            active += 1
            peak = max(peak, active)
        try:
            if key in pack_keys:
                assert get_ident() != target.caller
                barrier.wait(timeout=5)
            return graph.payloads[key]
        finally:
            with lock:
                active -= 1

    remote._store._get = get
    remote._store._get_bytes = get

    def fetch():
        if kind == "manifest":
            return remote.materialize_ref(graph.dag, target)
        ref, available, omitted = remote.materialize_project_commit(remote._build_ref_payload(graph.tip), target)
        assert available == graph.commits
        assert not omitted
        return ref

    expected = graph.dag if kind == "manifest" else graph.tip
    assert fetch() == expected
    assert peak == expected_workers
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
    failing_key = remote._pack_key(graph.dag)

    def get(key):
        if key == failing_key:
            raise OSError("S3 read failed")
        return graph.payloads[key]

    remote._store._get = get
    remote._store._get_bytes = get
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


def test_stream_consumes_completions_while_an_earlier_download_is_blocked(fetch_graph):
    remote = fetch_graph.remote
    remote.n_workers = 2
    slow, fast = Ref("dag:" + "0" * 64), Ref("dag:" + "1" * 64)
    release = Event()
    started = Event()

    def get(key):
        if key == slow:
            started.set()
            assert release.wait(timeout=5)
        else:
            assert started.wait(timeout=5)
        return b"body"

    remote._store._get_bytes = get
    with remote._download_stream({slow, fast}, lambda ref: ref) as completed:
        try:
            first = next(completed)
            assert first == [(fast, b"body")]
        finally:
            release.set()
        assert list(completed) == [[(slow, b"body")]]


def test_failed_consumer_unblocks_full_completion_queue(fetch_graph):
    remote = fetch_graph.remote
    remote.n_workers = 2
    remote._store._get_bytes = lambda key: b"body"
    roots = {Ref(f"dag:{i:064x}") for i in range(100)}
    with pytest.raises(ValueError, match="consumer failed"):
        with remote._download_stream(roots, lambda ref: ref) as completed:
            next(completed)
            raise ValueError("consumer failed")
